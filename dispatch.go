package main

import (
    "github.com/go-martini/martini"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/sqs"

    "gopkg.in/redis.v3"

    "os"
    "os/exec"
    "bufio"
    "path/filepath"

    "strings"
    "strconv"

    "net/http"
    "net/url"

    "encoding/json"
    "log"
)

var (
    config Config

    rds *redis.Client

    queue *sqs.SQS
    queueUrl string
    receive *sqs.ReceiveMessageInput

    isIndexing string
)

type Config struct {
    QueueName string `json:"sqs_name"`
    QueueRegion string `json:"sqs_region"`
    QueueWaitTime int64 `json:"sqs_wait_time"`
    QueueVisibilityTimeout int64 `json:"sqs_visibility_timeout"`

    RedisAddr string `json:"redis_addr"`
    RedisPass string `json:"redis_pass"`
    RedisDB int64 `json:"redis_db"`

    AcceptedFileExtensions []string `json:"accepted_file_extensions"`

    S3BucketName string `json:"s3_bucket_name"`
    StoragePath string `json:"storage"`
    StorageMaxLinesPerFile int64 `json:"storage_max_lines_per_file"`

    EngineExecutable string `json:"engine_executable"`
    EngineArgs []string `json:"engine_args"`
}

type Packet struct {
    Action string `json:"action"`
    Payload map[string]interface{} `json:"payload"`
}

func (p Packet) Json() string {
    raw, _ := json.Marshal(p)
    return string(raw)
}

func (p Packet) Send() {
    rds.Publish(isIndexing, p.Json())
}

type IndexingJob struct {
    Type string

    Token string
    RepositoryPath string

    Link string
}

func initConfig() {
    cfg, _ := os.Open("config.json")
    parser := json.NewDecoder(cfg)
    parser.Decode(&config)
}

func initQueue() {
    queue = sqs.New(session.New(), &aws.Config {
        Region: &config.QueueRegion,
    })

    req := sqs.GetQueueUrlInput {
        QueueName: &config.QueueName,
    }
    res, err := queue.GetQueueUrl(&req)
    if err != nil {
        log.Fatal(err)
    }
    queueUrl = *res.QueueUrl

    receive = &sqs.ReceiveMessageInput {
        QueueUrl: &queueUrl,
        WaitTimeSeconds: &config.QueueWaitTime,
        VisibilityTimeout: &config.QueueVisibilityTimeout,
    }
}

func initRedis() {
    rds = redis.NewClient(&redis.Options {
        Addr: config.RedisAddr,
        Password: config.RedisPass,
        DB: config.RedisDB,
    })
}

func main() {
    initConfig()
    initRedis()

    if len(os.Args) < 2 {
        log.Fatal("Expected at least 1 arg")
    }

    mode := os.Args[1]
    switch mode {
    case "index":
        index()
        break
    case "query":
        query()
        break
    }
}

func query() {
    m := martini.Classic()
    m.Post("/", func(w http.ResponseWriter, r *http.Request) string {
        r.ParseForm()
        query, source, token := r.FormValue("query"), r.FormValue("source"), r.FormValue("token")

        sToken, err := rds.Get(query + "|" + source).Result()
        if token == "" || err != nil || sToken != token {
            return "You are not authorized to be here."
        }

        executable := config.EngineExecutable
        args       := append([]string{"-jar"}, config.EngineArgs...)
        args       = append(args, []string{
            executable, "-m", "query", "-i", url.QueryEscape(source) + ".db",
        }...)

        log.Print("Executing java with ", args)
        quer := exec.Command("java", args...)

        read, _ := quer.StdoutPipe()
        scanner := bufio.NewScanner(read)

        p := Packet {
            Action: "results",
            Payload: map[string]interface{} {},
        }

        ccount := 0
        var results []map[string]interface{}
        quer.Start()
        for scanner.Scan() {
            parts := strings.Split(scanner.Text(), ",")
            if len(parts) == 1 {
                p.Payload["files"] = parts[0]
                count, _ := strconv.Atoi(parts[0])
                results = make([]map[string]interface{}, count)
            } else {
                file := parts[0]
                src, _ := ioutil.ReadFile(file)
                start, _ := strconv.Atoi(parts[1])
                end, _ := strconv.Atoi(parts[2])

                lines, relStart, relEnd := extractLines(string(src), start, end)
                j := map[string]interface{}{}
                for k, v := range lines {
                    j[strconv.Itoa(k)] = v
                }
                results[ccount] = map[string]interface{} {
                    "lines": j,
                    "file": file,
                    "relative_start": relStart,
                    "relative_end": relEnd,
                }

                ccount++
            }
        }
        quer.Wait()
        p.Payload["found"] = results
        return p.Json()
    })
    m.RunOnAddr("localhost:3001")
}

func extractLines(src string, start int, end int) (map[int]string, int, int) {
    lines := map[int]string{}

    currentLine, lineStartPos, relativeStartPos, relativeEndPos := 1, 0, 0, 0

    for i := 0; i < start; i++ {
        if src[i] == '\n' {
            currentLine++
            lineStartPos = i + 1
        }
        if i == start - 1 {
            relativeStartPos = i - lineStartPos + 1
        }
    }

    for i := start; i < len(src); i++ {
        if i == end {
            relativeEndPos = i - lineStartPos
        }
        if src[i] == '\n' || i == len(src) - 1 {
            sub := src[lineStartPos : i]
            lines[currentLine] = sub

            if len(lines) == 15 {
                return lines, relativeStartPos, relativeEndPos
            }

            currentLine++
            lineStartPos = i + 1

            if i >= end {
                break
            }
        }
    }

    return lines, relativeStartPos, relativeEndPos
}

func index() {
    isIndexing = ""
    initQueue()

    for {
        if isIndexing != "" {
            continue
        }

        isIndexing = "Reading"

        out, err := queue.ReceiveMessage(receive);
        if err != nil {
            initQueue()
            isIndexing = ""
            continue
        }

        if len(out.Messages) == 0 {
            isIndexing = ""
            continue
        }

        msg := *out.Messages[0]
        receipt := *msg.ReceiptHandle
        var job IndexingJob
        json.Unmarshal([]byte(*msg.Body), &job)

        token, repo, link := job.Token, job.RepositoryPath, job.Link
        var cloneURL, path string

        if job.Type == "github" {
            cloneURL   = "https://" + token + "@github.com/" + repo + ".git"
            isIndexing = repo
            path       = url.QueryEscape(repo)
        } else if job.Type == "link" {
            cloneURL   = link
            isIndexing = link
            path       = url.QueryEscape(link)
        }

        Packet {
            Action: "cloning",
            Payload: map[string]interface{} {
                "status": "started",
            },
        }.Send()

        os.MkdirAll("_repos/" + path, 0777)
        clone := exec.Command("sh", "clone.sh", path, cloneURL)
        clone.Run()
        clone.Wait()

        Packet {
            Action: "cloning",
            Payload: map[string]interface{} {
                "status": "finished",
            },
        }.Send()

        executable := config.EngineExecutable
        args       := append([]string{"-jar"}, config.EngineArgs...)
        args       = append(args, []string{
            executable, "-m", "index", "-i", "_repos/" + path, "-o", path + ".db",
        }...)

        log.Print("Executing java with", args)
        index := exec.Command("java", args...)

        stdout, _ := index.StdoutPipe()
        scanner := bufio.NewScanner(stdout)

        index.Start()
        for scanner.Scan() {
            output := strings.Split(scanner.Text(), ",")
            Packet {
                Action: "indexing",
                Payload: map[string]interface{} {
                    "percent": output[0],
                    "files": output[1],
                    "lines": output[2],
                },
            }.Send()
        }
        index.Wait()

        scp := exec.Command("scp", path + ".db", config.StoragePath)
        scp.Run()
        scp.Wait()

        uploadToS3(path)

        queue.DeleteMessage(&sqs.DeleteMessageInput {
            QueueUrl: &queueUrl,
            ReceiptHandle: &receipt,
        })

        Packet {
            Action: "finished",
            Payload: map[string]interface{} {},
        }.Send()
        isIndexing = ""
    }
}

func uploadToS3(path string) {
    os.MkdirAll("_processedrepos/" + path, 0777)

    filepath.Walk("_repos/" + path, split)
}

func split(path string, info os.FileInfo, err error) error {
    relative := strings.Join(strings.Split(path, "/")[1:], "/")
    if info.IsDir() {
        os.MkdirAll("_processedrepos/" + relative, 0777)
    } else {
        sep := strings.Split(relative, ".")
        if !contains(config.AcceptedFileExtensions, sep[1]) {
            return nil
        }

        lines := []string { "" }

        file, _ := os.Open(path)
        defer file.Close()

        scanner := bufio.NewScanner(file)
        for scanner.Scan() {
            lines = append(lines, scanner.Text())
        }

        limit := 20
        if len(lines) <= limit {
            return nil
        }

        for i := 0; i != len(lines) / limit + 1; i++ {
            min, max := (i * limit) + 1, (i + 1) * limit

            newf, _ := os.Create("_processedrepos/" + sep[0] + "-L" + strconv.Itoa(min) + "-L" + strconv.Itoa(max) + "." + sep[1])
            defer newf.Close()
            for ; min != max + 1; min++ {
                if len(lines) <= min {
                    break
                }
                newf.Write([]byte(lines[min] + "\n"))
            }
            newf.Sync()
        }

    }
    return nil
}

func contains(slice []string, el string) bool {
    for _, a := range slice {
        if a == el {
            return true
        }
    }
    return false
}
