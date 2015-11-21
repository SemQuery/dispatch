package main

import (
    "github.com/go-martini/martini"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/sqs"

    "gopkg.in/redis.v3"

    "os"
    "os/exec"
    "strings"
    "strconv"
    "net/http"
    "net/url"
    "bufio"
    "encoding/json"
    "io/ioutil"
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

    StoragePath string `json:"storage"`

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

    if os.Args[1] == "index" {
        index()
    } else if os.Args[1] == "query" {
        query()
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

        func() {
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
        }()
        index.Wait()

        scp := exec.Command("scp", path + ".db", config.StoragePath)
        scp.Run()
        scp.Wait()

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
