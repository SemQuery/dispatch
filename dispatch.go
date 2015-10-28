package main

import (
    "github.com/go-martini/martini"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/service/sqs"

    "gopkg.in/redis.v3"
    "gopkg.in/mgo.v2"

    "os"
    "os/exec"
    "strings"
    "strconv"
    "net/http"
    "bufio"
    "encoding/json"
    "io/ioutil"
    "log"
)

var (
    config Config

    rds *redis.Client
    mongod *mgo.Database

    queue *sqs.SQS
    receive *sqs.ReceiveMessageInput
    send *sqs.SendMessageInput

    isIndexing string
)

type Config struct {
    QueueRegion string `json:"queue_region"`
    QueueURL string `json:"queue_url"`
    QueueWaitTime int64 `json:"queue_wait_time"`

    DBAddr string `json:"db_addr"`
    DBName string `json:"db_name"`
    DBUser string `json:"db_user"`
    DBPass string `json:"db_pass"`

    RedisAddr string `json:"redis_addr"`
    RedisPass string `json:"redis_pass"`
    RedisDB int64 `json:"redis_db"`

    StoragePath string `json:"storage"`

    EngineExecutable string `json:"engine_executable"`
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

func initConfig() {
    cfg, err := os.Open("config.json")
    if err != nil {
        log.Fatal(err)
    }
    parser := json.NewDecoder(cfg)
    if err = parser.Decode(config); err != nil {
        log.Fatal(err)
    }
}

func initQueue() {
    queue = sqs.New(&aws.Config {
        Region: aws.String(""),
    })

    queueURL := config.QueueURL
    receive = &sqs.ReceiveMessageInput {
        QueueUrl: &queueURL,
        WaitTimeSeconds: aws.Int64(config.QueueWaitTime),
    }
    send = &sqs.SendMessageInput {
        QueueUrl: &queueURL,
    }
}

func initRedis() {
    rds = redis.NewClient(&redis.Options {
        Addr: config.RedisAddr,
        Password: config.RedisPass,
        DB: config.RedisDB,
    })
}

func initMongoDB() {
    session, err := mgo.DialWithInfo(&mgo.DialInfo{
        Addrs: []string{config.DBAddr},
        Database: config.DBName,
        Username: config.DBUser,
        Password: config.DBPass,
    })
    if err != nil {
        log.Fatal(err)
    }

    mongod = session.DB(config.DBName)
}

func main() {
    initConfig()
    initRedis()
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
        query, repo, token := r.FormValue("query"), r.FormValue("repo"), r.FormValue("token")

        sToken, err := rds.Get(query + "|" + repo).Result()
        if token == "" || err != nil || sToken != token {
            return "You are not authorized to be here."
        }

        executable := config.EngineExecutable
        quer := exec.Command("java", "-jar", executable, "_repos/" + repo, query)
        quer.Run()

        read, _ := quer.StdoutPipe()
        scanner := bufio.NewScanner(read)

        p := Packet {
            Action: "Results",
            Payload: map[string]interface{} {},
        }

        ccount := 0
        var results []map[string]interface{}
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
                results[ccount] = map[string]interface{} {
                    "lines": lines,
                    "file": file,
                    "relative_start": relStart,
                    "relative_end": relEnd,
                }

                ccount++
            }
        }
        return p.Json()
    })
    m.Run()
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
    initMongoDB()

    for {
        if isIndexing != "" {
            continue
        }

        out, err := queue.ReceiveMessage(receive);
        if err != nil {
            continue
        }

        path := *out.Messages[0].MessageAttributes["path"].StringValue
        token := *out.Messages[0].MessageAttributes["token"].StringValue

        if err = rds.Get(path).Err(); err != nil {
            isIndexing = path

            os.MkdirAll("_repos/" + path, 0777)
            cloneURL := "https://" + token + "@github.com/" + path + ".git"
            clone := exec.Command("cd", "_repos/" + path + ";", "git", "init;", "git", "pull", cloneURL)
            clone.Run()
            clone.Wait()

            //Update redis db with clone progress here

            executable := config.EngineExecutable
            index := exec.Command("java", "-jar", executable, "_repos/" + path, path)

            stdout, _ := index.StdoutPipe()
            scanner := bufio.NewScanner(stdout)

            go func() {
                index.Start()
                for scanner.Scan() {
                    output := strings.Split(scanner.Text(), ",")
                    Packet {
                        Action: "Indexing",
                        Payload: map[string]interface{} {
                            "percent": output[0],
                            "files": output[1],
                            "lines": output[2],
                        },
                    }.Send()
                }
            }()
            index.Wait()

            scp := exec.Command("scp", "", config.StoragePath + "/" + path)
            scp.Run()
            scp.Wait()

            Packet {
                Action: "Finished",
                Payload: map[string]interface{} {},
            }.Send()
            isIndexing = ""
        }
    }
}
