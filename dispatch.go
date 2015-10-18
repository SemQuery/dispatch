package main

import (
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/service/sqs"

    "gopkg.in/redis.v3"

    "os"
    "os/exec"
    "bufio"
    "encoding/json"
    "log"
)

var (
    config Config

    rds *redis.Client

    queue *sqs.SQS
    receive *sqs.ReceiveMessageInput
    send *sqs.SendMessageInput

    isIndexing string
)

type Config struct {
    QueueRegion string `json:"queue_region"`
    QueueURL string `json:"queue_url"`
    QueueWaitTime int64 `json:"queue_wait_time"`

    RedisAddr string `json:"redis_addr"`
    RedisPass string `json:"redis_pass"`
    RedisDB int64 `json:"redis_db"`

    EngineExecutable string `json:"engine_executable"`
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

func main() {
    isIndexing = ""
    initConfig()
    initQueue()
    initRedis()

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
            rds.Publish(path, "!Cloning")

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
                    rds.Publish(path, scanner.Text())
                }
                rds.Publish(path, "END")
            }()
            index.Wait()
            isIndexing = ""
        }
    }
}
