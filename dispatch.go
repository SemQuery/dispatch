package main

import (
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/service/sqs"

    "gopkg.in/redis.v3"

    "os"
    "os/exec"
    "bufio"
)

var (
    rds *redis.Client

    queue *sqs.SQS
    receive *sqs.ReceiveMessageInput
    send *sqs.SendMessageInput

    isIndexing string
)

func initQueue() {
    queue = sqs.New(&aws.Config {
        Region: aws.String(""),
    })

    queueURL := ""
    receive = &sqs.ReceiveMessageInput {
        QueueUrl: &queueURL,
        WaitTimeSeconds: aws.Int64(1000),
    }
    send = &sqs.SendMessageInput {
        QueueUrl: &queueURL,
    }
}

func initRedis() {
    rds = redis.NewClient(&redis.Options {
        Addr: "",
        Password: "",
        DB: 0,
    })
}

func main() {
    isIndexing = ""
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
            rds.Publish(path, "Starting Clone")

            os.MkdirAll("_repos" + path, 0777)
            cloneURL := "https://" + token + "@github.com/" + path + ".git"
            clone := exec.Command("cd", "_repos/" + path + ";", "git", "init;", "git", "pull", cloneURL)
            clone.Run()
            clone.Wait()

            //Update redis db with clone progress here

            executable := ""
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
