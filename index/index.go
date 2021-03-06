package index

import (
    "github.com/aws/aws-sdk-go/aws"
    "github.com/semquery/dispatch/common"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/sqs"
    "github.com/aws/aws-sdk-go/service/s3/s3manager"

    "io"
    "os"
    "log"
    "fmt"
    "bufio"
    "strings"
    "net/url"
    "os/exec"
    "path/filepath"
    "encoding/json"
    "compress/gzip"
)

var (
    queue *sqs.SQS
    queueUrl string

    uploader *s3manager.Uploader
    encoding string = "gzip"
)

type IndexingJob struct {
    Type string
    ID string

    Token string
    URL string
}

func InitAWS() {
    sess := session.New(&aws.Config {
        Region: &common.Config.QueueRegion,
    })

    queue = sqs.New(sess, sess.Config)

    res, err := queue.GetQueueUrl(&sqs.GetQueueUrlInput {
        QueueName: &common.Config.QueueName,
    })
    if err != nil {
        log.Fatal(err)
    }
    queueUrl = *res.QueueUrl

    uploader = s3manager.NewUploader(sess)
}

func send(id string, p common.Packet) {
    common.Redis.Publish("indexing:" + id, p.Json())
}

func Start() {
    receiveInput := &sqs.ReceiveMessageInput {
        QueueUrl: &queueUrl,
        WaitTimeSeconds: &common.Config.QueueWaitTime,
        VisibilityTimeout: &common.Config.QueueVisibilityTimeout,
    }

    for {

        out, err := queue.ReceiveMessage(receiveInput);
        if err != nil {
            InitAWS()
            continue
        }

        if len(out.Messages) == 0 {
            continue
        }

        msg := *out.Messages[0]
        var job IndexingJob
        json.Unmarshal([]byte(*msg.Body), &job)

        queue.DeleteMessage(&sqs.DeleteMessageInput {
            QueueUrl: &queueUrl,
            ReceiptHandle: msg.ReceiptHandle,
        })

        URL, _ := url.Parse(job.URL)
        URL.User = url.User(job.Token)
        cloneURL := URL.String()

        path := url.QueryEscape(job.URL)

        send(job.ID, common.Packet {
            Action: common.CloningAction,
            Payload: common.M{
                "status": common.StartedStatus,
            },
        })

        os.MkdirAll("_repos/" + path, 0777)
        clone := exec.Command("sh", "clone.sh", path, cloneURL)
        clone.Run()
        clone.Wait()

        send(job.ID, common.Packet {
            Action: common.CloningAction,
            Payload: common.M{
                "status": common.FinishedStatus,
            },
        })

        executable := common.Config.EngineExecutable
        args       := append([]string{"-jar"}, common.Config.EngineArgs...)
        args       = append(args, []string{
            executable, "-m", "index", "-i", "_repos/" + path, "-o", job.ID + ".db",
        }...)

        log.Print("Executing java with args: ", args)

        index := exec.Command("java", args...)

        stdout, _ := index.StdoutPipe()
        scanner := bufio.NewScanner(stdout)

        index.Start()
        for scanner.Scan() {
            output := strings.Split(scanner.Text(), ",")
            send(job.ID, common.Packet {
                Action: common.IndexingAction,
                Payload: common.M{
                    "percent": output[0],
                    "files": output[1],
                    "lines": output[2],
                },
            })
        }
        err = index.Wait()
        if err != nil {
            log.Fatal("Bad indexing command ", err)
        }

        send(job.ID, common.Packet {
            Action: common.SyncingAction,
            Payload: common.M{
                "status": common.StartedStatus,
            },
        })

        scp := exec.Command("scp", job.ID + ".db", common.Config.StoragePath)
        err = scp.Run()
        if err != nil {
            log.Fatal("scp error: ", err)
        }

        filepath.Walk("_repos/" + path, upload)

        send(job.ID, common.Packet {
            Action: common.SyncingAction,
            Payload: common.M{
                "status": common.FinishedStatus,
            },
        })

        rm := exec.Command("rm", "-rf", "_repos/" + path)
        err = rm.Run()
        if err != nil {
            log.Fatal("rm error: ", err)
        }

        send(job.ID, common.Packet {
            Action: common.FinishedAction,
            Payload: common.M{},
        })
    }
}

func upload(path string, info os.FileInfo, err error) error {
    relative := strings.Join(strings.Split(path, "/")[1:], "/")
    if !info.IsDir() {
        ext := strings.TrimPrefix(filepath.Ext(path), ".")
        if !contains(common.Config.AcceptedFileExtensions, ext) {
            return nil
        }

        file, _ := os.Open(path)
        reader, writer := io.Pipe()

        go func() {
            gw := gzip.NewWriter(writer)
            io.Copy(gw, file)

            file.Close()
            gw.Close()
            writer.Close()
        }()

        uploader.Upload(&s3manager.UploadInput {
            Body: reader,
            Key: &relative,
            Bucket: &common.Config.S3BucketName,
        })
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
