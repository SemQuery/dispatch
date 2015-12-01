package index

import (
    "github.com/aws/aws-sdk-go/aws"
    "github.com/semquery/dispatch/common"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/sqs"
    "github.com/aws/aws-sdk-go/service/s3/s3manager"

    "os"
    "log"
    "bufio"
    "strconv"
    "strings"
    "net/url"
    "os/exec"
    "path/filepath"
    "encoding/json"
)

var (
    queue *sqs.SQS
    queueUrl string

    uploader *s3manager.Uploader
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
            Payload: map[string]interface{} {
                "status": common.StartedStatus,
            },
        })

        os.MkdirAll("_repos/" + path, 0777)
        clone := exec.Command("sh", "clone.sh", path, cloneURL)
        clone.Run()
        clone.Wait()

        send(job.ID, common.Packet {
            Action: common.CloningAction,
            Payload: map[string]interface{} {
                "status": common.FinishedStatus,
            },
        })

        executable := common.Config.EngineExecutable
        args       := append([]string{"-jar"}, common.Config.EngineArgs...)
        args       = append(args, []string{
            executable, "-m", "index", "-i", "_repos/" + path, "-o", path + ".db",
        }...)

        index := exec.Command("java", args...)

        stdout, _ := index.StdoutPipe()
        scanner := bufio.NewScanner(stdout)

        index.Start()
        for scanner.Scan() {
            output := strings.Split(scanner.Text(), ",")
            send(job.ID, common.Packet {
                Action: common.IndexingAction,
                Payload: map[string]interface{} {
                    "percent": output[0],
                    "files": output[1],
                    "lines": output[2],
                },
            })
        }
        index.Wait()

        send(job.ID, common.Packet {
            Action: common.SyncingAction,
            Payload: map[string]interface{} {
                "status": common.StartedStatus,
            },
        })

        scp := exec.Command("scp", path + ".db", common.Config.StoragePath)
        scp.Run()

        os.MkdirAll("_processedrepos/" + path, 0777)
        filepath.Walk("_repos/" + path, upload)

        send(job.ID, common.Packet {
            Action: common.SyncingAction,
            Payload: map[string]interface{} {
                "status": common.FinishedStatus,
            },
        })

        rm := exec.Command("rm", "-rf", path + ".db", "_processedrepos/" + path, "_repos/" + path)
        rm.Run()

        send(job.ID, common.Packet {
            Action: common.FinishedAction,
            Payload: map[string]interface{} {},
        })
    }
}

func upload(path string, info os.FileInfo, err error) error {
    relative := strings.Join(strings.Split(path, "/")[1:], "/")
    if info.IsDir() {
        os.MkdirAll("_processedrepos/" + relative, 0777)
    } else {
        sep := strings.Split(relative, ".")
        if !contains(common.Config.AcceptedFileExtensions, sep[1]) {
            return nil
        }

        lines := []string { "" }

        file, _ := os.Open(path)
        defer file.Close()

        scanner := bufio.NewScanner(file)
        for scanner.Scan() {
            lines = append(lines, scanner.Text())
        }

        limit := common.Config.StorageMaxLinesPerFile

        for i := 0; i != len(lines) / limit + 1; i++ {
            min, max := (i * limit) + 1, (i + 1) * limit

            newn := "_processedrepos/" + sep[0] + "-L" + strconv.Itoa(min) + "-L" + strconv.Itoa(max) + "." + sep[1]
            newf, _ := os.Create(newn)
            defer newf.Close()
            for ; min != max + 1; min++ {
                if len(lines) <= min {
                    break
                }
                newf.Write([]byte(lines[min] + "\n"))
            }
            newf.Sync()

            uploader.Upload(&s3manager.UploadInput {
                Body: newf,
                Bucket: &common.Config.S3BucketName,
                Key: &newn,
            })
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
