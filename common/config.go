package common

import (
    "os"
    "encoding/json"
)

var Config DispatchConfig

type DispatchConfig struct {
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
    StorageMaxLinesPerFile int `json:"storage_max_lines_per_file"`

    EngineExecutable string `json:"engine_executable"`
    EngineArgs []string `json:"engine_args"`
}

func InitConfig() {
    cfg, _ := os.Open("config.json")
    parser := json.NewDecoder(cfg)
    parser.Decode(&Config)
}


