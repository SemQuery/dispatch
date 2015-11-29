package main

import (
    "github.com/semquery/dispatch/common"
    "github.com/semquery/dispatch/query"
    "github.com/semquery/dispatch/index"

    "os"
    "log"
)

func main() {
    common.InitConfig()
    common.InitRedis()

    if len(os.Args) < 2 {
        log.Fatal("Expected at least 1 arg")
    }

    switch os.Args[1] {
    case "index":
        index.InitAWS()
        index.Start()
        break
    case "query":
        query.Start()
        break
    }
}
