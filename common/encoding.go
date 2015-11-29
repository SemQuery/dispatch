package common

import (
    "encoding/json"
)

const (
    IndexingAction = "indexing"
    CloningAction  = "cloning"
    SyncingAction  = "sync"
    FinishedAction = "finished"

    StartedStatus  = "started"
    FinishedStatus = "finished"
)

type Packet struct {
    Action string `json:"action"`
    Payload map[string]interface{} `json:"payload"`
}

func (p Packet) Json() string {
    raw, _ := json.Marshal(p)
    return string(raw)
}
