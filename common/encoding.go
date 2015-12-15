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

// generic string -> any map
type M map[string]interface{}

type Packet struct {
    Action string `json:"action"`
    Payload M `json:"payload"`
}

func (p Packet) Json() string {
    raw, _ := json.Marshal(p)
    return string(raw)
}
