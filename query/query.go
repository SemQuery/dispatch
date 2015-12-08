package query

import (
    "github.com/go-martini/martini"
    "github.com/semquery/dispatch/common"

    "bufio"
    "strings"
    "strconv"
    "net/url"
    "os/exec"
    "net/http"
)

func Start() {
    m := martini.Classic()
    m.Post("/", func(w http.ResponseWriter, r *http.Request) string {
        r.ParseForm()
        query, source, token := r.FormValue("query"), r.FormValue("source"), r.FormValue("token")

        sToken, err := common.Redis.Get(query + "|" + source).Result()
        if token == "" || err != nil || sToken != token {
            return "You are not authorized to be here."
        }

        executable := common.Config.EngineExecutable
        args       := append([]string{"-jar"}, common.Config.EngineArgs...)
        args       = append(args, []string{
            executable, "-m", "query", "-i", url.QueryEscape(source) + ".db",
        }...)

        quer := exec.Command("java", args...)

        read, _ := quer.StdoutPipe()
        scanner := bufio.NewScanner(read)

        p := common.Packet {
            Action: "results",
            Payload: map[string]interface{} {},
        }

        var results []map[string]interface{}
        for scanner.Scan() {
            parts := strings.Split(scanner.Text(), ",")
            if len(parts) == 1 {
                p.Payload["files"] = parts[0]
                count, _          := strconv.Atoi(parts[0])
                results            = make([]map[string]interface{}, count)
            } else {
                results = append(results, map[string]interface{} {
                    "src":   parts[0],
                    "start": parts[1],
                    "end":   parts[2],
                })
            }
        }
        p.Payload["results"] = results
        return p.Json()
    })
    m.RunOnAddr("localhost:3001")
}
