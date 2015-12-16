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
    m.Post("/", func(w http.ResponseWriter, r *http.Request) (string, int) {
        r.ParseForm()
        query, source := r.FormValue("query"), r.FormValue("source")

        executable := common.Config.EngineExecutable
        args       := append([]string{"-jar"}, common.Config.EngineArgs...)
        args       = append(args, []string{
            executable, "-m", "query", "-i", url.QueryEscape(source) + ".db", "-q", query,
        }...)

        queryCmd := exec.Command("java", args...)

        read, err := queryCmd.StdoutPipe()
        if err != nil {
            return err.Error(), 500
        }
        scanner := bufio.NewScanner(read)

        p := common.Packet {
            Action: "results",
            Payload: common.M{},
        }

        var results []common.M
        for scanner.Scan() {
            parts := strings.Split(scanner.Text(), ",")
            if len(parts) == 1 {
                p.Payload["files"] = parts[0]
                count, _          := strconv.Atoi(parts[0])
                results            = make([]common.M, count)
            } else {
                results = append(results, common.M{
                    "src":   parts[0],
                    "start": parts[1],
                    "end":   parts[2],
                })
            }
        }
        p.Payload["results"] = results
        return p.Json(), 200
    })
    m.RunOnAddr("localhost:3001")
}
