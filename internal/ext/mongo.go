package ext

import (
	"encoding/json"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"runtime"
	"strings"
)

func readOut(reader io.Reader, fun func(bm *bson.M)) {
	prevBuf := ""
	currBuf := make([]byte, 1024*8)
	for {
		num, err := reader.Read(currBuf)
		if err != nil {
			if err == io.EOF || strings.Contains(err.Error(), "closed") {
				err = nil
			}
			return
		}
		if num < 1 {
			continue
		}

		str := string(currBuf[:num])
		lines := strings.Split(prevBuf+str, "\n")
		l := len(lines) - 1
		for i, line := range lines {
			var item bson.M
			err := json.Unmarshal([]byte(line), &item)
			if err != nil {
				if i == l {
					prevBuf = line
				}
				continue
			}
			var _id primitive.ObjectID
			oid, ok := item["_id"]
			if ok {
				switch oid.(type) {
				case map[string]interface{}:
					idI := oid.(map[string]interface{})["$oid"]
					switch idI.(type) {
					case string:
						_id, _ = primitive.ObjectIDFromHex(idI.(string))
						item["_id"] = _id
					}
				default:
					// pass
				}
			}
			fun(&item)
		}
	}
}

// MongoDump 使用mongodump导出数据
func MongoDump(execPath string, uri string, ns string, fun func(bm *bson.M, err error), extra map[string]interface{}) (err error) {
	info, err := os.Stat(execPath)
	if err != nil {
		return err
	}
	if runtime.GOOS == "linux" {
		perm := info.Mode().Perm()
		mode := perm & os.FileMode(73)
		if uint32(mode) != uint32(73) {
			return errors.New("no exec permission")
		}
	}

	nss := strings.Split(ns, ".")
	if len(nss) != 2 {
		return errors.New("namespace format error")
	}

	// ./mongoexport.exe  --archive
	// --uri='mongodb://admin:94215b0cb86d9ceb@10.10.10.10:33017/?authsource=admin&connect=direct'
	// --db=dbname
	// --collection=coll
	cmdStr := fmt.Sprintf("%s --uri=\"%s\" --db=%s --collection=%s ", execPath, uri, nss[0], nss[1])
	limit, ok := extra["limit"]
	if ok {
		cmdStr = fmt.Sprintf("%s --limit=%s", cmdStr, limit)
	}
	skip, ok := extra["skip"]
	if ok {
		cmdStr = fmt.Sprintf("%s --skip=%s", cmdStr, skip)
	}

	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("cmd", "/C", cmdStr) // windows
	} else {
		cmd = exec.Command("bash", "-c", cmdStr) // darwin or linux
	}

	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		return err
	}

	go func(stdout io.Reader) {
		readOut(stdout, func(bm *bson.M) {
			fun(bm, nil)
		})
	}(stdout)
	go func(stderr io.Reader) {
		body, err := ioutil.ReadAll(stderr)
		if err != nil {
			fun(nil, err)
			return
		}
		if !strings.Contains(string(body), "done dumping") {
			fun(nil, errors.New(string(body)))
		}
	}(stderr)

	if err := cmd.Wait(); err != nil {
		fun(nil, err)
		return err
	}
	return nil
}
