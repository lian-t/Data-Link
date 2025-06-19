package main

import (
	"data-link-2.0/datalink/linkd"
	"encoding/json"
	"io/ioutil"
	"testing"
)

func TestValid(t *testing.T) {
	path := "D:\\workspace\\data-link-2.0\\examples\\mysql2txt.json"
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return
	}
	c := map[string]interface{}{}
	json.Unmarshal(bytes, &c)
	err = linkd.ValidTaskMap(c)
	if err != nil {
		t.Error(err)
	}
}
