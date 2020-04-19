package main

import (
	"encoding/json"
	msgpack "github.com/vmihailenco/msgpack"
	"strconv"
	"strings"
)

type Message struct {
	Topic   string
	Payload []byte
	Values []string
	Keys   []float64
}

func MsgParse(payload []byte) (map[string]interface{}, error) {
	var j map[string]interface{}

	// first, try msgpack
	err := msgpack.Unmarshal(payload, &j)
	if err != nil {
		// next, try json
		err := json.Unmarshal(payload, &j)
		if err != nil {
			// try plain numbers
			s := string(payload)
			if strings.Contains(s, ".") {
				value, err := strconv.ParseFloat(s, 64)
				if err == nil {
					j = map[string]interface{}{"value": value}
				} else {
					return j, err
				}
			} else {
				value, err := strconv.ParseInt(s, 10, 64)
				if err == nil {
					j = map[string]interface{}{"value": value}
				}else {
					return j, err
				}
			}
		}
	}

	if _, ok := j["time"]; ok {
		j["_time"] = j["time"]
		delete(j, "time")
	}
	return j, nil
}
