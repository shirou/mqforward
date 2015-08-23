package main

import (
	"encoding/json"

	msgpack "gopkg.in/vmihailenco/msgpack.v1"
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
		if err != nil { // fail
			return j, err
		}
	}
	if _, ok := j["time"]; ok {
		j["_time"] = j["time"]
		delete(j, "time")
	}
	return j, nil
}
