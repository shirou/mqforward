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

func MsgParse(payload []byte) ([]string, []interface{}, error) {
	var j map[string]interface{}

	// first, try msgpack
	err := msgpack.Unmarshal(payload, &j)
	if err != nil {
		// next, try json
		err := json.Unmarshal(payload, &j)
		if err != nil { // fail
			return []string{}, make([]interface{}, 0, 0), err
		}
	}

	keys := make([]string, 0, len(j))
	values := make([]interface{}, 0, len(j))
	for k, v := range j {
		keys = append(keys, k)
		values = append(values, v)
	}

	return keys, values, nil
}
