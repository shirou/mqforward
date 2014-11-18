package main

import (
	"reflect"
	"testing"
)

func Test_Msg2Series(t *testing.T) {

	msgs := []Message{
		Message{
			Topic:   "a/b",
			Payload: []byte(`{"x": 1, "y": 2}`),
		},
		Message{
			Topic:   "a/b",
			Payload: []byte(`{"x": 1, "y": 2}`),
		},
	}

	ret := Msg2Series(msgs)
	for _, r := range ret {
		if r.Name != "a.b" {
			t.Error("wrong name")
		}
		e := []string{"x", "y"}
		if reflect.DeepEqual(r.Columns, e) == false {
			t.Error("could not get keys")
		}

		if len(r.Points) != 2 {
			t.Error("should be 2")
		}
	}
}
