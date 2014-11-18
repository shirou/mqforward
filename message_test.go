package main

import (
	"fmt"
	"reflect"
	"testing"
)

func Test_MsgParse(t *testing.T) {
	p := []byte(`{"x": 2, "y": 4}`)

	keys, values, err := MsgParse(p)
	if err != nil {
		t.Error(err)
	}
	e1 := []string{"x", "y"}
	if reflect.DeepEqual(keys, e1) == false {
		t.Error("could not get keys")
	}
	e2 := fmt.Sprintln("%v", []int{2, 4})
	if fmt.Sprintln("%v", values) != e2 {
		t.Error("could not get values")
	}

}
