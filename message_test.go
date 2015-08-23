package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MsgParse(t *testing.T) {
	assert := assert.New(t)

	p := []byte(`{"x": 2, "y": 4}`)

	j, err := MsgParse(p)
	assert.Nil(err)

	assert.True(2 == j["x"].(float64))
	assert.True(4 == j["y"].(float64))
}
