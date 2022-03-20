package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Encoder(t *testing.T) {
	assert := assert.New(t)
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
	conf := &InfluxDBConf{
		Db:             "db",
		TagsAttributes: []string{},
	}
	coder := NewMqttSeriesEncoder(conf)

	ret := coder.Encode(msgs)
	assert.Equal(2, len(ret.Points()))
	for _, r := range ret.Points() {
		assert.Equal("a.b", r.Name())
		e := map[string]interface{}{
			"x": float64(1),
			"y": float64(2),
		}
		actual, err := r.Fields()
		assert.NoError(err)
		assert.Equal(e, actual)
	}
}
