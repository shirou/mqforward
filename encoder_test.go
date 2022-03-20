package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Encoder(t *testing.T) {
	assert := assert.New(t)
	msgs := []Message{
		{
			Topic:   "a/b",
			Payload: []byte(`{"x": 1, "y": 2}`),
		},
		{
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

func Test_Tags(t *testing.T) {
	assert := assert.New(t)
	msgs := []Message{
		{
			Topic:   "a/b",
			Payload: []byte(`{"x": 1, "y": 2, "loc": "top"}`),
		},
		{
			Topic:   "a/b",
			Payload: []byte(`{"x": 1, "y": 2, "loc": "top"}`),
		},
	}
	conf := &InfluxDBConf{
		Db:             "db",
		TagsAttributes: []string{"loc"},
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

		tags := map[string]string{
			"topic": "a/b",
			"loc":   "top",
		}
		actualTags := r.Tags()
		assert.Equal(tags, actualTags)
	}
}

func Test_TagMapping(t *testing.T) {
	assert := assert.New(t)
	msgs := []Message{
		{
			Topic:   "p/a/b",
			Payload: []byte(`{"x": 1, "y": 2}`),
		},
		{
			Topic:   "p/a/b",
			Payload: []byte(`{"x": 1, "y": 2}`),
		},
	}
	conf := &InfluxDBConf{
		Db:         "db",
		TopicMap:   []string{"p/{loc}/{sensor}"},
		NoTopicTag: true,
	}
	coder := NewMqttSeriesEncoder(conf)

	ret := coder.Encode(msgs)
	assert.Equal(2, len(ret.Points()))
	for _, r := range ret.Points() {
		assert.Equal("p.a.b", r.Name())
		e := map[string]interface{}{
			"x": float64(1),
			"y": float64(2),
		}
		actual, err := r.Fields()
		assert.NoError(err)
		assert.Equal(e, actual)

		tags := map[string]string{
			"loc":    "a",
			"sensor": "b",
		}
		actualTags := r.Tags()
		assert.Equal(tags, actualTags)
	}
}
