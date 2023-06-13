package main

import (
	"testing"

	lp "github.com/influxdata/line-protocol"
	"github.com/stretchr/testify/assert"
)

func Test_Encoder(t *testing.T) {
	assert := assert.New(t)
	msg := Message{
		Topic:   "a/b",
		Payload: []byte(`{"x": 1, "y": 2}`),
	}
	conf := &InfluxDBConf{
		Db:             "db",
		TagsAttributes: []string{},
	}
	coder := NewMqttSeriesEncoder(conf)

	ret := coder.Encode(msg)
	assert.NotNil(ret)
	assert.Equal("a.b", ret.Name())
	e := []*lp.Field{
		{
			Key:   "x",
			Value: float64(1),
		},
		{
			Key:   "y",
			Value: float64(2),
		},
	}
	actual := ret.FieldList()
	for i := 0; i < len(e); i++ {
		found := false
		for j := 0; j < len(actual); j++ {
			if e[i].Key == actual[j].Key {
				assert.Equalf(e[i], actual[j], "test field %d does not match with actual tag %d", i, j)
				found = true
				break
			}
		}
		assert.True(found, "test field %d is not in actual tags", i)
	}
}

func Test_Tags(t *testing.T) {
	assert := assert.New(t)
	msg := Message{
		Topic:   "a/b",
		Payload: []byte(`{"x": 1, "y": 2, "loc": "top"}`),
	}
	conf := &InfluxDBConf{
		Db:             "db",
		TagsAttributes: []string{"loc"},
	}
	coder := NewMqttSeriesEncoder(conf)

	ret := coder.Encode(msg)
	assert.NotNil(ret)
	assert.Equal("a.b", ret.Name())
	e := []*lp.Field{
		{
			Key:   "x",
			Value: float64(1),
		},
		{
			Key:   "y",
			Value: float64(2),
		},
	}
	actual := ret.FieldList()
	assert.Equal(len(e), len(actual))
	for i := 0; i < len(e); i++ {
		found := false
		for j := 0; j < len(actual); j++ {
			if e[i].Key == actual[j].Key {
				assert.Equalf(e[i], actual[j], "test field %d does not match with actual tag %d", i, j)
				found = true
				break
			}
		}
		assert.True(found, "test field %d is not in actual tags", i)
	}

	tags := []*lp.Tag{
		{
			Key:   "topic",
			Value: "a/b",
		},
		{
			Key:   "loc",
			Value: "top",
		},
	}
	actualTags := ret.TagList()
	assert.Equal(len(tags), len(actualTags))
	for i := 0; i < len(tags); i++ {
		found := false
		for j := 0; j < len(actualTags); j++ {
			if tags[i].Key == actualTags[j].Key {
				assert.Equalf(tags[i], actualTags[j], "test tag %d does not match with actual tag %d", i, j)
				found = true
				break
			}
		}
		assert.True(found, "test tag %d is not in actual tags", i)
	}
}

func Test_TagMapping(t *testing.T) {
	assert := assert.New(t)
	msg := Message{
		Topic:   "p/a/b",
		Payload: []byte(`{"x": 1, "y": 2}`),
	}
	conf := &InfluxDBConf{
		Db:         "db",
		TopicMap:   []string{"p/{loc}/{sensor}"},
		NoTopicTag: true,
		Series:     "data",
	}
	coder := NewMqttSeriesEncoder(conf)

	ret := coder.Encode(msg)
	assert.NotNil(ret)
	assert.Equal("data", ret.Name())
	e := []*lp.Field{
		{
			Key:   "x",
			Value: float64(1),
		},
		{
			Key:   "y",
			Value: float64(2),
		},
	}
	actual := ret.FieldList()
	assert.Equal(len(e), len(actual))
	for i := 0; i < len(e); i++ {
		found := false
		for j := 0; j < len(actual); j++ {
			if e[i].Key == actual[j].Key {
				assert.Equalf(e[i], actual[j], "test field %d does not match with actual tag %d", i, j)
				found = true
				break
			}
		}
		assert.True(found, "test field %d is not in actual tags", i)
	}

	tags := []*lp.Tag{
		{
			Key:   "loc",
			Value: "a",
		},
		{
			Key:   "sensor",
			Value: "b",
		},
	}

	actualTags := ret.TagList()
	assert.Equal(len(tags), len(actualTags))
	for i := 0; i < len(tags); i++ {
		found := false
		for j := 0; j < len(actualTags); j++ {
			if tags[i].Key == actualTags[j].Key {
				assert.Equalf(tags[i], actualTags[j], "test tag %d does not match with actual tag %d", i, j)
				found = true
				break
			}
		}
		assert.True(found, "test tag %d is not in actual tags", i)
	}
}
