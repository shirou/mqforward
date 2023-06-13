package main

import (
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
)

type MqttSeriesEncoder struct {
	Config   *InfluxDBConf
	matchers []TopicMatcher
}

func createTopicMatcher(topicMap []string) []TopicMatcher {
	m := []TopicMatcher{}

	for _, t := range topicMap {
		m = append(m, *NewTopicMatcher(t))
	}

	return m
}

func NewMqttSeriesEncoder(conf *InfluxDBConf) *MqttSeriesEncoder {
	return &MqttSeriesEncoder{
		Config:   conf,
		matchers: createTopicMatcher(conf.TopicMap),
	}
}

func (ifc *MqttSeriesEncoder) Encode(msg Message) *write.Point {
	now := time.Now()

	if msg.Topic == "" && len(msg.Payload) == 0 {
		return nil
	}
	j, err := MsgParse(msg.Payload)
	if err != nil {
		log.Warn(err)
		return nil
	}

	name := ifc.Config.Series
	if len(name) == 0 {
		name = strings.Replace(msg.Topic, "/", ".", -1)
	}

	tags := map[string]string{}

	// Store default tag attributes
	if !ifc.Config.NoTopicTag {
		tags["topic"] = msg.Topic
	}

	// Transform user-defined JSON fields to tags
	for _, tag := range ifc.Config.TagsAttributes {
		if v, ok := j[tag]; ok {
			if tagVal, ok := v.(string); ok {
				tags[tag] = tagVal
				delete(j, tag)
			}
		}
	}

	// Append first match from TopicMap
	for _, m := range ifc.matchers {
		b, v := m.Match(msg.Topic)
		if b {
			for tag, tagVal := range v {
				tags[tag] = tagVal
			}
			break
		}
	}

	return influxdb2.NewPoint(name, tags, j, now)
}
