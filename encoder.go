package main

import (
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	influxdb "github.com/influxdata/influxdb1-client/v2"
)

type MqttSeriesEncoder struct {
	Config *InfluxDBConf
}

func NewMqttSeriesEncoder(conf *InfluxDBConf) *MqttSeriesEncoder {
	return &MqttSeriesEncoder{
		Config: conf,
	}
}

func (ifc *MqttSeriesEncoder) Encode(msgs []Message) influxdb.BatchPoints {
	now := time.Now()

	// Create a new point batch
	bp, err := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
		Database:  ifc.Config.Db,
		Precision: "s",
	})

	if err != nil {
		log.Warn(err)
		return nil
	}

	for _, msg := range msgs {
		if msg.Topic == "" && len(msg.Payload) == 0 {
			break
		}
		j, err := MsgParse(msg.Payload)
		if err != nil {
			log.Warn(err)
			continue
		}

		name := strings.Replace(msg.Topic, "/", ".", -1)

		// Store default tag attributes
		tags := map[string]string{
			"topic": msg.Topic,
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

		pt, err := influxdb.NewPoint(name, tags, j, now)
		if err != nil {
			log.Warn(err)
			continue
		}
		bp.AddPoint(pt)
	}

	return bp
}
