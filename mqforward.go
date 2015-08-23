package main

import (
	"fmt"

	log "github.com/Sirupsen/logrus"
)

const (
	StatusStopped = "stopped"
	StatusStarted = "started"

	MaxBufferSize = 4 // bufferd size to send influxDB
)

type Forwarder struct {
	mqclient *MqttClient
	ifclient *InfluxDBClient

	mqttChan    chan Message
	ifChan      chan Message
	commandChan chan string
}

func NewForwarder(mqttconf MqttConf, ifconf InfluxDBConf) (*Forwarder, error) {
	// channel from MQTT
	mqttChan := make(chan Message, MaxBufferSize)
	// channel  to InfluxDB
	ifChan := make(chan Message, MaxBufferSize)
	// channel main to MQTT
	commandChan := make(chan string)

	mqclient, err := NewMqttClient(mqttconf, mqttChan, commandChan)
	if err != nil {
		return nil, fmt.Errorf("mqtt init err: %s", err)
	}
	ifclient, err := NewInfluxDBClient(ifconf, ifChan, commandChan)
	if err != nil {
		return nil, fmt.Errorf("influxdb init err: %s", err)
	}
	go ifclient.Start()

	return &Forwarder{
		mqclient:    mqclient,
		ifclient:    ifclient,
		mqttChan:    mqttChan,
		ifChan:      ifChan,
		commandChan: commandChan,
	}, nil
}

func (f *Forwarder) Start() error {
	for {
		select {
		case msg, ok := <-f.mqttChan:
			if !ok {
				return fmt.Errorf("msg pipe closed")
			}
			log.Debug("msg comes from mqtt")
			f.ifChan <- msg
		}
	}

	return fmt.Errorf("quit start loop")
}
