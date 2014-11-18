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

	ifChan chan Message
}

func NewForwarder(mqttconf MqttConf, ifconf InfluxDBConf) (*Forwarder, error) {
	mqclient, err := NewMqttClient(mqttconf)
	if err != nil {
		return nil, fmt.Errorf("mqtt: %s", err)
	}
	ifclient, err := NewInfluxDBClient(ifconf)
	if err != nil {
		return nil, fmt.Errorf("influxdb: %s", err)
	}

	return &Forwarder{
		mqclient: mqclient,
		ifclient: ifclient,
	}, nil
}

func (f *Forwarder) Start() error {
	command := make(chan string)
	// channel from MQTT to InfluxDB
	f.ifChan = make(chan Message, MaxBufferSize)

	err := f.mqclient.Start(command, f.ifChan)
	if err != nil {
		return err
	}

	err = f.ifclient.Start(command, f.ifChan)
	if err != nil {
		return err
	}

	log.Warn("stopped")
	return fmt.Errorf("mqforward stopped")
}
