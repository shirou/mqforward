package main

import (
	"crypto/rand"
	"fmt"
	"strings"

	MQTT "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
	log "github.com/Sirupsen/logrus"
)

const (
	MaxClientIdLen = 10
)

type MqttConf struct {
	Hostname   string
	Port       int
	Username   string
	Password   string
	Cafilepath string
	Topic      string
	Debug      string
}

type MqttClient struct {
	Client *MQTT.MqttClient
	Opts   *MQTT.ClientOptions
	Config MqttConf

	ifChan chan Message
}

//
// with Connects connect to the MQTT broker with Options.
func NewMqttClient(conf MqttConf) (*MqttClient, error) {
	opts := MQTT.NewClientOptions()

	port := conf.Port
	if port == 0 {
		port = 1883
	}
	scheme := "tcp"
	if port == 8883 {
		scheme = "ssl"
	}
	brokerUri := fmt.Sprintf("%s://%s:%d", scheme, conf.Hostname, port)
	log.Infof("Broker URI: %s", brokerUri)
	opts.AddBroker(brokerUri)

	if conf.Username != "" {
		opts.SetUsername(conf.Username)
	}
	if conf.Password != "" {
		opts.SetPassword(conf.Password)
	}

	clientId := getRandomClientId()
	opts.SetClientId(clientId)

	mc := MQTT.NewClient(opts)
	_, err := mc.Start()
	if err != nil {
		return nil, err
	}
	ret := &MqttClient{
		Client: mc,
		Opts:   opts,
		Config: conf,
	}

	return ret, nil
}

// getRandomClientId returns randomized ClientId.
func getRandomClientId() string {
	const alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, MaxClientIdLen)
	rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphanum[b%byte(len(alphanum))]
	}
	return "mqttcli-" + string(bytes)
}

func (m *MqttClient) onMessageReceived(client *MQTT.MqttClient, message MQTT.Message) {
	log.Infof("topic:%s  / msg:%s", message.Topic(), message.Payload())

	// Remove topic root
	ct := strings.TrimRight(m.Config.Topic, "#")
	topic := strings.Replace(message.Topic(), ct, "", 1)

	chun := Message{
		Topic:   topic,
		Payload: message.Payload(),
	}

	m.ifChan <- chun
}

func (m *MqttClient) Start(command chan string, ifchan chan Message) error {
	topic := m.Config.Topic
	if strings.HasSuffix(topic, "#") == false {
		topic = topic + "#"
	}

	log.Infof("mqtt subscribe: %s", topic)

	qos := 0
	topicFilter, err := MQTT.NewTopicFilter(topic, byte(qos))
	if err != nil {
		return err
	}

	m.ifChan = ifchan

	_, err = m.Client.StartSubscription(m.onMessageReceived, topicFilter)
	if err != nil {
		return err
	}
	return nil
}
