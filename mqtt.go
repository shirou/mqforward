package main

import (
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
	MQTT "github.com/eclipse/paho.mqtt.golang"
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
	ClientCert string
	PrivateKey string
	Topic      string
	Debug      string
}

type MqttClient struct {
	Client     MQTT.Client
	Opts       *MQTT.ClientOptions
	Config     MqttConf
	Subscribed map[string]byte

	mqttChan    chan Message // chan to forwarder
	commandChan chan string  // chan from forwarder
	lock        *sync.Mutex  // use for reconnect
}

//
// with Connects connect to the MQTT broker with Options.
func NewMqttClient(conf MqttConf, mqttChan chan Message, commandChan chan string) (*MqttClient, error) {
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
	opts.SetClientID(clientId)
	opts.SetAutoReconnect(true)

	topic := conf.Topic
	if strings.HasSuffix(topic, "#") == false {
		topic = topic + "#"
	}
	subscribed := map[string]byte{
		topic: byte(0),
	}

	tlsConfig, ok, err := makeTlsConfig(conf.Cafilepath, conf.ClientCert, conf.PrivateKey, false)
	if err != nil {
		return nil, err
	}
	if ok {
		opts.SetTLSConfig(tlsConfig)
	}

	ret := &MqttClient{
		Config:      conf,
		Subscribed:  subscribed,
		lock:        new(sync.Mutex),
		mqttChan:    mqttChan,
		commandChan: commandChan,
	}
	opts.SetOnConnectHandler(ret.SubscribeOnConnect)
	opts.SetConnectionLostHandler(ret.ConnectionLost)

	ret.Opts = opts

	client, err := ret.Connect(conf, opts, subscribed)
	if err != nil {
		return nil, err
	}
	ret.Client = client

	return ret, nil
}

// connects MQTT broker
func (m MqttClient) Connect(conf MqttConf, opts *MQTT.ClientOptions, subscribed map[string]byte) (MQTT.Client, error) {
	m.Client = MQTT.NewClient(m.Opts)

	log.Info("connecting...")

	if token := m.Client.Connect(); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}
	return m.Client, nil
}

// getRandomClientId returns randomized ClientId.
func getRandomClientId() string {
	const alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, MaxClientIdLen)
	rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphanum[b%byte(len(alphanum))]
	}
	return "mqttforward-" + string(bytes)
}

func (m *MqttClient) SubscribeOnConnect(client MQTT.Client) {
	log.Infof("mqtt connected")
	log.Infof("subscribed: %v", m.Subscribed)

	if len(m.Subscribed) > 0 {
		token := client.SubscribeMultiple(m.Subscribed, m.onMessageReceived)
		token.Wait()
		if token.Error() != nil {
			log.Error(token.Error())
		}
	}
}
func (m *MqttClient) ConnectionLost(client MQTT.Client, reason error) {
	log.Errorf("client disconnected: %s", reason)
}

func (m *MqttClient) Disconnect() error {
	if m.Client.IsConnected() {
		m.Client.Disconnect(20)
		log.Info("client disconnected")
	}
	return nil
}

func (m *MqttClient) onMessageReceived(client MQTT.Client, message MQTT.Message) {
	log.Debugf("topic:%s", message.Topic())

	// Remove topic root
	ct := strings.TrimRight(m.Config.Topic, "#")
	topic := strings.Replace(message.Topic(), ct, "", 1)

	chun := Message{
		Topic:   topic,
		Payload: message.Payload(),
	}

	m.mqttChan <- chun
}

func getCertPool(pemPath string) (*x509.CertPool, error) {
	certs := x509.NewCertPool()

	pemData, err := ioutil.ReadFile(pemPath)
	if err != nil {
		return nil, err
	}
	certs.AppendCertsFromPEM(pemData)
	return certs, nil
}

// makeTlsConfig creates new tls.Config. If returned ok is false, does not need set to MQTToption.
func makeTlsConfig(cafile, cert, key string, insecure bool) (*tls.Config, bool, error) {
	TLSConfig := &tls.Config{InsecureSkipVerify: false}
	var ok bool
	if insecure {
		TLSConfig.InsecureSkipVerify = true
		ok = true
	}
	if cafile != "" {
		certPool, err := getCertPool(cafile)
		if err != nil {
			return nil, false, err
		}
		TLSConfig.RootCAs = certPool
		ok = true
	}
	if cert != "" {
		certPool, err := getCertPool(cert)
		if err != nil {
			return nil, false, err
		}
		TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
		TLSConfig.ClientCAs = certPool
		ok = true
	}
	if key != "" {
		if cert == "" {
			return nil, false, fmt.Errorf("key specified but cert is not specified")
		}
		cert, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, false, err
		}
		TLSConfig.Certificates = []tls.Certificate{cert}
		ok = true
	}
	return TLSConfig, ok, nil
}
