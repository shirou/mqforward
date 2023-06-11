package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net/url"
	"time"

	log "github.com/Sirupsen/logrus"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

const (
	DefaultTick = 1
	PingTimeout = 500 * time.Millisecond
)

type InfluxDBConf struct {
	Hostname       string
	Port           int
	Url            string
	Db             string
	Token          string
	Tick           int
	UDP            bool
	Debug          string
	TagsAttributes []string
	TopicMap       []string // maps the end of the mqtt topic to tags `weather/{loc}/{sensor}`
	NoTopicTag     bool     // does not forward the topic as tag
	Series         string   // override the series name instead of topic mapping
	CaCerts        []string
	Scheme         string
	Insecure       bool // skips certificate validation
	Bucket         string
	Org            string
}

type InfluxDBClient struct {
	Client influxdb2.Client
	Config InfluxDBConf
	Coder  *MqttSeriesEncoder

	ifChan chan Message

	write api.WriteAPI
}

func LoadCertPool(conf InfluxDBConf) *x509.CertPool {
	certPool, err := x509.SystemCertPool()
	if err != nil {
		log.Errorf("Error while loading system cert pool")
		log.Error(err)
	}
	for _, path := range conf.CaCerts {
		path = ExpandPath(path)
		log.Debugf("Loading certificate %s", path)
		raw, err := ioutil.ReadFile(path)
		if err != nil {
			log.Errorf("Error while loading certificate %s", path)
			log.Fatal(err)
		}
		certPool.AppendCertsFromPEM(raw)
	}

	return certPool
}

func NewInfluxDBClient(conf InfluxDBConf, ifChan chan Message) (*InfluxDBClient, error) {
	host := conf.Url
	if len(host) == 0 {
		scheme := conf.Scheme
		if scheme == "" {
			scheme = "http"
		}
		host = fmt.Sprintf("%s://%s:%d", conf.Scheme, conf.Hostname, conf.Port)
	}
	log.Infof("influxdb host: %s", host)

	_, err := url.Parse(host)
	if err != nil {
		return nil, err
	}

	certPool := LoadCertPool(conf)

	// Make client
	client := influxdb2.NewClientWithOptions(host, conf.Token,
		influxdb2.DefaultOptions().
			SetTLSConfig(&tls.Config{
				RootCAs:            certPool,
				InsecureSkipVerify: conf.Insecure,
			}))

	// Check connectivity
	ctx, cancel := context.WithTimeout(context.Background(), PingTimeout)
	defer cancel()
	_, err = client.Ping(ctx)
	if err != nil {
		return nil, err
	}

	log.Infof("influxdb connected.")

	tick := conf.Tick
	if tick == 0 {
		tick = DefaultTick
	}

	ifc := InfluxDBClient{
		Client: client,
		Coder:  NewMqttSeriesEncoder(&conf),
		Config: conf,
		ifChan: ifChan,
		write:  client.WriteAPI(conf.Org, conf.Bucket),
	}

	return &ifc, nil
}

// Start start sending
func (ifc *InfluxDBClient) Start() error {
	for {
		msg := <-ifc.ifChan
		point := ifc.Coder.Encode(msg)
		if point == nil {
			continue
		}
		ifc.write.WritePoint(point)
	}
}
