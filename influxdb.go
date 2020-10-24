package main

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	_ "github.com/influxdata/influxdb1-client" // this is important because of the bug in go mod
	influxdb "github.com/influxdata/influxdb1-client/v2"
	"github.com/oleiade/lane"
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
	UserName       string
	Password       string
	Tick           int
	UDP            bool
	Debug          string
	TagsAttributes []string
}

type InfluxDBClient struct {
	Client influxdb.Client
	Config InfluxDBConf

	Status string
	Tick   int

	Buffer *lane.Deque

	ifChan      chan Message
	commandChan chan string
}

func NewInfluxDBClient(conf InfluxDBConf, ifChan chan Message, commandChan chan string) (*InfluxDBClient, error) {
	host := conf.Url
	if len(host) == 0 {
		host = fmt.Sprintf("http://%s:%d", conf.Hostname, conf.Port)
	}
	log.Infof("influxdb host: %s", host)

	_, err := url.Parse(host)
	if err != nil {
		return nil, err
	}
	// Make client
	con, err := influxdb.NewHTTPClient(influxdb.HTTPConfig{
		Addr:     host,
		Username: conf.UserName,
		Password: conf.Password,
	})
	if err != nil {
		return nil, err
	}
	// Check connectivity
	_, _, err = con.Ping(PingTimeout)
	if err != nil {
		return nil, err
	}

	log.Infof("influxdb connected.")

	tick := conf.Tick
	if tick == 0 {
		tick = DefaultTick
	}

	ifc := InfluxDBClient{
		Client: con,
		Tick:   tick,
		Status: StatusStopped,
		Config: conf,
		// prepare 2times by MaxBufferSize for Buffer itself
		Buffer:      lane.NewCappedDeque(MaxBufferSize * 2),
		ifChan:      ifChan,
		commandChan: commandChan,
	}

	return &ifc, nil
}

func (ifc *InfluxDBClient) Send() error {
	if ifc.Buffer.Size() == 0 {
		return nil
	}
	log.Debugf("send to influxdb: size=%d", ifc.Buffer.Size())
	var err error
	buf := make([]Message, MaxBufferSize)

	for i := 0; i < MaxBufferSize; i++ {
		msg := ifc.Buffer.Shift()
		if msg == nil {
			break
		}

		m, ok := msg.(Message)
		if !ok {
			log.Warn("could not cast to message")
			break
		}
		if m.Topic == "" && len(m.Payload) == 0 {
			break
		}
		buf[i] = m
	}
	bp := ifc.Msg2Series(buf)

	if err = ifc.Client.Write(bp); err != nil {
		return err
	}
	return nil
}

// Stop stops sending data, after all data sent.
func (ifc *InfluxDBClient) Stop() {
	ifc.Status = StatusStopped
}

// Start start sending
func (ifc *InfluxDBClient) Start() error {
	ifc.Status = StatusStarted
	duration := time.Duration(ifc.Tick)
	ticker := time.NewTicker(duration * time.Second)

	for {
		select {
		case <-ticker.C:
			if ifc.Status == StatusStopped {
				log.Info("stopped by Status")
				return fmt.Errorf("stopped by Status")
			}
			err := ifc.Send()
			if err != nil {
				log.Errorf("influxdb write err: %s", err)
			}
		case msg := <-ifc.ifChan:
			log.Debugf("add: %s", msg.Topic)
			ifc.Buffer.Append(msg)
		case msg := <-ifc.commandChan:
			switch msg {
			case "stop":
				ticker.Stop()
				ifc.Status = StatusStopped
			}
		}
	}
	return nil
}

func (ifc *InfluxDBClient) Msg2Series(msgs []Message) influxdb.BatchPoints {
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
