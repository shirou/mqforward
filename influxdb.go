package main

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	influxdb "github.com/influxdata/influxdb/client"
	"github.com/oleiade/lane"
)

const (
	DefaultTick = 1
)

type InfluxDBConf struct {
	Hostname string
	Port     int
	Db       string
	UserName string
	Password string
	Tick     int
	UDP      bool
	Debug    string
}

type InfluxDBClient struct {
	Client *influxdb.Client
	Config InfluxDBConf

	Status string
	Tick   int

	Buffer *lane.Deque

	ifChan      chan Message
	commandChan chan string
}

func NewInfluxDBClient(conf InfluxDBConf, ifChan chan Message, commandChan chan string) (*InfluxDBClient, error) {
	host := fmt.Sprintf("http://%s:%d", conf.Hostname, conf.Port)
	log.Infof("influxdb host: %s", host)

	u, err := url.Parse(host)
	if err != nil {
		return nil, err
	}
	ifConf := influxdb.Config{
		URL:      *u,
		Username: conf.UserName,
		Password: conf.Password,
		//		IsUDP:    conf.UDP,
	}
	con, err := influxdb.NewClient(ifConf)
	if err != nil {
		return nil, err
	}
	// Check connectivity
	_, _, err = con.Ping()
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
	bp := Msg2Series(buf)
	bp.Database = ifc.Config.Db

	var res *influxdb.Response
	if res, err = ifc.Client.Write(bp); err != nil {
		return err
	}
	if res != nil && res.Err != nil {
		return res.Err
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

func Msg2Series(msgs []Message) influxdb.BatchPoints {
	pts := make([]influxdb.Point, 0, len(msgs))
	now := time.Now()

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
		tags := map[string]string{
			"topic": msg.Topic,
		}
		pt := influxdb.Point{
			Measurement: name,
			Tags:        tags,
			Fields:      j,
			Time:        now,
			Precision:   "s", // TODO
		}
		pts = append(pts, pt)
	}
	bp := influxdb.BatchPoints{
		RetentionPolicy: "default",
		Points:          pts,
	}

	return bp
}
