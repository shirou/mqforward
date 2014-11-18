package main

import (
	"fmt"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	influxdb "github.com/influxdb/influxdb/client"
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
}

func NewInfluxDBClient(conf InfluxDBConf) (*InfluxDBClient, error) {
	host := fmt.Sprintf("%s:%d", conf.Hostname, conf.Port)
	log.Infof("influxdb host: %s", host)

	c, err := influxdb.NewClient(&influxdb.ClientConfig{
		Host:     host,
		Username: conf.UserName,
		Password: conf.Password,
		Database: conf.Db,
		IsUDP:    conf.UDP,
	})
	if err != nil {
		return nil, err
	}

	tick := conf.Tick
	if tick == 0 {
		tick = DefaultTick
	}

	ifc := InfluxDBClient{
		Client: c,
		Tick:   tick,
		Status: StatusStopped,
		Config: conf,
		// prepare 2times by MaxBufferSize for Buffer itself
		Buffer: lane.NewCappedDeque(MaxBufferSize * 2),
	}

	return &ifc, nil
}

func (ifc *InfluxDBClient) Send() error {
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
	series := Msg2Series(buf)

	if ifc.Config.UDP {
		if err := ifc.Client.WriteSeriesOverUDP(series); err != nil {
			log.Warnf("write error: %s", err)
		}
	} else {
		if err := ifc.Client.WriteSeries(series); err != nil {
			log.Warnf("write error: %s", err)
		}
	}

	return nil
}

// Stop stops sending data, after all data sent.
func (ifc *InfluxDBClient) Stop() {
	ifc.Status = StatusStopped
}

// Start start sending
func (ifc *InfluxDBClient) Start(command chan string, msgChan chan Message) error {
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
				return err
			}
		case msg := <-msgChan:
			log.Infof("add to %s", msg.Topic)
			ifc.Buffer.Append(msg)
		case msg := <-command:
			switch msg {
			case "stop":
				ticker.Stop()
				ifc.Status = StatusStopped
			}
		}
	}
	return nil
}

func Msg2Series(msgs []Message) []*influxdb.Series {
	ret := make(map[string]*influxdb.Series)

	for _, msg := range msgs {
		if msg.Topic == "" && len(msg.Payload) == 0 {
			break
		}
		name := strings.Replace(msg.Topic, "/", ".", -1)
		s, ok := ret[name]

		columns, values, err := MsgParse(msg.Payload)
		if err != nil {
			log.Warn(err)
			continue
		}
		if !ok {
			ret[name] = &influxdb.Series{
				Name:    name,
				Columns: columns, // assume same name has same columns
				Points:  make([][]interface{}, 0, len(msgs)),
			}
			s = ret[name]
		}
		s.Points = append(s.Points, values)
	}

	series := make([]*influxdb.Series, 0, len(ret))
	for _, value := range ret {
		log.Infof("name:%s, columns:%s", value.Name, value.Columns)
		series = append(series, value)
	}

	return series
}
