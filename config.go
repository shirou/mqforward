package main

import (
	"os"
	"runtime"
	"strings"

	log "github.com/Sirupsen/logrus"
	gcfg "gopkg.in/gcfg.v1"
)

type GeneralConf struct {
	Debug bool
}

type Config struct {
	General  GeneralConf
	Mqtt     MqttConf     `gcfg:"mqforward-mqtt"`
	InfluxDB InfluxDBConf `gcfg:"mqforward-influxdb"`
}

func UserHomeDir() string {
	if runtime.GOOS == "windows" {
		home := os.Getenv("HOMEDRIVE") + os.Getenv("HOMEPATH")
		if home == "" {
			home = os.Getenv("USERPROFILE")
		}
		return home
	}
	return os.Getenv("HOME")
}

func LoadConf(path string) (MqttConf, InfluxDBConf, error) {
	home := UserHomeDir()
	path = strings.Replace(path, "~", home, 1)

	var cfg Config
	err := gcfg.ReadFileInto(&cfg, path)
	if err != nil {
		return MqttConf{}, InfluxDBConf{}, err
	}

	if cfg.General.Debug {
		log.SetLevel(log.DebugLevel)
	}

	return cfg.Mqtt, cfg.InfluxDB, nil
}
