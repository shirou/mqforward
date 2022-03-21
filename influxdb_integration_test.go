//go:build integration
// +build integration

package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"strconv"
	"testing"
	"time"

	influxdb "github.com/influxdata/influxdb/client"
	"github.com/stretchr/testify/assert"
)

func Test_Write(t *testing.T) {
	assert := assert.New(t)

	host, err := url.Parse(fmt.Sprintf("http://%s:%d", "localhost", 8086))
	assert.Nil(err)
	ifConf := influxdb.Config{
		URL:      *host,
		Username: "test",
		Password: "password",
	}
	con, err := influxdb.NewClient(ifConf)
	assert.Nil(err)
	// Check connectivity
	_, _, err = con.Ping()
	assert.Nil(err)

	var (
		shapes     = []string{"circle", "rectangle", "square", "triangle"}
		colors     = []string{"red", "blue", "green"}
		sampleSize = 1000
		pts        = make([]influxdb.Point, sampleSize)
	)

	rand.Seed(42)
	for i := 0; i < sampleSize; i++ {
		pts[i] = influxdb.Point{
			Measurement: "shapes",
			Tags: map[string]string{
				"color": strconv.Itoa(rand.Intn(len(colors))),
				"shape": strconv.Itoa(rand.Intn(len(shapes))),
			},
			Fields: map[string]interface{}{
				"value": rand.Intn(sampleSize),
			},
			Time:      time.Now(),
			Precision: "s",
			Raw:       "cpu,host=server01,region=uswest value=1 1434055562000000000",
		}
	}

	bps := influxdb.BatchPoints{
		Points:          pts,
		Database:        "test",
		RetentionPolicy: "default",
	}
	_, err = con.Write(bps)
	if err != nil {
		log.Fatal(err)
	}
}
