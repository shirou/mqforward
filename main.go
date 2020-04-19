package main

import (
	"os"

	log "github.com/Sirupsen/logrus"
	colorable "github.com/mattn/go-colorable"
	cli "github.com/urfave/cli/v2"
)

var usage = `
mqforward
`

func init() {
	log.SetLevel(log.InfoLevel)
	log.SetOutput(colorable.NewColorableStdout())
}

func runForward(c *cli.Context) error {
	if c.Bool("d") {
		log.SetLevel(log.DebugLevel)
	}

	path := c.String("c")

	mqconf, inconf, err := LoadConf(path)
	if err != nil {
		log.Fatal(err)
	}

	f, err := NewForwarder(mqconf, inconf)
	if err != nil {
		log.Fatal(err)
	}
	f.Start()
	return nil
}

func main() {
	app := cli.NewApp()
	app.Name = "mqforward"
	app.Usage = usage
	app.Commands = []*cli.Command{
		{
			Name: "run",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  "c",
					Usage: "Config file path",
					Value: "~/.mqforward.ini",
				},
				&cli.BoolFlag{
					Name:  "d",
					Usage: "enable debug messages.",
				},
			},
			Action: runForward,
		},
	}

	app.Run(os.Args)
}
