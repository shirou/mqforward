mqforward
=====================

mqforward is forwarder from MQTT to Influxdb.
This subscribes a topic with wildcard and forward a payload to
Influxdb. The value should be JSON or msgpack.

for example,

- Publish ``{"a": 1, "b": 2}`` to "mqforward/a/b", 
- Subscribe "mqforward/#", 

in the influxdb,

- value ``a=1 and b=2`` into the a.b series. so you can ``SELECT a, b FROM "a.b"``


If udp is true in the config, send series over UDP to InfluxDB. otherwise, use HTTP.
  
usage
---------

install
+++++++++++++++

Just `go get` (it takes for a while)

::

  $ go get github.com/shirou/mqforward

then, type

::

  $ go install github.com/shirou/mqforward


config
+++++++++++++++

Config example is below. If you put config to `~/.mqforward.ini`, it will be loaded automatically.

::

   [mqforward-mqtt]
   hostname= localhost
   port = 1883
   username= ""
   password= ""
   topic = mqforward/#

   [mqforward-influxdb]
   hostname = 127.0.0.1
   port = 4444
   db = test
   username = root
   password = root

run
+++++++++++++++

Set path to $GOPATH/bin,

::

   $ mqforward run

or 

::

   mqforward run -c someconfig.ini

license
-----------

MIT
