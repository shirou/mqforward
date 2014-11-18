mqforward
=====================

mqforward is forwarder from MQTT to Influxdb.
This subscribes a topic with wildcard and forward a payload to
Influxdb. The value should be JSON or msgpack.

for example,

- Publish ``{"a": 1, "b": 2}`` to "mqforward/a/b", 
- Subscribe "mqforward/#", 

in the influxdb,

- value ``a=1 and b=2`` into the a.b series. so you can ``SELECT a, b FROM a.b``


If udp is true in the config, send series over UDP to InfluxDB. otherwise, use HTTP.
  
usage
---------

install
+++++++++++++++

::

  go get github.com/shirou/mqforward

config
+++++++++++++++

example is below. if you put this to `~/.mqforward.ini`, load
automatically.

::

   [mqforward-mqtt]
   hostname= test.mosquitto.org
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
   udp = true  # true if use UDP or false

run
+++++++++++++++

::

   mqforward run

or 

::

   mqforward run -c someconfig.ini

license
-----------

MIT
