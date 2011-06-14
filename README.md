= Kafka.go - Publisher & Consumer for Kafka in Go =
---------------------------------------------------------------------
Kafka is a distributed publish-subscribe messaging system: (http://sna-projects.com/kafka/)
Go language: (http://golang.org/)

Get up and running
------------------
Install kafka.go package:
make install

Make the tools (publisher & consumer)
make tools

Start zookeeper, Kafka server
For more info on Kafka, see: http://sna-projects.com/kafka/quickstart.php




== Tools ==

Start a consumer:
<pre><code>
   ./tools/consumer/consumer -topic test -consumeforever
  Consuming Messages :
  From: localhost:9092, topic: test, partition: 0
   ---------------------- 
</code></pre>

Now the consumer will just poll until a message is received.
  
Publish a message:
<pre><code>
  ./tools/publisher/publisher -topic test -message "Hello World"
</code></pre>

The consumer should output message.

== API Usage ==

Publishing
----------

<pre><code>

broker := kafka.NewBrokerPublisher("localhost:9092", "mytesttopic", 0)
broker.Publish(kafka.NewMessage([]byte("tesing 1 2 3")))

</code></pre>




