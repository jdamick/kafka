[![Build Status](https://travis-ci.org/jdamick/kafka.png?branch=master)](https://travis-ci.org/jdamick/kafka)

# kafka - Publisher & Consumer for Kafka 0.7.x in Go #

[![Join the chat at https://gitter.im/jdamick/kafka](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/jdamick/kafka?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Kafka is a distributed publish-subscribe messaging system: (http://kafka.apache.org)

Go language: (http://golang.org/) <br/>

##### For Kafka 0.8.x take a look at https://github.com/Shopify/sarama

## Changes

### May 2015

* fixed bug handling partial message at end of a fetch response when the payload is < 4 bytes
* if the the kafka log segment being read is cleaned up, attempt resuming the consumer from the earliest available offset

### April 2015

* added support for Snappy compression
* fixed handling of partial messages at the end of each fetch response
* added ProduceFromChannel() method in the publisher, mirroring the ConsumeOnChannel() method in the consumer
* changed the quit channel type to empty struct{}, adding ability to stop the consumer on demand without race conditions
* reused connection in BatchPublish(), instead of establishing a brand new connection every time.
* applied gofmt / golint on the code (renamed Id() to ID() for compliance)
* added comments
* better distinction between DEBUG and ERROR logs, with info on how to get the consumer unstuck when the max fetch size is too small

### April 2013

* Merged back from the apache repository & outstanding patches from jira applied


## Get up and running ##

Install go (version 1): <br/>
For more info see: http://weekly.golang.org/doc/install.html#install 

Make sure to set your GOROOT properly (http://golang.org/doc/install.html#environment).
Also set your GOPATH appropriately: http://weekly.golang.org/doc/code.html#tmp_13


Build from source:

<code>make kafka</code>
<br/>
Make the tools (publisher & consumer) <br/>
<code>make tools</code>
<br/>
Start zookeeper, Kafka server <br/>
For more info on Kafka, see: http://incubator.apache.org/kafka/quickstart.html



## Tools ##

Start a consumer:
<pre><code>
   $GOPATH/bin/consumer -topic test -consumeforever
  Consuming Messages :
  From: localhost:9092, topic: test, partition: 0
   ---------------------- 
</code></pre>

Now the consumer will just poll until a message is received.
  
Publish a message:
<pre><code>
  $GOPATH/bin/publisher -topic test -message "Hello World"
</code></pre>

The consumer should output message.

## API Usage ##

### Publishing ###


<pre><code>

broker := kafka.NewBrokerPublisher("localhost:9092", "mytesttopic", 0)
broker.Publish(kafka.NewMessage([]byte("testing 1 2 3")))

</code></pre>


### Publishing Compressed Messages ###

<pre><code>

broker := kafka.NewBrokerPublisher("localhost:9092", "mytesttopic", 0)
broker.Publish(kafka.NewCompressedMessage([]byte("testing 1 2 3")))

</code></pre>


### Consumer ###

<pre><code>
broker := kafka.NewBrokerConsumer("localhost:9092", "mytesttopic", 0, 0, 1048576)
broker.Consume(func(msg *kafka.Message) { msg.Print() })

</code></pre>

Or the consumer can use a channel based approach:

<pre><code>
broker := kafka.NewBrokerConsumer("localhost:9092", "mytesttopic", 0, 0, 1048576)
go broker.ConsumeOnChannel(msgChan, 10, quitChan)

</code></pre>

### Consuming Offsets ###

<pre><code>
broker := kafka.NewBrokerOffsetConsumer("localhost:9092", "mytesttopic", 0)
offsets, err := broker.GetOffsets(-1, 1)
</code></pre>


### Contact ###

jeffreydamick (at) gmail (dot) com

http://twitter.com/jeffreydamick

Big thank you to [NeuStar](http://neustar.biz) for sponsoring this work.


