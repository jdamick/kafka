/*
 *  Copyright (c) 2011 NeuStar, Inc.
 *  All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  NeuStar, the Neustar logo and related names and logos are registered
 *  trademarks, service marks or tradenames of NeuStar, Inc. All other
 *  product names, company names, marks, logos and symbols may be trademarks
 *  of their respective owners.
 */

package kafka

import (
	"net"
)

// BrokerPublisher holds a Kafka broker instance and the publisher settings
type BrokerPublisher struct {
	broker *Broker
	conn   *net.TCPConn
}

// NewBrokerPublisher returns a new broker instance for a publisher
func NewBrokerPublisher(hostname string, topic string, partition int) *BrokerPublisher {
	return &BrokerPublisher{broker: newBroker(hostname, topic, partition)}
}

// Publish writes a message to the kafka broker
func (b *BrokerPublisher) Publish(message *Message) (int, error) {
	return b.BatchPublish(message)
}

// BatchPublish writes a batch of messages to the kafka broker
func (b *BrokerPublisher) BatchPublish(messages ...*Message) (int, error) {
	request := b.broker.EncodePublishRequest(messages...)

	var err error
	// only establish one connection
	if nil == b.conn {
		b.conn, err = b.broker.connect()
		if nil != err {
			return -1, err
		}
	}
	// attempt sending the request reusing the existing connection
	num, err := b.conn.Write(request)
	if err != nil {
		// the connection might have gone away, attempt reconnecting
		b.conn, err = b.broker.connect()
		if nil != err {
			return -1, err
		}
		num, err := b.conn.Write(request)
		if nil != err {
			return -1, err
		}
		return num, err
	}
	return num, err
}

// ProduceFromChannel reads the messages from a Kafka log and sends them to a Message channel
func (b *BrokerPublisher) ProduceFromChannel(msgChan chan *Message, quit chan struct{}) (int, error) {
	conn, err := b.broker.connect()
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	num := 0
	for {
		select {
		case m := <-msgChan:
			request := b.broker.EncodePublishRequest(m)
			_, err := conn.Write(request)
			if err != nil {
				return num, err
			}
			num++
		case <-quit:
			break
		}
	}
	return num, nil
}
