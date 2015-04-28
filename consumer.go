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
	"encoding/binary"
	"io"
	"log"
	"net"
	"time"
)

// BrokerConsumer holds a Kafka broker instance and the consumer settings
type BrokerConsumer struct {
	broker  *Broker
	offset  uint64
	maxSize uint32
	codecs  map[byte]PayloadCodec
}

// NewBrokerConsumer creates a new broker consumer
// * hostname - host and optionally port, delimited by ':'
// * topic to consume
// * partition to consume from
// * offset to start consuming from
// * maxSize (in bytes) of the message to consume (this should be at least as big as the biggest message to be published)
func NewBrokerConsumer(hostname string, topic string, partition int, offset uint64, maxSize uint32) *BrokerConsumer {
	return &BrokerConsumer{broker: newBroker(hostname, topic, partition),
		offset:  offset,
		maxSize: maxSize,
		codecs:  DefaultCodecsMap}
}

// NewBrokerOffsetConsumer creates a simplified consumer that defaults the offset and maxSize to 0.
// * hostname - host and optionally port, delimited by ':'
// * topic to consume
// * partition to consume from
func NewBrokerOffsetConsumer(hostname string, topic string, partition int) *BrokerConsumer {
	return &BrokerConsumer{broker: newBroker(hostname, topic, partition),
		offset:  0,
		maxSize: 0,
		codecs:  DefaultCodecsMap}
}

// AddCodecs is a utility method to add Custom Payload Codecs for Consumer Decoding
// payloadCodecs - an array of PayloadCodec implementations
func (consumer *BrokerConsumer) AddCodecs(payloadCodecs []PayloadCodec) {
	// merge to the default map, so one 'could' override the default codecs..
	for k, v := range codecsMap(payloadCodecs) {
		consumer.codecs[k] = v
	}
}

// ConsumeOnChannel fetches messages from kafka and enqueues them in a channel
func (consumer *BrokerConsumer) ConsumeOnChannel(msgChan chan *Message, pollTimeoutMs int64, quit chan struct{}) (int, error) {
	conn, err := consumer.broker.connect()
	if err != nil {
		return -1, err
	}

	forceQuit := make(chan struct{})

	num := 0
	done := make(chan bool, 1)
	go func() {
	Loop:
		for {
			_, err := consumer.consumeWithConn(conn, func(msg *Message) {
				//msg.Print()
				msgChan <- msg
				num++
			}, quit)

			if err != nil {
				if err != io.EOF && err.Error() != "use of closed network connection" { //
					log.Println("Fatal Error: ", err)
					panic(err)
				}
				close(forceQuit)
				break
			}

			// time.Sleep(time.Millisecond * time.Duration(pollTimeoutMs))
			select {
			case <-quit:
				log.Println("Kafka consumer - received request to stop")
				break Loop
			case <-time.After(time.Millisecond * time.Duration(pollTimeoutMs)):
				// carry on after the polling interval
			}
		}
		done <- true
	}()

	// wait to be told to stop...
	select {
	case <-forceQuit:
		// stopping from within this function
	case <-quit:
		// we were told to stop from outside
	}

	conn.Close()
	<-done
	close(msgChan)
	return num, err
}

// MessageHandlerFunc defines the interface for message handlers accepted by Consume()
type MessageHandlerFunc func(msg *Message)

// Consume makes a single fetch request and sends the messages in the message set to a handler function
func (consumer *BrokerConsumer) Consume(handlerFunc MessageHandlerFunc, stop <-chan struct{}) (int, error) {
	conn, err := consumer.broker.connect()
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	num, err := consumer.consumeWithConn(conn, handlerFunc, stop)

	if err != nil {
		log.Println("Fatal Error: ", err)
	}

	return num, err
}

func (consumer *BrokerConsumer) consumeWithConn(conn *net.TCPConn, handlerFunc MessageHandlerFunc, stop <-chan struct{}) (int, error) {
	_, err := conn.Write(consumer.broker.EncodeConsumeRequest(consumer.offset, consumer.maxSize))
	if err != nil {
		log.Println("Failed kafka fetch request:", err.Error())
		return -1, err
	}

	length, payload, err := consumer.broker.readResponse(conn)
	//log.Println("kafka fetch request of", length, "bytes starting from offset", consumer.offset)

	if err != nil {
		return -1, err
	}

	num := 0
	if length > 2 {
		// parse out the messages
		currentOffset := uint64(0)
		for currentOffset <= uint64(length-4) {
			totalLength, msgs, err1 := Decode(payload[currentOffset:], consumer.codecs)
			if ErrIncompletePacket == err1 {
				// Reached the end of the current packet and the last message is incomplete.
				if 0 == num {
					// This is the very first message in the batch => we need to request a larger packet
					// or the consumer will get stuck here indefinitely
					log.Printf("ERROR: Incomplete message at offset %d %d, change the configuration to a larger max fetch size\n",
						consumer.offset,
						currentOffset)
				} else {
					// Partial message at end of current batch, need a new Fetch Request from a newer offset
					log.Printf("DEBUG: Incomplete message at offset %d %d for topic '%s' (%s, partition %d)\n",
						consumer.offset,
						currentOffset,
						consumer.broker.topic,
						consumer.broker.hostname,
						consumer.broker.partition)
				}
				break
			}
			msgOffset := consumer.offset + currentOffset
			for _, msg := range msgs {
				// do a non-blocking select to see whether we received a request to stop reading
				select {
				case <-stop:
					//fmt.Println("received request to stop whilst iterating message set")
					return num, err
				default:
					// update the offset of whole message set
					// multiple messages can be at the same offset (compressed for example)
					msg.offset = msgOffset
					handlerFunc(&msg)
					num++
				}

			}
			currentOffset += uint64(4 + totalLength)
		}
		// update the broker's offset for next consumption
		consumer.offset += currentOffset
	}

	return num, err
}

// GetOffsets returns a list of valid offsets (up to maxNumOffsets) before the given time, where
// time is in milliseconds (-1, from the latest offset available, -2 from the smallest offset available)
// The result is a list of offsets, in descending order.
func (consumer *BrokerConsumer) GetOffsets(time int64, maxNumOffsets uint32) ([]uint64, error) {
	var offsets []uint64

	conn, err := consumer.broker.connect()
	if err != nil {
		return offsets, err
	}

	defer conn.Close()

	_, err = conn.Write(consumer.broker.EncodeOffsetRequest(time, maxNumOffsets))
	if err != nil {
		return offsets, err
	}

	length, payload, err := consumer.broker.readResponse(conn)
	if err != nil {
		return offsets, err
	}

	if length > 4 {
		// get the number of offsets
		numOffsets := binary.BigEndian.Uint32(payload[0:])
		var currentOffset uint64 = 4
		for currentOffset < uint64(length-4) && uint32(len(offsets)) < numOffsets {
			offset := binary.BigEndian.Uint64(payload[currentOffset:])
			offsets = append(offsets, offset)
			currentOffset += 8 // offset size
		}
	}

	return offsets, err
}

// GetOffset returns the current offset for a broker.
func (consumer *BrokerConsumer) GetOffset() uint64 {
	return consumer.offset
}
