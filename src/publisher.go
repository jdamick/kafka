/*
 * Copyright 2000-2011 NeuStar, Inc. All rights reserved.
 * NeuStar, the Neustar logo and related names and logos are registered
 * trademarks, service marks or tradenames of NeuStar, Inc. All other 
 * product names, company names, marks, logos and symbols may be trademarks
 * of their respective owners.  
 */

package kafka

import (
  "fmt"
  "encoding/binary"
  "container/list"
  "bytes"
  "os"
)


type BrokerPublisher struct {
  broker *Broker
}

func NewBrokerPublisher(hostname string, topic string, partition int) *BrokerPublisher {
  return &BrokerPublisher{broker: newBroker(hostname, topic, partition)}
}


func (b *Broker) Publish(message *Message) (int, os.Error) {
  messages := list.New()
  messages.PushBack(message)
  return b.BatchPublish(messages)
}

func (b *Broker) BatchPublish(messages *list.List) (num int, error os.Error) {
  conn, err := b.connect()
  if err != nil {
    return -1, err
  }

  // TODO: MULTIPRODUCE
  num, err = conn.Write(b.EncodePublishRequest(REQUEST_PRODUCE, messages))
  if err != nil {
    fmt.Println("Fatal Error: ", err)
    return -1, err
  }
  conn.Close()
  return num, error
}


// <REQUEST_SIZE: uint32><REQUEST_TYPE: uint16><TOPIC SIZE: uint16><TOPIC: bytes><PARTITION: uint32><MESSAGE SET SIZE: uint32><MESSAGE SETS>
func (b *Broker) EncodePublishRequest(requestType Request, messages *list.List) []byte {
  // 4 + 2 + 2 + topicLength + 4 + 4
  request := bytes.NewBuffer([]byte{})

  request.Write(uint32bytes(0)) // placeholder for request size
  request.Write(uint16bytes(int(requestType)))
  request.Write(uint16bytes(len(b.topic)))
  request.WriteString(b.topic)
  request.Write(uint32bytes(b.partition))

  messageSetSizePos := request.Len()
  request.Write(uint32bytes(0)) // placeholder message len

  written := 0
  for element := messages.Front(); element != nil; element = element.Next() {
    message := element.Value.(*Message)
    wrote, _ := request.Write(message.Encode())
    written += wrote
  }

  // now add the accumulated size of that the message set was
  binary.BigEndian.PutUint32(request.Bytes()[messageSetSizePos:], uint32(written))
  // now add the size of the whole to the first uint32
  binary.BigEndian.PutUint32(request.Bytes()[0:], uint32(request.Len()-4))

  return request.Bytes()
}
