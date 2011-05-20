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
  "bytes"
  "os"
  "bufio"
  "io"
  "strconv"
  "net"
  "time"
)

type BrokerConsumer struct {
  broker  *Broker
  offset  uint64
  maxSize uint32
}

func NewBrokerConsumer(hostname string, topic string, partition int, offset uint64, maxSize uint32) *BrokerConsumer {
  return &BrokerConsumer{broker: newBroker(hostname, topic, partition),
    offset:  offset,
    maxSize: maxSize}
}


func (consumer *BrokerConsumer) ConsumeOnChannel(msgChan chan *Message, pollTimeoutMs int64, quit chan bool) (int, os.Error) {
  conn, err := consumer.broker.connect()
  if err != nil {
    return -1, err
  }

  num := 0
  done := make(chan bool, 1)
  go func() {
    for {
      _, err := consumer.consumeWithConn(conn, func(msg *Message) {
        msgChan <- msg
        num += 1
      })

      if err != nil {
        if err != os.EOF {
          fmt.Println("Fatal Error: ", err)
        }
        break
      }
      time.Sleep(pollTimeoutMs * 1000000)
    }
    done <- true
  }()

  // wait to be told to stop..
  <-quit
  conn.Close()
  close(msgChan)
  <-done
  return num, err
}

type MessageHandlerFunc func(msg *Message)

func (consumer *BrokerConsumer) Consume(handlerFunc MessageHandlerFunc) (int, os.Error) {
  conn, err := consumer.broker.connect()
  if err != nil {
    return -1, err
  }

  num, err := consumer.consumeWithConn(conn, handlerFunc)

  if err != nil {
    fmt.Println("Fatal Error: ", err)
  }

  conn.Close()
  return num, err
}


func (consumer *BrokerConsumer) consumeWithConn(conn *net.TCPConn, handlerFunc MessageHandlerFunc) (int, os.Error) {
  _, err := conn.Write(consumer.broker.EncodeConsumeRequest(REQUEST_FETCH, consumer.offset, consumer.maxSize))
  if err != nil {
    return -1, err
  }

  reader := bufio.NewReader(conn)
  length := make([]byte, 4)
  len, err := io.ReadFull(reader, length)
  if err != nil || len != 4 {
    return -1, err
  }

  expectedLength := binary.BigEndian.Uint32(length)
  messages := make([]byte, expectedLength)
  len, err = io.ReadFull(reader, messages)
  if err != nil {
    return -1, err
  }

  if len != int(expectedLength) {
    err = os.NewError(fmt.Sprintf("Fatal Error: Unexpected Length: %d  expected:  %d", len, expectedLength))
    return -1, err
  }

  errorCode := binary.BigEndian.Uint16(messages[0:2])
  if errorCode != 0 {
    return -1, os.NewError(strconv.Uitoa(uint(errorCode)))
  }

  num := 0
  if len > 2 {
    // parse out the messages
    var currentOffset uint64 = 0
    for currentOffset <= uint64(expectedLength-4) {
      msg := Decode(messages[currentOffset+2:])
      if msg == nil {
        return num, os.NewError("Error Decoding Message")
      }
      msg.offset = consumer.offset + currentOffset
      currentOffset += uint64(4 + msg.totalLength)
      handlerFunc(msg)
      num += 1
    }
    // update the broker's offset for next consumption
    consumer.offset += currentOffset
  }

  return num, err
}


// <REQUEST_SIZE: uint32><REQUEST_TYPE: uint16><TOPIC SIZE: uint16><TOPIC: bytes><PARTITION: uint32><OFFSET: uint64><MAX SIZE: uint32>
func (b *Broker) EncodeConsumeRequest(requestType Request, offset uint64, maxSize uint32) []byte {
  request := bytes.NewBuffer([]byte{})

  request.Write(uint32bytes(0)) // placeholder for request size
  request.Write(uint16bytes(int(requestType)))
  request.Write(uint16bytes(len(b.topic)))
  request.WriteString(b.topic)
  request.Write(uint32bytes(b.partition))

  request.Write(uint64ToUint64bytes(offset))
  request.Write(uint32toUint32bytes(maxSize))
  binary.BigEndian.PutUint32(request.Bytes()[0:], uint32(request.Len()-4))

  return request.Bytes()
}
