/*
 * Copyright 2000-2011 NeuStar, Inc. All rights reserved.
 * NeuStar, the Neustar logo and related names and logos are registered
 * trademarks, service marks or tradenames of NeuStar, Inc. All other 
 * product names, company names, marks, logos and symbols may be trademarks
 * of their respective owners.  
 */

package kafka

import (
	"hash/crc32"
	"encoding/binary"
	"bytes"
	"fmt"
)


// Request Types
type Request uint16

const (
	REQUEST_PRODUCE      Request = 0
	REQUEST_FETCH        = 1
	REQUEST_MULTIFETCH   = 2
	REQUEST_MULTIPRODUCE = 3
	REQUEST_OFFSETS      = 4
)

type Message struct {
	magic       byte
	checksum    [4]byte
	payload     []byte
	offset      uint64 // only used after decoding
	totalLength uint32 // total length of the message (decoding)
}

func (m *Message) Offset() uint64 {
	return m.offset
}

func (m *Message) Payload() []byte {
	return m.payload
}

func (m *Message) PayloadString() string {
	return string(m.payload)
}

func NewMessage(payload []byte) *Message {
	message := &Message{}
	message.magic = byte(MAGIC_DEFAULT)
	binary.BigEndian.PutUint32(message.checksum[0:], crc32.ChecksumIEEE(payload))
	message.payload = payload
	return message
}

// MESSAGE SET: <MESSAGE LENGTH: uint32><MAGIC: 1 byte><CHECKSUM: uint32><MESSAGE PAYLOAD: bytes>
func (m *Message) Encode() []byte {
	msgLen := 1 + 4 + len(m.payload)
	msg := make([]byte, 4+msgLen)
	binary.BigEndian.PutUint32(msg[0:], uint32(msgLen))
	msg[4] = m.magic
	copy(msg[5:], m.checksum[0:])
	copy(msg[9:], m.payload)
	return msg
}

func Decode(packet []byte) *Message {
	length := binary.BigEndian.Uint32(packet[0:])
	if length > uint32(len(packet[4:])) {
		fmt.Printf("length mismatch, expected at least: %X, was: %X\n", length, len(packet[4:]))
		return nil
	}
	msg := Message{}
	msg.totalLength = length
	msg.magic = packet[4]
	copy(msg.checksum[:], packet[5:9])
	payloadLength := length - 1 - 4
	msg.payload = packet[9 : 9+payloadLength]

	payloadChecksum := make([]byte, 4)
	binary.BigEndian.PutUint32(payloadChecksum, crc32.ChecksumIEEE(msg.payload))
	if !bytes.Equal(payloadChecksum, msg.checksum[:]) {
		fmt.Printf("checksum mismatch, expected: %X was: %X\n", payloadChecksum, msg.checksum[:])
		return nil
	}
	return &msg
}

func (msg *Message) Print() {
	fmt.Println("----- Begin Message ------")
	fmt.Printf("magic: %X\n", msg.magic)
	fmt.Printf("checksum: %X\n", msg.checksum)
	if len(msg.payload) < 1048576 { // 1 MB 
		fmt.Printf("payload: %X\n", msg.payload)
		fmt.Printf("payload(string): %s\n", msg.PayloadString())
	} else {
		fmt.Printf("long payload, length: %d\n", len(msg.payload))
	}
	fmt.Printf("offset: %d\n", msg.offset)
	fmt.Println("----- End Message ------")
}
