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
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
)

const (
	// MAGIC_DEFAULT is the default value for the Kafka wire format.
	// Compression Support uses '1' - https://cwiki.apache.org/confluence/display/KAFKA/Compression
	MAGIC_DEFAULT = 1

	// NO_LEN_HEADER_SIZE is the length of the header after the 4 bytes representing the length
	// magic + compression + chksum
	NO_LEN_HEADER_SIZE = 1 + 1 + 4
)

// Error constants
var (
	ErrMalformedPacket       = fmt.Errorf("kafka message malformed, expecting at least 4 bytes")
	ErrIncompletePacket      = fmt.Errorf("kafka message incomplete, expecting larger packet")
	ErrIncompleteInnerPacket = fmt.Errorf("incomplete kafka message within a compressed message")
	ErrInvalidMagic          = fmt.Errorf("incorrect magic value")
	ErrChecksumMismatch      = fmt.Errorf("checksum mismatch on kafka message")
)

// Message wraps the message headers and the payload
type Message struct {
	magic       byte
	compression byte
	checksum    [4]byte
	payload     []byte
	offset      uint64 // only used after decoding
	totalLength uint32 // total length of the raw message (from decoding)

}

// Offset returns the position of the current message in the log
func (m *Message) Offset() uint64 {
	return m.offset
}

// Payload returns the actual payload of the message, as byte array
func (m *Message) Payload() []byte {
	return m.payload
}

// PayloadString returns the actual payload of the message, as string
func (m *Message) PayloadString() string {
	return string(m.payload)
}

// NewMessageWithCodec creates a new Message instance, with the payload encoded with the given codec
func NewMessageWithCodec(payload []byte, codec PayloadCodec) *Message {
	message := &Message{}
	message.magic = byte(MAGIC_DEFAULT)
	message.compression = codec.ID()
	message.payload = codec.Encode(payload)
	binary.BigEndian.PutUint32(message.checksum[0:], crc32.ChecksumIEEE(message.payload))
	return message
}

// NewMessage creates a message with no compression
func NewMessage(payload []byte) *Message {
	return NewMessageWithCodec(payload, DefaultCodecsMap[NO_COMPRESSION_ID])
}

// NewCompressedMessage creates a Message using the default compression method (gzip)
func NewCompressedMessage(payload []byte) *Message {
	return NewCompressedMessages(NewMessage(payload))
}

// NewCompressedMessages encodes a batch of Messages using the default compression method (gzip)
func NewCompressedMessages(messages ...*Message) *Message {
	buf := bytes.NewBuffer([]byte{})
	for _, message := range messages {
		buf.Write(message.Encode())
	}
	return NewMessageWithCodec(buf.Bytes(), DefaultCodecsMap[GZIP_COMPRESSION_ID])
}

// Encode marshals the Message object into kafka's wire format
// MESSAGE SET: <MESSAGE LENGTH: uint32><MAGIC: 1 byte><COMPRESSION: 1 byte><CHECKSUM: uint32><MESSAGE PAYLOAD: bytes>
func (m *Message) Encode() []byte {
	msgLen := NO_LEN_HEADER_SIZE + len(m.payload)
	msg := make([]byte, 4+msgLen)
	binary.BigEndian.PutUint32(msg[0:], uint32(msgLen))
	msg[4] = m.magic
	msg[5] = m.compression

	copy(msg[6:], m.checksum[0:])
	copy(msg[10:], m.payload)

	return msg
}

// DecodeWithDefaultCodecs decodes the message(s) with the default codecs list
func DecodeWithDefaultCodecs(packet []byte) (uint32, []Message, error) {
	return Decode(packet, DefaultCodecsMap)
}

// Decode scans a packet for messages and decodes them
func Decode(packet []byte, payloadCodecsMap map[byte]PayloadCodec) (uint32, []Message, error) {
	messages := []Message{}

	message, err := decodeMessage(packet, payloadCodecsMap)

	// invalid / incomplete message
	if nil != err || nil == message || message.totalLength < 1 {
		log.Println("DEBUG:", err.Error())
		return 0, messages, err
	}

	// one message, no compression
	if message.compression == NO_COMPRESSION_ID {
		return message.totalLength, append(messages, *message), nil
	}

	// wonky special case for compressed messages having embedded messages
	err = nil
	payloadLen := uint32(len(message.payload))
	messageLenLeft := payloadLen
	var innerMsg *Message
	for nil == err && messageLenLeft > 0 {
		start := payloadLen - messageLenLeft
		innerMsg, err = decodeMessage(message.payload[start:], payloadCodecsMap)
		if nil != err {
			log.Println("DEBUG:", err.Error())
			if ErrIncompletePacket == err {
				// the current top-level message is incomplete, reached end of packet
				err = nil
			}
			break
		}
		messageLenLeft = messageLenLeft - innerMsg.totalLength - 4 // message length uint32
		messages = append(messages, *innerMsg)
	}

	return message.totalLength, messages, err
}

func decodeMessage(packet []byte, payloadCodecsMap map[byte]PayloadCodec) (*Message, error) {
	if len(packet) < 5 {
		return nil, ErrMalformedPacket
	}

	length := binary.BigEndian.Uint32(packet[0:])
	if length > uint32(len(packet[4:])) {
		log.Printf("DEBUG: length mismatch, expected at least: %d, was: %d\n", length, len(packet[4:]))
		return nil, ErrIncompletePacket
	}

	msg := Message{}
	msg.totalLength = length
	msg.magic = packet[4]

	rawPayload := []byte{}
	if msg.magic == 0 {
		msg.compression = byte(0)
		copy(msg.checksum[:], packet[5:9])
		payloadLength := length - 1 - 4
		if uint64(len(packet)) < uint64(9+payloadLength) {
			log.Printf("packet incomplete, expected at least %d, got %d ===== %d\n", payloadLength+9, len(packet), length)
			return nil, ErrIncompleteInnerPacket
		}
		rawPayload = packet[9 : 9+payloadLength]
	} else if msg.magic == MAGIC_DEFAULT {
		msg.compression = packet[5]
		copy(msg.checksum[:], packet[6:10])
		payloadLength := length - NO_LEN_HEADER_SIZE
		if uint64(len(packet)) < uint64(10+payloadLength) {
			log.Printf("packet incomplete, expected at least %d, got %d ===== %d\n", payloadLength+10, len(packet), length)
			return nil, ErrIncompleteInnerPacket
		}
		rawPayload = packet[10 : 10+payloadLength]
	} else {
		log.Printf("incorrect magic byte in kafka message header, expected: %X was: %X\n", MAGIC_DEFAULT, msg.magic)
		return nil, ErrInvalidMagic
	}

	payloadChecksum := make([]byte, 4)
	binary.BigEndian.PutUint32(payloadChecksum, crc32.ChecksumIEEE(rawPayload))
	if !bytes.Equal(payloadChecksum, msg.checksum[:]) {
		msg.Print()
		log.Printf("checksum mismatch, expected: % X was: % X\n", payloadChecksum, msg.checksum[:])
		return nil, ErrChecksumMismatch
	}
	msg.payload = payloadCodecsMap[msg.compression].Decode(rawPayload)

	return &msg, nil
}

// Print is a debug method to print the Message object
func (m *Message) Print() {
	log.Println("----- Begin Message ------")
	log.Printf("magic: %X\n", m.magic)
	log.Printf("compression: %X\n", m.compression)
	log.Printf("checksum: %X\n", m.checksum)
	if len(m.payload) < 1048576 { // 1 MB
		log.Printf("payload: % X\n", m.payload)
		log.Printf("payload(string): %s\n", m.PayloadString())
	} else {
		log.Printf("long payload, length: %d\n", len(m.payload))
	}
	log.Printf("length: %d\n", m.totalLength)
	log.Printf("offset: %d\n", m.offset)
	log.Println("----- End Message ------")
}
