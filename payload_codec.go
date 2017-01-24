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
	"compress/gzip"
	"encoding/binary"
	"log"

	"github.com/golang/snappy"
)

// compression flags
const (
	NO_COMPRESSION_ID     = 0
	GZIP_COMPRESSION_ID   = 1
	SNAPPY_COMPRESSION_ID = 2
)

var snappyMagic = []byte{130, 83, 78, 65, 80, 80, 89, 0}

// PayloadCodec defines an interface for the Codecs supported by the Kafka library
type PayloadCodec interface {

	// ID returns the 1-byte id of the codec
	ID() byte

	// Encode is the encoder interface for compression implementation
	Encode(data []byte) []byte

	// Decode is the decoder interface for decompression implementation
	Decode(data []byte) []byte
}

// DefaultCodecs is a list of codecs supported and packaged with the library itself
var DefaultCodecs = []PayloadCodec{
	new(NoCompressionPayloadCodec),
	new(GzipPayloadCodec),
	new(SnappyPayloadCodec),
}

// DefaultCodecsMap is a map[id]Codec representation of the supported codecs
var DefaultCodecsMap = codecsMap(DefaultCodecs)

func codecsMap(payloadCodecs []PayloadCodec) map[byte]PayloadCodec {
	payloadCodecsMap := make(map[byte]PayloadCodec, len(payloadCodecs))
	for _, c := range payloadCodecs {
		payloadCodecsMap[c.ID()] = c
	}
	return payloadCodecsMap
}

// NoCompressionPayloadCodec - No compression codec, noop
type NoCompressionPayloadCodec struct {
}

// ID returns the 1-byte id of the codec
func (codec *NoCompressionPayloadCodec) ID() byte {
	return NO_COMPRESSION_ID
}

// Encode encodes the message without any compression (no-op)
func (codec *NoCompressionPayloadCodec) Encode(data []byte) []byte {
	return data
}

// Decode decodes the message without any compression (no-op)
func (codec *NoCompressionPayloadCodec) Decode(data []byte) []byte {
	return data
}

// GzipPayloadCodec - Gzip Codec
type GzipPayloadCodec struct {
}

// ID returns the 1-byte id of the codec
func (codec *GzipPayloadCodec) ID() byte {
	return GZIP_COMPRESSION_ID
}

// Encode encodes the message with GZip compression
func (codec *GzipPayloadCodec) Encode(data []byte) []byte {
	buf := bytes.NewBuffer([]byte{})
	zipper, _ := gzip.NewWriterLevel(buf, gzip.BestSpeed)
	zipper.Write(data)
	zipper.Close()
	return buf.Bytes()
}

// Decode decodes the message with GZip compression
func (codec *GzipPayloadCodec) Decode(data []byte) []byte {
	buf := bytes.NewBuffer([]byte{})
	zipper, err := gzip.NewReader(bytes.NewBuffer(data))
	if nil != err {
		log.Println("Error creating gzip reader:", err)
	}
	unzipped := make([]byte, 100)
	for {
		n, err := zipper.Read(unzipped)
		if n > 0 {
			buf.Write(unzipped[0:n])
		}
		if nil != err {
			if err.Error() != "EOF" {
				log.Println("Unexpected error reading gzipped data:", err.Error())
			}
			break
		}
	}
	zipper.Close()
	return buf.Bytes()
}

// SnappyPayloadCodec - Snappy Codec
type SnappyPayloadCodec struct {
}

// ID returns the 1-byte id of the codec
func (codec *SnappyPayloadCodec) ID() byte {
	return SNAPPY_COMPRESSION_ID
}

// Encode encodes the message with Snappy compression
func (codec *SnappyPayloadCodec) Encode(data []byte) []byte {
	return snappy.Encode(nil, data)
}

// Decode decodes the message with Snappy compression
func (codec *SnappyPayloadCodec) Decode(data []byte) []byte {
	if bytes.Equal(data[:8], snappyMagic) {
		var pos = uint32(16)
		var max = uint32(len(data))
		var decoded []byte
		for pos < max {
			size := binary.BigEndian.Uint32(data[pos : pos+4])
			pos = pos + 4
			chunk, err := snappy.Decode(nil, data[pos:pos+size])
			if nil != err {
				panic("Could not decode Snappy-encoded message: " + err.Error())
			}
			pos = pos + size
			decoded = append(decoded, chunk...)
		}
		return decoded
	}
	decoded, err := snappy.Decode(nil, data)
	if nil != err {
		panic("Could not decode Snappy-encoded message: " + err.Error())
	}
	return decoded
}
