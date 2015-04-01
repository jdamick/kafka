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

	"code.google.com/p/snappy-go/snappy"
)

const (
	NO_COMPRESSION_ID     = 0
	GZIP_COMPRESSION_ID   = 1
	SNAPPY_COMPRESSION_ID = 2
)

var snappyMagic = []byte{130, 83, 78, 65, 80, 80, 89, 0}

type PayloadCodec interface {

	// the 1 byte id of the codec
	Id() byte

	// encoder interface for compression implementation
	Encode(data []byte) []byte

	// decoder interface for decompression implementation
	Decode(data []byte) []byte
}

// Default Codecs

var DefaultCodecs = []PayloadCodec{
	new(NoCompressionPayloadCodec),
	new(GzipPayloadCodec),
	new(SnappyPayloadCodec),
}

var DefaultCodecsMap = codecsMap(DefaultCodecs)

func codecsMap(payloadCodecs []PayloadCodec) map[byte]PayloadCodec {
	payloadCodecsMap := make(map[byte]PayloadCodec, len(payloadCodecs))
	for _, c := range payloadCodecs {
		payloadCodecsMap[c.Id()] = c
	}
	return payloadCodecsMap
}

// No compression codec, noop

type NoCompressionPayloadCodec struct {
}

func (codec *NoCompressionPayloadCodec) Id() byte {
	return NO_COMPRESSION_ID
}

func (codec *NoCompressionPayloadCodec) Encode(data []byte) []byte {
	return data
}

func (codec *NoCompressionPayloadCodec) Decode(data []byte) []byte {
	return data
}

// Gzip Codec

type GzipPayloadCodec struct {
}

func (codec *GzipPayloadCodec) Id() byte {
	return GZIP_COMPRESSION_ID
}

func (codec *GzipPayloadCodec) Encode(data []byte) []byte {
	buf := bytes.NewBuffer([]byte{})
	zipper, _ := gzip.NewWriterLevel(buf, gzip.BestSpeed)
	zipper.Write(data)
	zipper.Close()
	return buf.Bytes()
}

func (codec *GzipPayloadCodec) Decode(data []byte) []byte {
	buf := bytes.NewBuffer([]byte{})
	zipper, _ := gzip.NewReader(bytes.NewBuffer(data))
	unzipped := make([]byte, 100)
	for {
		n, err := zipper.Read(unzipped)
		if n > 0 && err == nil {
			buf.Write(unzipped[0:n])
		} else {
			break
		}
	}

	zipper.Close()
	return buf.Bytes()
}

// Snappy Codec

type SnappyPayloadCodec struct {
}

func (codec *SnappyPayloadCodec) Id() byte {
	return SNAPPY_COMPRESSION_ID
}

func (codec *SnappyPayloadCodec) Encode(data []byte) []byte {
	encoded, err := snappy.Encode(nil, data)
	if nil != err {
		panic("Could not encode message: " + err.Error())
	}
	return encoded
}

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
