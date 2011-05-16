
package kafka


import (
	"encoding/binary"
)


func uint16bytes(value int) []byte {
	result := make([]byte, 2)
	binary.BigEndian.PutUint16(result, uint16(value))
	return result
}

func uint32bytes(value int) []byte {
	result := make([]byte, 4)
	binary.BigEndian.PutUint32(result, uint32(value))
	return result
}

func uint32toUint32bytes(value uint32) []byte {
	result := make([]byte, 4)
	binary.BigEndian.PutUint32(result, value)
	return result
}

func uint64ToUint64bytes(value uint64) []byte {
	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, uint64(value))
	return result
}
