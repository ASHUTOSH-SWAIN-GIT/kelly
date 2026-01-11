package log

import (
	"encoding/binary"
	"hash/crc32"
)

type Message struct {
	Offset  int64
	Payload []byte
}

func EncodeMessage(msg Message) ([]byte, error) {
	size := int32(len(msg.Payload))
	buf := make([]byte, 8+4+4+len(msg.Payload))

	binary.BigEndian.PutUint64(buf[0:8], uint64(msg.Offset))
	binary.BigEndian.PutUint32(buf[8:12], uint32(size))

	crc := crc32.ChecksumIEEE(msg.Payload)
	binary.BigEndian.PutUint32(buf[12:16], crc)

	copy(buf[16:], msg.Payload)
	return buf, nil
}

func DecodeMessage(data []byte) (Message, int, error) {
	offset := int64(binary.BigEndian.Uint64(data[0:8]))
	size := int(binary.BigEndian.Uint32(data[8:12]))

	start := 16
	end := start + size

	payload := make([]byte, size)
	copy(payload, data[start:end])

	msg := Message{
		Offset:  offset,
		Payload: payload,
	}

	return msg, end, nil
}
