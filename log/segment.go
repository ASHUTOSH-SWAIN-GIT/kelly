package log

import (
	"os"
	"path/filepath"
)

type Segment struct {
	BaseOffset int64
	NextOffset int64
	Index      *Index
	File       *os.File
	Size       int64
}

func NewSegment(dir string, baseOffset int64) (*Segment, error) {
	filename := filepath.Join(dir, formatOffset(baseOffset)+".log")

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	index, err := NewIndex(dir, baseOffset)
	if err != nil {
		return nil, err
	}

	return &Segment{
		BaseOffset: baseOffset,
		NextOffset: baseOffset,
		File:       file,
		Index:      index,
		Size:       info.Size(),
	}, nil
}

func (s *Segment) Append(payload []byte) (int64, error) {
	msg := Message{
		Offset:  s.NextOffset,
		Payload: payload,
	}

	data, err := EncodeMessage(msg)
	if err != nil {
		return 0, err
	}

	position := s.Size

	n, err := s.File.Write(data)
	if err != nil {
		return 0, err
	}

	err = s.Index.Write(s.NextOffset, position)
	if err != nil {
		return 0, err
	}

	s.Size += int64(n)
	offset := s.NextOffset
	s.NextOffset++

	return offset, nil
}

func (s *Segment) Close() error {
	return s.File.Close()
}

func (s *Segment) ReadFrom(Offset int64, maxBytes int64) ([]Message, error) {
	var messages []Message

	position, err := s.Index.Read(Offset)
	if err != nil || position < 0 {
		return messages, nil
	}

	buf := make([]byte, maxBytes)
	n, err := s.File.ReadAt(buf, position)
	if err != nil && n == 0 {
		return messages, nil
	}

	read := 0
	for read < n {
		msg, consumed, err := DecodeMessage(buf[read:])
		if err != nil {
			break
		}

		messages = append(messages, msg)
		read += consumed
	}
	return messages, nil
}
