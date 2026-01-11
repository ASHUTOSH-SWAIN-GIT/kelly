package log

import (
	"os"
)

type Log struct {
	Dir         string
	SegmentSize int64
	Segments    []*Segment
	Active      *Segment
}

func NewLog(dir string, segmentSize int64) (*Log, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	segment, err := NewSegment(dir, 0)
	if err != nil {
		return nil, err
	}

	return &Log{
		Dir:         dir,
		SegmentSize: segmentSize,
		Segments:    []*Segment{segment},
		Active:      segment,
	}, nil
}

func (l *Log) Append(payload []byte) (int64, error) {
	if l.Active.Size >= l.SegmentSize {
		err := l.rotate()
		if err != nil {
			return 0, err
		}
	}
	return l.Active.Append(payload)
}

func (l *Log) rotate() error {
	baseOffset := l.Active.NextOffset

	segment, err := NewSegment(l.Dir, baseOffset)
	if err != nil {
		return err
	}

	l.Segments = append(l.Segments, segment)
	l.Active = segment

	return nil
}

func (l *Log) findSegment(offset int64) *Segment {
	for i := len(l.Segments) - 1; i >= 0; i-- {
		if l.Segments[i].BaseOffset <= offset {
			return l.Segments[i]
		}
	}
	return nil
}

func (l *Log) Read(offset int64, maxBytes int64) ([]Message, error) {
	var result []Message
	remaining := maxBytes

	seg := l.findSegment(offset)
	if seg == nil {
		return result, nil
	}

	started := false

	for _, s := range l.Segments {
		if s == seg {
			started = true
		}
		if !started {
			continue
		}
		msgs, err := s.ReadFrom(offset, remaining)
		if err != nil {
			return result, err
		}
		for _, m := range msgs {
			result = append(result, m)
			remaining -= int64(len(m.Payload))
			offset = m.Offset + 1

			if remaining <= 0 {
				return result, nil
			}
		}

	}
	return result, nil
}
