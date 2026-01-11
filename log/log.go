package log

import "os"

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
