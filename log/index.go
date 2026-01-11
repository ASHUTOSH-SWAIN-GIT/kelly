package log

import (
	"encoding/binary"
	"os"
	"path/filepath"
)

const indexEntrySize = 8

type Index struct {
	File       *os.File
	BaseOffset int64
	Size       int64
}

func NewIndex(dir string, baseOffset int64) (*Index, error) {
	filename := filepath.Join(dir, formatOffset(baseOffset)+".index")

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return &Index{
		File:       file,
		BaseOffset: baseOffset,
		Size:       info.Size(),
	}, nil
}

func (i *Index) Write(offset int64, position int64) error {
	buf := make([]byte, indexEntrySize)
	rel := int32(offset - i.BaseOffset)

	binary.BigEndian.PutUint32(buf[0:4], uint32(rel))
	binary.BigEndian.PutUint32(buf[4:8], uint32(position))

	_, err := i.File.Write(buf)
	if err != nil {
		return err
	}

	i.Size += indexEntrySize
	return nil
}

func (i *Index) Read(offset int64) (int64, error) {
	rel := int32(offset - i.BaseOffset)
	entries := i.Size / indexEntrySize

	low := int64(0)
	high := entries - 1

	for low <= high {
		mid := (low + high) / 2
		pos := mid * indexEntrySize

		buf := make([]byte, indexEntrySize)
		_, err := i.File.ReadAt(buf, pos)
		if err != nil {
			return 0, err
		}
		midOffset := int32(binary.BigEndian.Uint32(buf[0:4]))
		midPos := int64(binary.BigEndian.Uint32(buf[4:8]))
		if midOffset == rel {
			return midPos, nil
		}
		if midOffset < rel {
			low = mid + 1
		} else {
			high = mid - 1
		}

	}
	return -1, nil
}
