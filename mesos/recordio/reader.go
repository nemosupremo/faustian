package recordio

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// ErrFrameTooLarge is an error that is returned if a frame that is larger than
// the maximum allowed size (not including the frame header) is read.
var ErrFrameTooLarge = fmt.Errorf("frame: frame size exceeds maximum")

// Reader reads individual frames from a frame-formatted input Reader.
type Reader interface {
	// ReadFrame reads the next frame, returning the frame's size and an io.Reader
	// for that frame's data. The io.Reader is restricted such that it cannot read
	// past the frame.
	//
	// The frame must be fully read before another Reader call can be made.
	// Failure to do so will cause the Reader to become unsynchronized.
	ReadFrame() (int64, io.Reader, error)

	// ReadFrame returns the contents of the next frame. If there are no more
	// frames available, ReadFrame will return io.EOF.
	ReadFrameAll() ([]byte, error)
}

// reader is an implementation of a Reader that uses an underlying
// io.Reader and io.ByteReader to read frames.
//
// The io.Reader and io.ByteReader must read from the same source.
type reader struct {
	*bufio.Reader

	maxSize int
}

// NewReader creates a new Reader which reads frame data from the
// supplied Reader instance.
//
// If the Reader instance is also an io.ByteReader, its ReadByte method will
// be used directly.
func NewReader(r io.Reader, maxSize int) Reader {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}
	return &reader{
		Reader:  br,
		maxSize: maxSize,
	}
}

func (r *reader) ReadFrame() (int64, io.Reader, error) {
	// Read the frame size.
	line, err := r.ReadBytes('\n')
	if err != nil {
		return 0, nil, err
	}

	count, err := strconv.Atoi(string(line[:len(line)-1]))
	if err != nil {
		return 0, nil, err
	}

	if count > r.maxSize {
		return 0, nil, ErrFrameTooLarge
	}

	lr := &io.LimitedReader{
		R: r,
		N: int64(count),
	}
	return int64(count), lr, nil
}

func (r *reader) ReadFrameAll() ([]byte, error) {
	count, fr, err := r.ReadFrame()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}

	data := make([]byte, count)
	if _, err := fr.Read(data); err != nil {
		return nil, err
	}
	return data, nil
}
