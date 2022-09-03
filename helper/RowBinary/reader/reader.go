package reader

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"time"
)

var ErrUvarintOverflow = errors.New("varint overflow")

const (
	SIZE_INT8  = 1
	SIZE_INT16 = 2
	SIZE_INT32 = 4
	SIZE_INT64 = 8
)

func DateUint16(n uint16) time.Time {
	return time.Unix(int64(n)*86400, 0).UTC()
}

type Reader struct {
	wrapped io.Reader

	buf []byte
}

func NewReader(rdr io.Reader) *Reader {
	return &Reader{
		wrapped: rdr,
		buf:     make([]byte, 65536),
	}
}

func (r *Reader) read(want int) ([]byte, error) {
	if n, err := r.wrapped.Read(r.buf[0:want]); err != nil {
		return nil, err
	} else if n < want {
		return nil, io.EOF
	} else {
		return r.buf[:want], nil
	}
}

func (r *Reader) ReadUvarint() (uint64, error) {
	var (
		x   uint64
		s   uint
		err error
		n   int
	)

	for i := 0; ; i++ {
		if i >= SIZE_INT64 {
			return 0, ErrUvarintOverflow
		}
		n, err = r.wrapped.Read(r.buf[i : i+1])
		if err != nil {
			return 0, err
		}
		if n != 1 {
			return 0, io.EOF
		}
		if r.buf[i] < 0x80 {
			if i > 9 || i == 9 && r.buf[i] > 1 {
				return 0, ErrUvarintOverflow
			}
			return x | uint64(r.buf[i])<<s, nil
		}
		x |= uint64(r.buf[i]&0x7f) << s
		s += 7
	}

}

func (r *Reader) ReadUint8() (uint8, error) {
	if buf, err := r.read(SIZE_INT8); err != nil {
		return 0, err
	} else {
		return buf[0], nil
	}
}

func (r *Reader) ReadUint16() (uint16, error) {
	if buf, err := r.read(SIZE_INT16); err != nil {
		return 0, err
	} else {
		return binary.LittleEndian.Uint16(buf), nil
	}
}

func (r *Reader) ReadUint32() (uint32, error) {
	if buf, err := r.read(SIZE_INT32); err != nil {
		return 0, err
	} else {
		return binary.LittleEndian.Uint32(buf), nil
	}
}

func (r *Reader) ReadUint64() (uint64, error) {
	if buf, err := r.read(SIZE_INT64); err != nil {
		return 0, err
	} else {
		return binary.LittleEndian.Uint64(buf), nil
	}
}

func (r *Reader) ReadFloat64() (float64, error) {
	if buf, err := r.read(SIZE_INT64); err != nil {
		return 0, err
	} else {
		return math.Float64frombits(binary.LittleEndian.Uint64(buf)), nil
	}
}

func (r *Reader) ReadStringBytes() ([]byte, error) {
	if u, err := r.ReadUvarint(); err != nil {
		return nil, err
	} else {
		if u == 0 {
			return []byte{}, nil
		} else if buf, err := r.read(int(u)); err != nil {
			return nil, err
		} else {
			return buf, nil
		}
	}
}

func (r *Reader) ReadString() (string, error) {
	if buf, err := r.ReadStringBytes(); err != nil {
		return "", err
	} else {
		return string(buf), nil
	}
}

func (r *Reader) ReadDate() (time.Time, error) {
	if t, err := r.ReadUint16(); err != nil {
		return time.Unix(0, 0), err
	} else {
		return DateUint16(t), nil
	}
}

func (r *Reader) ReadStringList() ([]string, error) {
	if u, err := r.ReadUvarint(); err != nil {
		return nil, err
	} else {
		if u == 0 {
			return []string{}, nil
		}
		n := int(u)
		sList := make([]string, n)
		for i := 0; i < n; i++ {
			if sList[i], err = r.ReadString(); err != nil {
				return sList, err
			}
		}
		return sList, nil
	}
}
