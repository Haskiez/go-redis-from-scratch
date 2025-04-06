package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// https://redis.io/docs/latest/develop/reference/protocol-spec/#resp-protocol-description
const (
	STRING           = '+'
	ERROR            = '-'
	INTEGER          = ':'
	BULK             = '$'
	ARRAY            = '*'
	NULL             = '_'
	BOOLEAN          = '#'
	DOUBLES          = ','
	BIG_NUMBERS      = '('
	BULK_ERRORS      = '!'
	VERBATIM_STRINGS = '='
	MAPS             = '%'
	ATTRIBUTES       = '`'
	SETS             = '~'
	PUSHES           = '>'
)

type Value struct {
	typ   string
	str   string
	num   int
	bulk  string
	array []Value
}

func ErrorValue(err string) Value {
	return Value{typ: "error", str: err}
}

func NullValue() Value {
	return Value{typ: "null"}
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(reader io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(reader)}
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}
	return line[:len(line)-2], n, nil
}

func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		return Value{}, fmt.Errorf("unknown type: %v", string(_type))
	}
}

func (r *Resp) readArray() (Value, error) {
	v := Value{typ: "array"}

	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}
	v.array = make([]Value, length)
	for i := 0; i < length; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}
		v.array[i] = val
	}
	return v, nil
}

func (r *Resp) readBulk() (Value, error) {
	v := Value{typ: "bulk"}

	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, length)

	r.reader.Read(bulk)
	v.bulk = string(bulk)
	r.readLine()
	return v, nil
}

func (v Value) Marshal() []byte {
	switch v.typ {
	case "string":
		return v.marshalString()
	case "integer":
		return v.marshalInteger()
	case "bulk":
		return v.marshalBulk()
	case "array":
		return v.marshalArray()
	case "map":
		return v.marshalMap()
	case "error":
		return v.marshalError()
	case "null":
		return v.marshalNull()
	default:
		return []byte{}
	}
}

func (v Value) marshalString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v Value) marshalInteger() []byte {
	var bytes []byte
	bytes = append(bytes, INTEGER)
	bytes = append(bytes, []byte(strconv.Itoa(v.num))...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v Value) marshalBulk() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v Value) marshalArray() []byte {
	len := len(v.array)
	var bytes []byte
	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len; i++ {
		bytes = append(bytes, v.array[i].Marshal()...)
	}

	return bytes
}

func (v Value) marshalMap() []byte {
	len := len(v.array)
	var bytes []byte
	bytes = append(bytes, MAPS)
	bytes = append(bytes, strconv.Itoa(len/2)...)
	bytes = append(bytes, '\r', '\n')
	for i := 0; i < len; i++ {
		bytes = append(bytes, v.array[i].Marshal()...)
	}
	return bytes
}

func (v Value) marshalError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v Value) marshalNull() []byte {
	if respVersion == 3 {
		return []byte{NULL, '\r', '\n'}
	}
	return []byte("$-1\r\n")
}

func (w *Writer) Write(v Value) error {
	var bytes = v.Marshal()
	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}
