package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

type Value struct {
	typ   string
	str   string
	num   int
	bulk  string
	array []Value
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	fmt.Println("RESP: Creating new Resp reader")
	return &Resp{reader: bufio.NewReader(rd)}
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
	fmt.Printf("RESP: Read line: %q\n", line[:len(line)-2])
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
	fmt.Printf("RESP: Read integer: %d\n", i64)
	return int(i64), n, nil
}

func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	fmt.Printf("RESP: Reading type: %c\n", _type)

	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	case STRING:
		return r.readString()
	case ERROR:
		return r.readError()
	case INTEGER:
		return r.readIntegerValue()
	default:
		fmt.Printf("RESP: Unknown type: %v\n", string(_type))
		return Value{}, fmt.Errorf("unknown type: %v", string(_type))
	}
}

func (r *Resp) readArray() (Value, error) {
	fmt.Println("RESP: Reading array")
	v := Value{}
	v.typ = "array"

	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	fmt.Printf("RESP: Array length: %d\n", len)

	v.array = make([]Value, 0)
	for i := 0; i < len; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}
		v.array = append(v.array, val)
	}

	return v, nil
}

func (r *Resp) readBulk() (Value, error) {
	fmt.Println("RESP: Reading bulk string")
	v := Value{}
	v.typ = "bulk"

	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	fmt.Printf("RESP: Bulk string length: %d\n", len)

	bulk := make([]byte, len)
	r.reader.Read(bulk)
	v.bulk = string(bulk)

	// Read the trailing CRLF
	r.readLine()

	fmt.Printf("RESP: Bulk string content: %s\n", v.bulk)
	return v, nil
}

func (r *Resp) readString() (Value, error) {
	fmt.Println("RESP: Reading string")
	v := Value{}
	v.typ = "string"

	line, _, err := r.readLine()
	if err != nil {
		return v, err
	}

	v.str = string(line)
	fmt.Printf("RESP: String content: %s\n", v.str)
	return v, nil
}

func (r *Resp) readError() (Value, error) {
	fmt.Println("RESP: Reading error")
	v := Value{}
	v.typ = "error"

	line, _, err := r.readLine()
	if err != nil {
		return v, err
	}

	v.str = string(line)
	fmt.Printf("RESP: Error content: %s\n", v.str)
	return v, nil
}

func (r *Resp) readIntegerValue() (Value, error) {
	fmt.Println("RESP: Reading integer value")
	v := Value{}
	v.typ = "integer"

	num, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	v.num = num
	fmt.Printf("RESP: Integer value: %d\n", v.num)
	return v, nil
}

func (v Value) Marshal() []byte {
	switch v.typ {
	case "array":
		return v.marshalArray()
	case "bulk":
		return v.marshalBulk()
	case "string":
		return v.marshalString()
	case "null":
		return v.marshallNull()
	case "error":
		return v.marshallError()
	case "integer":
		return v.marshalInteger()
	default:
		return []byte{}
	}
}

func (v Value) marshalString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')
	fmt.Printf("RESP: Marshalled string: %q\n", bytes)
	return bytes
}

func (v Value) marshalBulk() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')
	fmt.Printf("RESP: Marshalled bulk: %q\n", bytes)
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

	fmt.Printf("RESP: Marshalled array: %q\n", bytes)
	return bytes
}

func (v Value) marshallError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')
	fmt.Printf("RESP: Marshalled error: %q\n", bytes)
	return bytes
}

func (v Value) marshallNull() []byte {
	fmt.Println("RESP: Marshalled null")
	return []byte("$-1\r\n")
}

func (v Value) marshalInteger() []byte {
	var bytes []byte
	bytes = append(bytes, INTEGER)
	bytes = append(bytes, strconv.Itoa(v.num)...)
	bytes = append(bytes, '\r', '\n')
	fmt.Printf("RESP: Marshalled integer: %q\n", bytes)
	return bytes
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	fmt.Println("RESP: Creating new Writer")
	return &Writer{writer: w}
}

func (w *Writer) Write(v Value) error {
	bytes := v.Marshal()
	fmt.Printf("RESP: Writing response: %q\n", bytes)
	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}
