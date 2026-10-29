package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"
)

// Minimal RESP2 client with a per-request deadline.
type RespClient struct {
	addr    string
	timeout time.Duration
	conn    net.Conn
	rd      *bufio.Reader
}

// ServerError is a "-ERR ..." reply: the server answered, the operation failed.
type ServerError struct{ Msg string }

func (e *ServerError) Error() string { return e.Msg }

type Reply struct {
	Kind byte // '+', ':', '$', '*'
	Str  string
	Int  int64
	Nil  bool
}

func NewRespClient(addr string, timeout time.Duration) *RespClient {
	return &RespClient{addr: addr, timeout: timeout}
}

func (c *RespClient) Close() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func (c *RespClient) Do(args ...string) (Reply, error) {
	if c.conn == nil {
		conn, err := net.DialTimeout("tcp", c.addr, c.timeout)
		if err != nil {
			return Reply{}, err
		}
		c.conn = conn
		c.rd = bufio.NewReader(conn)
	}
	c.conn.SetDeadline(time.Now().Add(c.timeout))

	buf := []byte(fmt.Sprintf("*%d\r\n", len(args)))
	for _, a := range args {
		buf = append(buf, fmt.Sprintf("$%d\r\n%s\r\n", len(a), a)...)
	}
	if _, err := c.conn.Write(buf); err != nil {
		c.Close()
		return Reply{}, err
	}
	r, err := c.readReply()
	if err != nil {
		var se *ServerError
		if !errors.As(err, &se) {
			c.Close() // the connection state is unknown after an I/O error
		}
	}
	return r, err
}

func (c *RespClient) readLine() (string, error) {
	line, err := c.rd.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", errors.New("malformed line")
	}
	return line[:len(line)-2], nil
}

func (c *RespClient) readReply() (Reply, error) {
	line, err := c.readLine()
	if err != nil {
		return Reply{}, err
	}
	if line == "" {
		return Reply{}, errors.New("empty reply")
	}
	switch line[0] {
	case '+':
		return Reply{Kind: '+', Str: line[1:]}, nil
	case '-':
		return Reply{}, &ServerError{Msg: line[1:]}
	case ':':
		n, err := strconv.ParseInt(line[1:], 10, 64)
		return Reply{Kind: ':', Int: n}, err
	case '$':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return Reply{}, err
		}
		if n < 0 {
			return Reply{Kind: '$', Nil: true}, nil
		}
		data := make([]byte, n+2)
		if _, err := ioReadFull(c.rd, data); err != nil {
			return Reply{}, err
		}
		return Reply{Kind: '$', Str: string(data[:n])}, nil
	default:
		return Reply{}, fmt.Errorf("unsupported reply type %q", line[0])
	}
}

func ioReadFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
