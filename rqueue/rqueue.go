package rqueue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"time"
)

// Top level Client wrapper. While the input and output messages are strings, the protocol
// is binary based. Straight up byte arguments will be supported in subsequent client releases
type RQueue struct {
	connection net.Conn
	reader     *bufio.Reader
}

// Subscribes the client to the topic
func (r *RQueue) Subscribe(topic string) {
	buf := new(bytes.Buffer)

	topicSlice := []byte(topic)
	topicSize := len(topicSlice)
	binary.Write(buf, binary.BigEndian, uint16(topicSize))
	buf.WriteByte(1)
	binary.Write(buf, binary.BigEndian, topicSlice)

	err := binary.Write(r.connection, binary.LittleEndian, buf.Bytes())
	if err != nil {
		log.Fatal(err)
	}
}

// Notifies or publishes other clients on the topic with the message
func (r *RQueue) Notify(topic string, message string) {
	buf := new(bytes.Buffer)

	topicSlice := []byte(topic)
	messageSlice := []byte(message)

	topicSize := len(topicSlice)
	messageSize := len(messageSlice)

	binary.Write(buf, binary.BigEndian, uint16(topicSize+messageSize+1))
	buf.WriteByte(7)
	buf.WriteByte(byte(topicSize))
	binary.Write(buf, binary.BigEndian, topicSlice)
	binary.Write(buf, binary.BigEndian, messageSlice)

	err := binary.Write(r.connection, binary.LittleEndian, buf.Bytes())
	if err != nil {
		log.Fatal(err)
	}

}

type Notify struct {
	topic   string
	message string
}

// GetMessage will receive the next message sitting on the wire, assumed to be of type Notify
func (r *RQueue) GetMessage() (*Notify, error) {
	/// assumes that the message is of type notify
	messageBytes, err := r.GetMessageBytes()
	if err != nil {
		return nil, err
	} else {
		topicLen := int(messageBytes[0])
		topic := messageBytes[1 : topicLen+1]
		text := messageBytes[topicLen+1:]
		return &Notify{topic: string(topic), message: string(text)}, nil
	}
}

// Gets the payload bytes for next message, blocking the current goroutine until one is assembled. Type is erased.
func (r *RQueue) GetMessageBytes() ([]byte, error) {
	bytes_read := 0
	preamble := make([]byte, 3)
	for {
		length, err := r.connection.Read(preamble[bytes_read:])
		if err != nil {
			return nil, err
		} else {
			bytes_read += length
			if bytes_read == len(preamble) {
				break
			}
		}
	}
	payload_read := 0
	payload_size := int((preamble[0] << 8) + preamble[1])
	payload := make([]byte, payload_size)
	for {
		length, err := r.connection.Read(payload[payload_read:])
		if err != nil {
			return nil, err
		} else {
			payload_read += length
			if payload_read == payload_size {
				return payload, nil
			}
		}
	}
}

// configuration settings for the client
type Config struct {
	// the hostname of the remote RQueue server
	host string
	// the TCP port of the remote RQueue server
	port int
}

// constructs a new RQueue client
func NewClient(config Config) (*RQueue, error) {
	conn, err := connect(config.host, config.port)
	if err != nil {
		return nil, err
	} else {
		return &RQueue{connection: conn, reader: bufio.NewReader(conn)}, nil
	}
}

func connect(host string, port int) (net.Conn, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	return net.Dial("tcp", addr)
}

//main contains example usage
func main() {
	config := Config{host: "0.0.0.0", port: 6567}
	rqueue, err := NewClient(config)
	if err != nil {
		log.Fatal(err)
	} else {
		rqueue.Subscribe("hello")
		time.Sleep(300 * time.Millisecond)
		rqueue.Notify("hello", "world")

		go func() {
			for {
				mess, err := rqueue.GetMessage()
				if err == nil {
					fmt.Println(mess.topic)
					fmt.Println(mess.message)
				}
			}
		}()

		select {}
	}

}
