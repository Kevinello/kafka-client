package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

// magicNum magic number for 2023 version of kafka message
//
//	@update 2023-02-23 02:30:12
const magicNum int8 = 0x23

// MessageHeader kafka message header
//
//	@author kevineluo
//	@update 2023-02-23 02:36:13
type MessageHeader struct {
	magic     int8
	Timestamp int64
	MetaLen   int
	BodyLen   int
}

// Message kafka message
//
//	@author kevineluo
//	@update 2023-02-23 10:42:47
type Message struct {
	// Header contains information about the message, such as version numbers and message types.
	Header MessageHeader
	// Meta is an optional field for storing arbitrary data associated with a message
	Meta []byte
	// Body is the main payload of the message.
	Body []byte
}

// NewMessage new empty message
//
//	@param appID int
//	@param plugin int
//	@param metaPart []byte
//	@param bodyPart []byte
//	@return item *Message
//	@author kevineluo
//	@update 2023-02-23 02:39:40
func NewMessage(meta []byte, body []byte) (item *Message) {
	header := MessageHeader{
		magicNum,
		time.Now().Unix(),
		len(meta),
		len(body),
	}
	item = &Message{
		header,
		meta,
		body,
	}
	return
}

// Bytes convert to pure bytes
//
//	@receiver msg *Message
//	@return b []byte
//	@return err error
//	@author kevineluo
//	@update 2023-02-23 02:41:55
func (msg *Message) Bytes() (b []byte, err error) {
	var buf bytes.Buffer

	if err = binary.Write(&buf, binary.LittleEndian, msg.Header); err != nil {
		return
	}
	if err = binary.Write(&buf, binary.LittleEndian, msg.Meta); err != nil {
		return
	}
	if err = binary.Write(&buf, binary.LittleEndian, msg.Body); err != nil {
		return
	}
	b = buf.Bytes()

	return
}

// ParseMessage parse kafka message from bytes
//
//	@param b []byte
//	@return msg *Message
//	@return err error
//	@author kevineluo
//	@update 2023-02-23 02:53:58
func ParseMessage(b []byte) (msg *Message, err error) {
	var buf = bytes.NewReader(b)
	var header = MessageHeader{}
	msg = &Message{}
	if err = binary.Read(buf, binary.LittleEndian, &header); err != nil {
		return
	}

	if header.magic != magicNum {
		err = fmt.Errorf("[ParseMessage] magic not match, we want %x, but get %x", magicNum, header.magic)
		return
	}

	metaStart := binary.Size(header)
	metaEnd := metaStart + int(header.MetaLen)
	bodyStart := metaEnd
	bodyEnd := bodyStart + int(header.BodyLen)
	if bodyEnd != len(b) {
		err = fmt.Errorf("[ParseMessage] bytes length not match, message length declared in header: %d, but in fact: %d", bodyEnd, len(b))
		return
	}

	msg.Header = header
	msg.Meta = b[metaStart:metaEnd]
	msg.Body = b[bodyStart:bodyEnd]
	return
}
