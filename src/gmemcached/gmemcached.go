package gmemcached

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type GMConnection struct {
	conn         net.Conn
	writer       *bufio.Writer
	reader       *bufio.Reader
	readTimeout  time.Duration
	writeTimeout time.Duration
	server       string
	port         int
}

type CommandType uint32

const (
	InvalidCommand    CommandType = 0
	StorageCommand    CommandType = 1
	RetrievalCommand  CommandType = 2
	DeletionCommand   CommandType = 3
	IncrDecrCommand   CommandType = 4
	TouchCommand      CommandType = 5
	SlabsCommand      CommandType = 6
	StatisticsCommand CommandType = 7
	DeletedCommand    CommandType = 8
)

type RespondType uint32

const (
	RT_UNKNOW      RespondType = 0
	RT_STORED      RespondType = 1
	RT_NOTSTORED   RespondType = 2
	RT_EXISTS      RespondType = 3
	RT_NOTFOUND    RespondType = 4
	RT_ERROR       RespondType = 5
	RT_CLIENTERROR RespondType = 6
	RT_SERVERERROR RespondType = 7
	RT_DELETED     RespondType = 8
	RT_TOUCHED     RespondType = 9
	RT_OK          RespondType = 10
	RT_BUSY        RespondType = 11
	RT_BADCLASS    RespondType = 12
	RT_NOSPARE     RespondType = 13
	RT_NOTFULL     RespondType = 14
	RT_UNSAFE      RespondType = 15
	RT_SAME        RespondType = 16
	RT_VALUE       RespondType = 17
)

type ReadStatus uint32

const (
	rs_START ReadStatus = 0
	rs_Body  ReadStatus = 1
	rs_Next  ReadStatus = 2
	rs_END   ReadStatus = 3
)

func ConnectTimeout(server string, port int, ct time.Duration, wt time.Duration, rt time.Duration) (*GMConnection, error) {
	CM := &GMConnection{server: server, port: port, readTimeout: rt, writeTimeout: wt}
	address := fmt.Sprintf("%s:%d", server, port)
	if conn, e := net.DialTimeout("tcp", address, ct); e == nil {
		CM.conn = conn
		CM.writer = bufio.NewWriter(conn)
		CM.reader = bufio.NewReader(conn)
		return CM, nil
	} else {
		return nil, e
	}
}

func Connect(server string, port int) (*GMConnection, error) {
	CM := &GMConnection{server: server, port: port}
	address := fmt.Sprintf("%s:%d", server, port)
	if conn, e := net.Dial("tcp", address); e == nil {
		CM.conn = conn
		CM.writer = bufio.NewWriter(conn)
		CM.reader = bufio.NewReader(conn)
		return CM, nil
	} else {
		return nil, e
	}
}

func (G *GMConnection) commandType(cmd string) (CommandType, string) {
	cmd = strings.ToLower(cmd)
	switch {
	case cmd == "set" || cmd == "add" || cmd == "replace" || cmd == "append" || cmd == "prepend" || cmd == "cas":
		return StorageCommand, cmd

	case cmd == "get" || cmd == "gets":
		return RetrievalCommand, cmd

	case cmd == "delete":
		return DeletedCommand, cmd

	case cmd == "incr" || cmd == "decr":
		return IncrDecrCommand, cmd

	case cmd == "touch":
		return TouchCommand, cmd

	case cmd == "slabs":
		return SlabsCommand, cmd

	case cmd == "stats":
		return StatisticsCommand, cmd
	}
	return InvalidCommand, ""
}

func (G *GMConnection) CreateCommand(cmd string, args ...interface{}) (*CommandSession, error) {
	session := &CommandSession{InvalidCommand,
		RT_UNKNOW,
		new(bytes.Buffer),
		new(bytes.Buffer),
		make(map[string]interface{})}

	cmdType, command := G.commandType(cmd)
	if cmdType == InvalidCommand {
		return nil, fmt.Errorf("command \"%s\" invalid", cmd)
	}
	session.CmdType = cmdType
	buf := session.requestBody

	fmt.Fprintf(buf, "%s", command)

	for _, v := range args {
		switch v := v.(type) {
		case []byte:
			fmt.Fprintf(buf, " ")
			buf.Write(v)
		case float64:
			fmt.Fprintf(buf, " %f", v)
		case nil:
			fmt.Fprintf(buf, " 0")
		default:
			fmt.Fprintf(buf, " %v", v)
		}
	}
	fmt.Fprintf(buf, "\r\n")
	return session, nil
}

func (G *GMConnection) SendCommand(session *CommandSession, data interface{}) error {
	buf := session.requestBody
	switch v := data.(type) {
	case []byte:
		buf.Write(v)
		fmt.Fprintf(buf, "\r\n")

	case float64:
		fmt.Fprintf(buf, "%f", v)
		fmt.Fprintf(buf, "\r\n")

	case nil:

	default:
		fmt.Fprintf(buf, "%v", v)
		fmt.Fprintf(buf, "\r\n")
	}

	if G.writeTimeout != 0 {
		G.conn.SetWriteDeadline(time.Now().Add(G.writeTimeout))
	}
	if _, e := G.writer.Write(session.requestBody.Bytes()); e != nil {
		return e
	}
	if e := G.writer.Flush(); e != nil {
		return e
	}
	return G.analyzeReply(session)
}

func (G *GMConnection) readLine(session *CommandSession, rs ReadStatus, key string) (ReadStatus, string, error) {

	if rs == rs_Body {
		var item map[string]interface{}
		if V, ok := session.value[key]; ok {
			if v, ok := V.(map[string]interface{}); ok {
				item = v
			}
		}
		if item == nil {
			return rs_END, "", fmt.Errorf("not find key")
		}
		var bl int64 = -1
		var e error
		if s, ok := item["bytes"]; ok {
			if str, ok := s.(string); ok {
				bl, e = strconv.ParseInt(str, 10, 64)
			}
		}
		if bl == -1 || e != nil {
			return rs_END, "", fmt.Errorf("body length error")
		}
		bl += 2
		bb := make([]byte, bl)

		var m int64 = 0
		var n int
		for {
			if G.readTimeout != 0 {
				G.conn.SetReadDeadline(time.Now().Add(G.readTimeout))
			}
			if n, e = G.reader.Read(bb[m:]); e != nil {
				return rs_END, "", e
			}
			session.replyBody.Write(bb[m : m+int64(n)])
			m += int64(n)
			if m >= bl {
				break
			}
		}
		b := bytes.Trim(bb, "\r\n")
		item["data"] = b
		return rs_Next, "", nil
	}

	if G.readTimeout != 0 {
		G.conn.SetReadDeadline(time.Now().Add(G.readTimeout))
	}
	b, e := G.reader.ReadBytes('\n')
	if e != nil {
		return rs_END, "", e
	}
	session.replyBody.Write(b)

	s := strings.Trim(string(b), "\r\n")
	as := strings.Split(s, " ")
	prefix := as[0]

	switch prefix {
	case "ERROR":
		session.ReplyType = RT_ERROR
		return rs_END, "", nil

	case "STORED":
		session.ReplyType = RT_STORED
		return rs_END, "", nil

	case "NOT_STORED":
		session.ReplyType = RT_NOTSTORED
		return rs_END, "", nil

	case "EXISTS":
		session.ReplyType = RT_EXISTS
		return rs_END, "", nil

	case "NOT_FOUND":
		session.ReplyType = RT_NOTFOUND
		return rs_END, "", nil

	case "DELETED":
		session.ReplyType = RT_DELETED
		return rs_END, "", nil

	case "TOUCHED":
		session.ReplyType = RT_TOUCHED
		return rs_END, "", nil

	case "CLIENT_ERROR":
		session.ReplyType = RT_CLIENTERROR
		session.value["error"] = strings.Join(as[1:], " ")
		return rs_END, "", nil

	case "SERVER_ERROR":
		session.ReplyType = RT_SERVERERROR
		session.value["error"] = strings.Join(as[1:], " ")
		return rs_END, "", nil

	case "BUSY":
		session.ReplyType = RT_BUSY
		session.value["message"] = strings.Join(as[1:], " ")
		return rs_END, "", nil

	case "BADCLASS":
		session.ReplyType = RT_BADCLASS
		session.value["message"] = strings.Join(as[1:], " ")
		return rs_END, "", nil

	case "NOSPARE":
		session.ReplyType = RT_NOSPARE
		session.value["message"] = strings.Join(as[1:], " ")
		return rs_END, "", nil

	case "NOTFULL":
		session.ReplyType = RT_NOTFULL
		session.value["message"] = strings.Join(as[1:], " ")
		return rs_END, "", nil

	case "UNSAFE":
		session.ReplyType = RT_UNSAFE
		session.value["message"] = strings.Join(as[1:], " ")
		return rs_END, "", nil

	case "SAME":
		session.ReplyType = RT_SAME
		session.value["message"] = strings.Join(as[1:], " ")
		return rs_END, "", nil

	case "VALUE":
		session.ReplyType = RT_VALUE
		session.value[as[1]] = make(map[string]interface{})
		V := session.value[as[1]]
		if v, ok := V.(map[string]interface{}); ok {
			v["flags"] = as[2]
			v["bytes"] = as[3]
			if len(as) >= 5 {
				v["unique"] = as[4]
			}
		}
		return rs_Body, as[1], nil

	case "STAT":
		session.ReplyType = RT_VALUE
		session.value[as[1]] = as[2]
		return rs_Next, "", nil

	case "END":
		return rs_END, "", nil
	}

	if session.CmdType == IncrDecrCommand {
		session.ReplyType = RT_VALUE
		session.value["data"] = as[0]
		return rs_END, "", nil
	}
	return rs_END, "", nil
}

func (G *GMConnection) analyzeReply(session *CommandSession) error {
	var key string
	var rs ReadStatus = rs_START
	var e error
	for {
		if rs, key, e = G.readLine(session, rs, key); e != nil || rs == rs_END {
			break
		}
	}
	return e
}
