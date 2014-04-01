package gmemcached

import (
	"bytes"
)

type CommandSession struct {
	CmdType     CommandType
	ReplyType   RespondType
	requestBody *bytes.Buffer
	replyBody   *bytes.Buffer
	value       map[string]interface{}
}

func (C *CommandSession) Error() string {
	if v, ok := C.value["error"]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func (C *CommandSession) Values() map[string]interface{} {
	return C.value
}

func (C *CommandSession) Item(key string) map[string]interface{} {
	if V, ok := C.value[key]; ok {
		if v, ok := V.(map[string]interface{}); ok {
			return v
		}
	}
	return nil
}

func (C *CommandSession) RequestBody() string {
	if C.requestBody != nil {
		return C.requestBody.String()
	}
	return ""
}

func (C *CommandSession) ReplyBody() string {
	if C.replyBody != nil {
		return C.replyBody.String()
	}
	return ""
}
