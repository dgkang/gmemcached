package gmemcached

import (
	"fmt"
	"strconv"
)

func SizeOfBody(b interface{}) int {
	switch v := b.(type) {
	case []byte:
		return len(v)
	case float64:
		return len(fmt.Sprintf("%f", v))
	case nil:
		return 0
	case string:
		return len(v)
	default:
		return len(fmt.Sprintf("%v", v))
	}
	return 0
}

func String(v interface{}) string {
	switch k := v.(type) {
	case string:
		return k
	case []byte:
		return string(k)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", k)
	}
	return ""
}

func Int64(v interface{}) (int64, error) {
	switch k := v.(type) {
	case string:
		if i, e := strconv.ParseInt(k, 10, 64); e == nil {
			return i, nil
		} else {
			return 0, e
		}
	case []byte:
		if i, e := strconv.ParseInt(string(k), 10, 64); e == nil {
			return i, nil
		} else {
			return 0, e
		}
	case int64:
		return k, nil
	}
	return 0, fmt.Errorf("%v convert to int64 failed", v)
}

func Float64(v interface{}) (float64, error) {
	switch k := v.(type) {
	case string:
		if i, e := strconv.ParseFloat(k, 64); e == nil {
			return i, nil
		} else {
			return 0, e
		}
	case []byte:
		if i, e := strconv.ParseFloat(string(k), 64); e == nil {
			return i, nil
		} else {
			return 0, e
		}
	case float64:
		return k, nil
	}
	return 0, fmt.Errorf("%v convert to float64 failed", v)
}

func Bytes(v interface{}) []byte {
	switch k := v.(type) {
	case string:
		return []byte(k)
	case []byte:
		return k
	}
	return nil
}
