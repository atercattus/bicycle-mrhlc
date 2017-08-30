package main

import (
	"bytes"
)

type (
	JSValueType int
)

const (
	jsValueTypeString = JSValueType(iota)
	jsValueTypeNumeric
)

func isSpace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n'
}

func isNum(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

// {"type": [{item}, {item}, ...]}
func ParseData(buf []byte, expectedType []byte, itemCallback func(item []byte) bool) error {
	const (
		stateBegin = iota
		stateWaitTypeKey
		stateTypeKey
		stateWaitTypeColon
		stateWaitArray
		stateWaitHashmapBegin
		stateWaitHashmapEnd
		stateAfterHashmapWaitCommaOrArrayEnd
		stateWaitEnd
	)

	var (
		state       = stateBegin
		keyFrom     int
		hashmapFrom int
	)

	for idx, ch := range buf {
		switch state {
		case stateBegin:
			if ch == '{' {
				state = stateWaitTypeKey
			} else if !isSpace(ch) {
				return ErrWrongData
			}
		case stateWaitTypeKey:
			if ch == '"' {
				state = stateTypeKey
				keyFrom = idx
			} else if !isSpace(ch) {
				return ErrWrongData
			}
		case stateTypeKey:
			if ch == '"' {
				dataType := buf[keyFrom+1 : idx]
				if !bytes.Equal(dataType, expectedType) {
					return ErrWrongData
				}
				state = stateWaitTypeColon
			}
		case stateWaitTypeColon:
			if ch == ':' {
				state = stateWaitArray
			} else if !isSpace(ch) {
				return ErrWrongData
			}
		case stateWaitArray:
			if ch == '[' {
				state = stateWaitHashmapBegin
			} else if !isSpace(ch) {
				return ErrWrongData
			}
		case stateWaitHashmapBegin:
			if ch == '{' {
				state = stateWaitHashmapEnd
				hashmapFrom = idx
			} else if !isSpace(ch) {
				return ErrWrongData
			}
		case stateWaitHashmapEnd:
			if ch == '}' {
				item := buf[hashmapFrom : idx+1]
				if !itemCallback(item) {
					return ErrWrongData
				}
				state = stateAfterHashmapWaitCommaOrArrayEnd
			}
		case stateAfterHashmapWaitCommaOrArrayEnd:
			if ch == ',' {
				state = stateWaitHashmapBegin
			} else if ch == ']' {
				state = stateWaitEnd
			} else if !isSpace(ch) {
				return ErrWrongData
			}
		case stateWaitEnd:
			if ch == '}' {
				return nil
			} else if !isSpace(ch) {
				return ErrWrongData
			}
		default:
			return ErrWrongData
		}
	}

	return nil
}

func ParseItem(buf []byte, fieldCallback func(key, value []byte, valueType JSValueType) bool) error {
	const (
		stateBegin = iota
		stateWaitKey
		stateKey
		stateWaitColon
		stateWaitValue
		stateStringValue
		stateNumericValue
		statePositiveIntegerValue
		stateWaitCommaOrHashmapEnd
	)

	var (
		state          = stateBegin
		keyFrom, keyTo int
		valueFrom      int
	)

	for idx, ch := range buf {
		switch state {
		case stateBegin:
			if ch == '{' {
				state = stateWaitKey
			} else if !isSpace(ch) {
				return ErrWrongData
			}
		case stateWaitKey:
			if ch == '"' {
				state = stateKey
				keyFrom = idx
			} else if !isSpace(ch) {
				return ErrWrongData
			}
		case stateKey:
			if ch == '"' {
				state = stateWaitColon
				keyTo = idx
			}
		case stateWaitColon:
			if ch == ':' {
				state = stateWaitValue
			} else if !isSpace(ch) {
				return ErrWrongData
			}
		case stateWaitValue:
			valueFrom = idx
			if ch == '"' {
				state = stateStringValue
			} else if isNum(ch) {
				state = statePositiveIntegerValue
			} else if ch == '-' {
				state = stateNumericValue
			} else if !isSpace(ch) {
				return ErrWrongData
			}
		case stateNumericValue:
			if isNum(ch) {
				state = statePositiveIntegerValue
			} else {
				return ErrWrongData
			}
		case stateStringValue:
			if ch == '"' {
				key := buf[keyFrom+1 : keyTo]
				value := buf[valueFrom+1 : idx]
				if !fieldCallback(key, value, jsValueTypeString) {
					return ErrWrongData
				}
				state = stateWaitCommaOrHashmapEnd
			}
		case statePositiveIntegerValue:
			if isNum(ch) {
				// do nothing
			} else if (ch == ',') || (ch == '}') || isSpace(ch) {
				key := buf[keyFrom+1 : keyTo]
				value := buf[valueFrom:idx]
				if !fieldCallback(key, value, jsValueTypeNumeric) {
					return ErrWrongData
				}

				if ch == ',' {
					state = stateWaitKey
				} else if ch == '}' {
					return nil
				} else {
					state = stateWaitCommaOrHashmapEnd
				}
			} else {
				return ErrWrongData
			}
		case stateWaitCommaOrHashmapEnd:
			if ch == ',' {
				state = stateWaitKey
			} else if ch == '}' {
				return nil
			} else if !isSpace(ch) {
				return ErrWrongData
			}
		default:
			return ErrWrongData
		}
	}

	return nil
}
