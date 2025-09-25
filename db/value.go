package db

type Value struct {
	val []byte
}

func NewValue(v []byte) Value {
	return Value{val: v}
}

func (v Value) GetValue() []byte {
	return v.val[:]
}

func (v Value) GetSize() uint32 {
	return uint32(len(v.val))
}
