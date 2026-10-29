package main

import (
	"fmt"

	"github.com/anishathalye/porcupine"
)

const (
	opGet = iota
	opPut
	opDel
)

type kvInput struct {
	Op    int
	Key   string
	Value string
}

type kvOutput struct {
	Found bool
	Value string
}

type kvState struct {
	Found bool
	Value string
}

// kvModel is a linearizable key-value register per key. Writes and deletes
// always succeed in the model; a read is legal only if it returns the
// current state. Histories are partitioned by key, which keeps checking
// tractable (linearizability is compositional).
var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		var keys []string
		for _, op := range history {
			k := op.Input.(kvInput).Key
			if _, ok := byKey[k]; !ok {
				keys = append(keys, k)
			}
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() interface{} { return kvState{} },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(kvState)
		in := input.(kvInput)
		switch in.Op {
		case opGet:
			out := output.(kvOutput)
			return out.Found == st.Found && (!out.Found || out.Value == st.Value), st
		case opPut:
			return true, kvState{Found: true, Value: in.Value}
		default:
			return true, kvState{}
		}
	},
	Equal: func(a, b interface{}) bool { return a.(kvState) == b.(kvState) },
	DescribeOperation: func(input, output interface{}) string {
		in := input.(kvInput)
		switch in.Op {
		case opGet:
			out := output.(kvOutput)
			if !out.Found {
				return fmt.Sprintf("get(%s) -> nil", in.Key)
			}
			return fmt.Sprintf("get(%s) -> %s", in.Key, out.Value)
		case opPut:
			return fmt.Sprintf("put(%s, %s)", in.Key, in.Value)
		default:
			return fmt.Sprintf("del(%s)", in.Key)
		}
	},
	DescribeState: func(state interface{}) string {
		st := state.(kvState)
		if !st.Found {
			return "nil"
		}
		return st.Value
	},
}
