package main

import (
	"fmt"
	"sort"

	"github.com/anishathalye/porcupine"
)

// staleReads finds reads that return a value already overwritten before the
// read began: the value's write w1 finished, then another write w2 to the
// same key started and finished, and only then the read was invoked. Every
// write value is unique, so such a read is a definite linearizability
// violation independent of how concurrent operations are ordered. Porcupine
// says *whether* a history is linearizable; this says *why* in terms a
// human can check.
func staleReads(history []porcupine.Operation) (count int, examples []string) {
	type write struct {
		call, ret int64
		desc      string
	}
	writesByKey := map[string][]write{}
	writerOf := map[string]porcupine.Operation{}
	for _, op := range history {
		in := op.Input.(kvInput)
		if in.Op == opGet || op.Return == int64(^uint64(0)>>1) {
			continue // reads, and writes whose outcome is unknown
		}
		writesByKey[in.Key] = append(writesByKey[in.Key], write{op.Call, op.Return, kvModel.DescribeOperation(in, op.Output)})
		if in.Op == opPut {
			writerOf[in.Value] = op
		}
	}

	// Per key: writes sorted by call time, with suffix minimum of return time.
	type index struct {
		writes []write
		minRet []int64
	}
	idx := map[string]index{}
	for k, ws := range writesByKey {
		sort.Slice(ws, func(i, j int) bool { return ws[i].call < ws[j].call })
		minRet := make([]int64, len(ws)+1)
		minRet[len(ws)] = int64(^uint64(0) >> 1)
		for i := len(ws) - 1; i >= 0; i-- {
			minRet[i] = min(ws[i].ret, minRet[i+1])
		}
		idx[k] = index{ws, minRet}
	}

	for _, op := range history {
		in := op.Input.(kvInput)
		out, _ := op.Output.(kvOutput)
		if in.Op != opGet || !out.Found {
			continue
		}
		w1, ok := writerOf[out.Value]
		if !ok {
			continue
		}
		ix := idx[in.Key]
		// First write that starts strictly after w1 returned.
		i := sort.Search(len(ix.writes), func(i int) bool { return ix.writes[i].call > w1.Return })
		if i < len(ix.writes) && ix.minRet[i] < op.Call {
			count++
			if len(examples) < 3 {
				var w2 write
				for _, w := range ix.writes[i:] {
					if w.ret < op.Call {
						w2 = w
						break
					}
				}
				examples = append(examples, fmt.Sprintf(
					"client %d read %s=%s at t=%.3fms, but %s finished at t=%.3fms after that value's write finished at t=%.3fms",
					op.ClientId, in.Key, out.Value, ms(op.Call), w2.desc, ms(w2.ret), ms(w1.Return)))
			}
		}
	}
	return count, examples
}

func ms(ns int64) float64 { return float64(ns) / 1e6 }

// inversions finds new-old inversions: read r1 observes write w's value,
// then a read r2 that begins after r1 completed observes the value of a
// write u that had completed before w even began. Once r1 has shown w,
// every later read must show w or something newer.
func inversions(history []porcupine.Operation) (count int, examples []string) {
	type read struct {
		op     porcupine.Operation
		writer porcupine.Operation
	}
	writerOf := map[string]porcupine.Operation{}
	for _, op := range history {
		if in := op.Input.(kvInput); in.Op == opPut {
			writerOf[in.Value] = op
		}
	}
	readsByKey := map[string][]read{}
	for _, op := range history {
		in := op.Input.(kvInput)
		out, _ := op.Output.(kvOutput)
		if in.Op != opGet || !out.Found {
			continue
		}
		if w, ok := writerOf[out.Value]; ok {
			readsByKey[in.Key] = append(readsByKey[in.Key], read{op, w})
		}
	}
	for key, reads := range readsByKey {
		sort.Slice(reads, func(i, j int) bool { return reads[i].op.Call < reads[j].op.Call })
		for i, r1 := range reads {
			for _, r2 := range reads[i+1:] {
				if r2.op.Call <= r1.op.Return || r2.writer.Return >= r1.writer.Call {
					continue
				}
				count++
				if len(examples) < 3 {
					examples = append(examples, fmt.Sprintf(
						"%s: client %d read %s (t=%.3f-%.3fms), then client %d read older %s (t=%.3f-%.3fms); %s finished at t=%.3fms before %s started at t=%.3fms",
						key, r1.op.ClientId, r1.writer.Input.(kvInput).Value, ms(r1.op.Call), ms(r1.op.Return),
						r2.op.ClientId, r2.writer.Input.(kvInput).Value, ms(r2.op.Call), ms(r2.op.Return),
						r2.writer.Input.(kvInput).Value, ms(r2.writer.Return), r1.writer.Input.(kvInput).Value, ms(r1.writer.Call)))
				}
			}
		}
	}
	return count, examples
}
