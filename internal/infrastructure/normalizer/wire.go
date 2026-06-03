package normalizer

import (
	"encoding/json"
	"sync"
)

var wireEncodePool = sync.Pool{
	New: func() any {
		return &wireEvent{}
	},
}

func MarshalWire(id, typ string, payload []byte, receivedAtUnixNano int64) ([]byte, error) {
	w := wireEncodePool.Get().(*wireEvent)
	defer func() {
		*w = wireEvent{}
		wireEncodePool.Put(w)
	}()

	w.ID = id
	w.Type = typ
	w.Payload = payload
	w.ReceivedAtUnixNano = receivedAtUnixNano
	return json.Marshal(w)
}
