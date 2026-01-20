package taskharbor

import "encoding/json"

/*
This interface is responsible for converting the user
payloads into bytes and vice-versa.
DRIVERS SHOULD ONLY STORE BYTES!
*/
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

/*
JsonCodec is the default codec implementation using encoding/json.
*/
type JsonCodec struct{}

/*
This function marshals a payload into bytes using JSON.
*/
func (c JsonCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

/*
This function unmarshals bytes into the provided pointer using JSON.
*/
func (c JsonCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
