package protocol

import (
	"reflect"
	"testing"
)

func TestOpts_Config(t *testing.T) {
	tests := []struct {
		name   string
		given  *Opts
		expect *Opts
	}{
		{
			name: "Test replicaof",
			given: &Opts{
				ReplicaOf: "127.0.0.1 9887",
			},
			expect: &Opts{
				ReplicaOf:  "127.0.0.1 9887",
				Role:       "slave",
				MasterHost: "127.0.0.1",
				MasterPort: "9887",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &Opts{
				PortNum:    tt.given.PortNum,
				ReplicaOf:  tt.given.ReplicaOf,
				Dir:        tt.given.Dir,
				Dbfilename: tt.given.Dbfilename,
			}
			o.Config()
			if !reflect.DeepEqual(tt.expect, o) {
				t.Errorf("the object is not as expected: expected(%v) vs (%v)", tt.expect, o)
			}
		})
	}
}
