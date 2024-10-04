package protocol

import (
	"testing"
)

var (
	cache = map[string]*Entry{
		"entry": {
			value:    "A",
			expireAt: 0,
		},
		"expiredEntry": {
			value:    "A",
			expireAt: 1234,
		},
	}
)

func TestStorage_Get(t *testing.T) {
	type args struct {
		cache map[string]*Entry
		key   string
	}
	tests := []struct {
		name string
		args args
		want *string
	}{
		{
			name: "Test get",
			args: args{
				cache: cache,
				key:   "entry",
			},
			want: &cache["entry"].value,
		},
		{
			name: "Test get with nonexisting value",
			args: args{
				cache: cache,
				key:   "abc",
			},
			want: nil,
		},
		{
			name: "Test get for expired entry",
			args: args{
				cache: cache,
				key:   "expiredEntry",
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Storage{
				cache: tt.args.cache,
			}
			if got := s.Get(tt.args.key); got != tt.want {
				t.Errorf("Storage.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStorage_Set(t *testing.T) {
	type fields struct {
		cache map[string]*Entry
	}
	type args struct {
		key      string
		value    string
		expireAt int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "Test set without expiry",
			fields: fields{
				cache: cache,
			},
			args: args{
				key:      "key1",
				value:    "value1",
				expireAt: 0,
			},
		},
		{
			name: "Test set with expiry",
			fields: fields{
				cache: cache,
			},
			args: args{
				key:      "key1",
				value:    "value1",
				expireAt: 13532521,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Storage{
				cache: tt.fields.cache,
			}
			s.Set(tt.args.key, tt.args.value, tt.args.expireAt)
		})
	}
}
