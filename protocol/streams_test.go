package protocol

import (
	"fmt"
	"testing"
	"time"
)

var (
	emptyTestStream = &Stream{
		entries: []*StreamEntry{},
	}

	testStream1 = &Stream{
		entries: []*StreamEntry{
			{
				id:      "1-1",
				kvpairs: map[string]string{"key1": "value1"},
			},
		},
	}

	testStream2 = &Stream{
		entries: []*StreamEntry{
			{
				id:      "0-2",
				kvpairs: map[string]string{"key1": "value1"},
			},
		},
	}

	corruptedStream1 = &Stream{
		entries: []*StreamEntry{
			{
				id:      "a-b",
				kvpairs: map[string]string{"key1": "value1"},
			},
		},
	}
)

func Test_validateStreamEntryID(t *testing.T) {
	type args struct {
		stream *Stream
		id     string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Test validateStreamEntryID 1",
			args: args{
				stream: testStream1,
				id:     "0-0",
			},
			want:    "-ERR The ID specified in XADD must be greater than 0-0\r\n",
			wantErr: false,
		},
		{
			name: "Test validateStreamEntryID 2",
			args: args{
				stream: testStream1,
				id:     "1-1",
			},
			want:    "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n",
			wantErr: false,
		},
		{
			name: "Test validateStreamEntryID 3",
			args: args{
				stream: testStream1,
				id:     "1-2",
			},
			want:    "",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := validateStreamEntryID(tt.args.stream, tt.args.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateStreamEntryID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("validateStreamEntryID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_autoGenSeqNum(t *testing.T) {
	type args struct {
		stream *Stream
		id     string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Test autoGenSeqNum 1",
			args: args{
				stream: emptyTestStream,
				id:     "1-*",
			},
			want:    "0",
			wantErr: false,
		},
		{
			name: "Test autoGenSeqNum 2",
			args: args{
				stream: testStream1,
				id:     "1-*",
			},
			want:    "2",
			wantErr: false,
		},
		{
			name: "Test autoGenSeqNum 3",
			args: args{
				stream: testStream2,
				id:     "0-*",
			},
			want:    "3",
			wantErr: false,
		},
		{
			name: "Test autoGenSeqNum 4",
			args: args{
				stream: testStream2,
				id:     "1-*",
			},
			want:    "0",
			wantErr: false,
		},
		{
			name: "Test autoGenSeqNum 5",
			args: args{
				stream: testStream2,
				id:     "66-*",
			},
			want:    "0",
			wantErr: false,
		},
		{
			name: "Test autoGenSeqNum 6",
			args: args{
				stream: testStream2,
				id:     "xxxxx-*",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := autoGenSeqNum(tt.args.stream, tt.args.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("autoGenSeqNum() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("autoGenSeqNum() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_autoGenID(t *testing.T) {
	type args struct {
		stream *Stream
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Test autoGenID 1",
			args: args{
				stream: testStream1,
			},
			want:    fmt.Sprintf("%d-%d", time.Now().UnixMilli(), 0),
			wantErr: false,
		},
		{
			name: "Test autoGenID 2",
			args: args{
				stream: corruptedStream1,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := autoGenID(tt.args.stream)
			if (err != nil) != tt.wantErr {
				t.Errorf("autoGenID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("autoGenID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getTimeAndSeq(t *testing.T) {
	type args struct {
		id string
	}
	tests := []struct {
		name      string
		args      args
		wantMilli int
		wantSeq   int
		wantErr   bool
	}{
		{
			name: "Test getTimeAndSeq 1",
			args: args{
				id: "5-7",
			},
			wantMilli: 5,
			wantSeq:   7,
			wantErr:   false,
		},
		{
			name: "Test getTimeAndSeq 2",
			args: args{
				id: "a-b",
			},
			wantMilli: -1,
			wantSeq:   -1,
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := getTimeAndSeq(tt.args.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("getTimeAndSeq() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantMilli {
				t.Errorf("getTimeAndSeq() got = %v, want %v", got, tt.wantMilli)
			}
			if got1 != tt.wantSeq {
				t.Errorf("getTimeAndSeq() got1 = %v, want %v", got1, tt.wantSeq)
			}
		})
	}
}
