package protocol

import (
	"fmt"
	"testing"
)

func TestGetArrayLength(t *testing.T) {
	tests := []struct {
		name    string
		str     string
		want    int
		wantErr bool
	}{
		{name: "Test GetArrayLength 1", str: "*3", want: 3, wantErr: false},
		{name: "Test GetArrayLength 2", str: "*11", want: 11, wantErr: false},
		{name: "Test GetArrayLength 3", str: "5*", want: 0, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.str, func(t *testing.T) {
			got, err := GetArrayLength(tt.str)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetArrayLength() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetArrayLength() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetBulkStringLength(t *testing.T) {
	tests := []struct {
		name    string
		str     string
		want    int
		wantErr bool
	}{
		{name: "Test GetBulkStringLength 1", str: "$3", want: 3, wantErr: false},
		{name: "Test GetBulkStringLength 2", str: "$11", want: 11, wantErr: false},
		{name: "Test GetBulkStringLength 3", str: "5$", want: 0, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.str, func(t *testing.T) {
			got, err := GetBulkStringLength(tt.str)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetBulkStringLength() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetBulkStringLength() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVerifyBulkStringLength(t *testing.T) {
	tests := []struct {
		name    string
		str     string
		len     int
		want    error
		wantErr bool
	}{
		{name: "Test VerifyBulkStringLength 1", str: "abc", len: 3, want: nil, wantErr: false},
		{name: "Test VerifyBulkStringLength 2", str: "asdfjklqwer", len: 11, want: nil, wantErr: false},
		{name: "Test VerifyBulkStringLength 3", str: "", len: 1, want: fmt.Errorf("Expected data length: %d Actual data length: %d", 1, len("")), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := VerifyBulkStringLength(tt.str, tt.len); (err != nil) != tt.wantErr {
				t.Errorf("VerifyBulkStringLength() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestToRespArray(t *testing.T) {
	type args struct {
		arr []string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test ToRespArray 1",
			args: args{
				arr: []string{"abc", "defg"},
			},
			want: "*2\r\n$3\r\nabc\r\n$4\r\ndefg\r\n",
		},
		{
			name: "Test ToRespArray 2",
			args: args{
				arr: []string{},
			},
			want: "*0\r\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ToRespArray(tt.args.arr); got != tt.want {
				t.Errorf("ToRespArray() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestToBulkString(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test ToBulkString 1",
			args: args{
				s: "abc",
			},
			want: "$3\r\nabc\r\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ToBulkString(tt.args.s); got != tt.want {
				t.Errorf("ToBulkString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestToSimpleError(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test ToSimpleError 1",
			args: args{
				s: "error message",
			},
			want: "-error message\r\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ToSimpleError(tt.args.s); got != tt.want {
				t.Errorf("ToSimpleError() = %v, want %v", got, tt.want)
			}
		})
	}
}
