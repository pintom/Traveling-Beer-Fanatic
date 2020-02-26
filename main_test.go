package main

import "testing"

func Test_hello(t *testing.T) {
	tests := []struct {
		name string
		args string
	}{
		{
			"World",
			"Hello World!",
		},
		{
			"Satalia",
			"Hello Satalia!",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
		})
	}
}
