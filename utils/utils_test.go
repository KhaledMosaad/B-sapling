package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_getPageOffset(t *testing.T) {
	tests := []struct {
		name    string
		pnumber uint32
		psize   uint64
		want    uint64
	}{
		{"it return offset correctly", 1, 4096, 4096},
		{"it return offset correctly", 2, 4096, 2 * 4096},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, GetPageOffset(test.pnumber, test.psize), test.want)
		})
	}
}
