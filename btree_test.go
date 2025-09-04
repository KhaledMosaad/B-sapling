// Portions of this code are derived from the cockroachdb/pebble project:
// https://github.com/cockroachdb/pebble
// Licensed under the BSD 3-Clause License.

package sapling

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	_ "github.com/KhaledMosaad/B-sapling/logger"
	"github.com/KhaledMosaad/B-sapling/storage"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

// These variable should not be used directly, only via hamletWordCount().
var KVData struct {
	once sync.Once
	data []storage.Pair
}

// hamletWordCount returns the data in testdata.h/txt, as a array of pair
func GenerateKVData() []storage.Pair {
	KVData.data = make([]storage.Pair, 0, 200000)
	KVData.once.Do(func() {
		f, err := os.Open("./testdata/test1.txt")
		if err != nil {
			panic(err)
		}
		defer f.Close()
		r := bufio.NewReader(f)

		for {
			s, err := r.ReadBytes('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}

			trimmed := strings.TrimSpace(string(s))
			kv := strings.Split(trimmed, " ")
			pair := storage.Pair{Key: []byte(kv[0]), Value: []byte(kv[2])}
			KVData.data = append(KVData.data, pair)
		}
	})
	return KVData.data
}

func BenchmarkUpsert(t *testing.B) {
	os.Remove("./local/tests/upserts/upsert_benchmark.db")
	b, _ := Open("./local/tests/upserts/upsert_benchmark.db")
	defer b.Close()

	// the usage of this data is the last test
	testData := GenerateKVData()

	t.Run("It upsert a lot of key value from testdata/h.txt", func(t *testing.B) {
		for i, pair := range testData {
			log.Trace().Any("Pair: ", pair).Msg("Insert new kv")
			success, _, err := b.Upsert(pair.Key, pair.Value)
			if suc := assert.Truef(t, success, fmt.Sprintf("Expecting success but it failed %# v at index %v", pair, i)); !suc {
				t.FailNow()
			}

			if suc := assert.Nilf(t, err, fmt.Sprintf("Expecting error is not nil %# v at index %v error with: %# v", pair, i, err)); !suc {
				t.FailNow()
			}
		}
	})
}
