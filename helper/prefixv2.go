package helper

import (
	"encoding/csv"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"

	"github.com/hdt3213/rdb/bytefmt"
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

type TmpNode struct {
	db        int
	keyPrefix string
	keyCount  int
	totalSize int
}

func (n TmpNode) GetSize() int {
	return n.totalSize
}

func getPrefixOfKey(key string, num int) string {
	if len(key) < num {
		return key
	}
	return key[:num]
}

// PrefixV2Analyse read rdb file and find the largest N keys.
// The invoker owns output, FindBiggestKeys won't close it
func PrefixV2Analyse(rdbFilename string, topN int, maxDepth int, output *os.File, options ...interface{}) error {
	if rdbFilename == "" {
		return errors.New("src file path is required")
	}
	if topN < 0 {
		return errors.New("n must greater than 0")
	} else if topN == 0 {
		topN = math.MaxInt
	}
	if maxDepth == 0 {
		maxDepth = math.MaxInt
	}

	// decode rdb file
	rdbFile, err := os.Open(rdbFilename)
	if err != nil {
		return fmt.Errorf("open rdb %s failed, %v", rdbFilename, err)
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	var dec decoder = core.NewDecoder(rdbFile)
	if dec, err = wrapDecoder(dec, options...); err != nil {
		return err
	}

	// key = db index + keyPrefix
	// value = count, size
	cache := make(map[string]TmpNode)
	err = dec.Parse(func(object model.RedisObject) bool {
		prefix := getPrefixOfKey(object.GetKey(), maxDepth)
		key := genKey(object.GetDBIndex(), prefix)
		if data, ok := cache[key]; ok {
			data.keyCount += 1
			data.totalSize += object.GetSize()
			cache[key] = data
		} else {
			cache[key] = TmpNode{
				db:        object.GetDBIndex(),
				keyPrefix: prefix,
				keyCount:  1,
				totalSize: object.GetSize(),
			}
		}
		return true
	})
	if err != nil {
		return err
	}

	// get top list
	toplist := newToplist(topN)
	for _, node := range cache {
		toplist.add(node)
	}

	// write into csv
	_, err = output.WriteString("database,prefix,size,size_readable,key_count\n")
	if err != nil {
		return fmt.Errorf("write header failed: %v", err)
	}
	csvWriter := csv.NewWriter(output)
	defer csvWriter.Flush()
	printNode := func(node TmpNode) error {
		dbStr := strconv.Itoa(node.db)
		return csvWriter.Write([]string{
			dbStr,
			node.keyPrefix,
			strconv.Itoa(node.totalSize),
			bytefmt.FormatSize(uint64(node.totalSize)),
			strconv.Itoa(node.keyCount),
		})
	}
	for _, n := range toplist.list {
		node := n.(TmpNode)
		err := printNode(node)
		if err != nil {
			return err
		}
	}

	return nil
}
