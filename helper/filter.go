package helper

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// clean data by days
// 202506 month, return the last day of the month
// 20250612 day
// 2025061215 hour
// 202506121504 minute
// 20250612150405 second
func parseDate(date string) (time.Time, error) {
	if len(date) < 6 || len(date) > 14 {
		return time.Time{}, errors.New("invalid date format")
	}

	if len(date) == 6 {
		layout := "200601"
		t, err := time.ParseInLocation(layout, date, time.UTC)
		if err != nil {
			return time.Time{}, err
		}
		return t.AddDate(0, 1, -1), nil
	}

	if len(date) < 8 {
		return time.Time{}, errors.New("invalid date format")
	}
	date = date[:8]
	layout := "20060102"
	return time.ParseInLocation(layout, date, time.UTC)
}

// Filter read rdb file and filter the keys that match the condition.
// The invoker owns output, Filter won't close it
func Filter(rdbFilename string, expiredDate string, output *os.File, options ...interface{}) error {
	if rdbFilename == "" {
		return errors.New("src file path is required")
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
		prefix := getPrefixOfKey(object.GetKey(), 10)
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

	// write into csv
	_, err = output.WriteString("database,prefix,size,size_readable,key_count\n")
	if err != nil {
		return fmt.Errorf("write header failed: %v", err)
	}
	csvWriter := csv.NewWriter(output)
	defer csvWriter.Flush()
	// printNode := func(node TmpNode) error {
	// 	dbStr := strconv.Itoa(node.db)
	// 	return csvWriter.Write([]string{
	// 		dbStr,
	// 		node.keyPrefix,
	// 		strconv.Itoa(node.totalSize),
	// 		bytefmt.FormatSize(uint64(node.totalSize)),
	// 		strconv.Itoa(node.keyCount),
	// 	})
	// }
	// for _, n := range toplist.list {
	// 	node := n.(TmpNode)
	// 	err := printNode(node)
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	return nil
}
