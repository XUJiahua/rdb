package helper

import (
	"encoding/csv"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/hdt3213/rdb/bytefmt"
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// getFieldFromKey extracts specified fields from a key using a separator.
// fieldIndexes are 1-based (first field is 1, not 0).
// Returns empty string if any specified field doesn't exist.
func getFieldFromKey(key string, fieldIndexes []int, separator string) string {
	fields := strings.Split(key, separator)

	var parts []string
	for _, idx := range fieldIndexes {
		// Convert 1-based index to 0-based
		if idx > 0 && idx <= len(fields) {
			parts = append(parts, fields[idx-1])
		} else {
			// Field doesn't exist, return empty string
			return ""
		}
	}

	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, separator)
}

// ParseFieldIndexes parses field index string like "1" or "1,2,3" into []int
func ParseFieldIndexes(fieldStr string) ([]int, error) {
	if fieldStr == "" {
		return nil, errors.New("field indexes cannot be empty")
	}

	parts := strings.Split(fieldStr, ",")
	indexes := make([]int, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		idx, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid field index '%s': %v", part, err)
		}
		if idx <= 0 {
			return nil, fmt.Errorf("field index must be positive, got %d", idx)
		}
		indexes = append(indexes, idx)
	}

	return indexes, nil
}

// FieldGroupAnalyse groups keys by specified field(s) and generates statistics.
// fieldIndexes: which fields to group by (1-based, e.g., []int{1} for first field)
// separator: field delimiter (e.g., ":")
// topN: number of top groups to output (0 means all)
// output: output file for CSV results
func FieldGroupAnalyse(rdbFilename string, fieldIndexes []int, separator string, topN int, output *os.File, options ...interface{}) error {
	if rdbFilename == "" {
		return errors.New("src file path is required")
	}
	if len(fieldIndexes) == 0 {
		return errors.New("at least one field index is required")
	}
	if separator == "" {
		return errors.New("separator cannot be empty")
	}
	if topN < 0 {
		return errors.New("n must greater than 0")
	} else if topN == 0 {
		topN = math.MaxInt
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

	// key = db index + field value
	// value = count, size
	cache := make(map[string]TmpNode)
	skippedCount := 0 // count keys that don't have the specified fields

	err = dec.Parse(func(object model.RedisObject) bool {
		fieldValue := getFieldFromKey(object.GetKey(), fieldIndexes, separator)
		if fieldValue == "" {
			// Skip keys that don't have the specified fields
			skippedCount++
			return true
		}

		key := genKey(object.GetDBIndex(), fieldValue)
		if data, ok := cache[key]; ok {
			data.keyCount += 1
			data.totalSize += object.GetSize()
			cache[key] = data
		} else {
			cache[key] = TmpNode{
				db:        object.GetDBIndex(),
				keyPrefix: fieldValue,
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
	_, err = output.WriteString("database,field_value,size,size_readable,key_count\n")
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

	if skippedCount > 0 {
		fmt.Fprintf(os.Stderr, "Warning: skipped %d keys that don't have the specified fields\n", skippedCount)
	}

	return nil
}
