package helper

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"

	"github.com/hdt3213/rdb/bytefmt"
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// DBStat stores statistics for a single database
type DBStat struct {
	db        int
	totalSize uint64
	keyCount  uint64
}

// GetSize returns the total size for sorting
func (s DBStat) GetSize() int {
	return int(s.totalSize)
}

// DBStatistics analyzes RDB file and generates per-database statistics
// Output columns: database, total_size, size_readable, key_count, avg_key_size, avg_size_readable
func DBStatistics(rdbFilename string, output *os.File, options ...interface{}) error {
	if rdbFilename == "" {
		return errors.New("src file path is required")
	}

	// Open RDB file
	rdbFile, err := os.Open(rdbFilename)
	if err != nil {
		return fmt.Errorf("open rdb %s failed, %v", rdbFilename, err)
	}
	defer func() {
		_ = rdbFile.Close()
	}()

	// Create decoder with options
	var dec decoder = core.NewDecoder(rdbFile)
	if dec, err = wrapDecoder(dec, options...); err != nil {
		return err
	}

	// Collect statistics per database
	dbStats := make(map[int]*DBStat)
	err = dec.Parse(func(object model.RedisObject) bool {
		db := object.GetDBIndex()
		if stat, ok := dbStats[db]; ok {
			stat.totalSize += uint64(object.GetSize())
			stat.keyCount++
		} else {
			dbStats[db] = &DBStat{
				db:        db,
				totalSize: uint64(object.GetSize()),
				keyCount:  1,
			}
		}
		return true
	})
	if err != nil {
		return err
	}

	// Sort databases by index
	dbIndices := make([]int, 0, len(dbStats))
	for db := range dbStats {
		dbIndices = append(dbIndices, db)
	}
	sort.Ints(dbIndices)

	// Write CSV header
	_, err = output.WriteString("database,total_size,size_readable,key_count,avg_key_size,avg_size_readable\n")
	if err != nil {
		return fmt.Errorf("write header failed: %v", err)
	}

	// Write statistics
	csvWriter := csv.NewWriter(output)
	defer csvWriter.Flush()

	for _, db := range dbIndices {
		stat := dbStats[db]
		avgSize := uint64(0)
		if stat.keyCount > 0 {
			avgSize = stat.totalSize / stat.keyCount
		}

		err := csvWriter.Write([]string{
			strconv.Itoa(stat.db),
			strconv.FormatUint(stat.totalSize, 10),
			bytefmt.FormatSize(stat.totalSize),
			strconv.FormatUint(stat.keyCount, 10),
			strconv.FormatUint(avgSize, 10),
			bytefmt.FormatSize(avgSize),
		})
		if err != nil {
			return fmt.Errorf("write csv failed: %v", err)
		}
	}

	return nil
}
