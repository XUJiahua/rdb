package helper

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/hdt3213/rdb/bytefmt"
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

func getLastElement(redisKey string) string {
	parts := strings.Split(redisKey, ":")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

type FilterAction func(object model.RedisObject)

// Filter read rdb file and filter the keys that match the condition.
// The invoker owns output, Filter won't close it
func Filter(rdbFilename string, filterDate string, action string, output *os.File, options ...interface{}) error {
	var filterAction FilterAction
	var keyCount int
	var totalSize int
	switch action {
	case "sum":
		filterAction = func(object model.RedisObject) {
			keyCount++
			totalSize += object.GetSize()
		}
	default:
		filterAction = func(object model.RedisObject) {
			output.WriteString(object.GetKey() + "\n")
		}
	}

	filterTime, err := parseDate(filterDate)
	if err != nil {
		return fmt.Errorf("parse expired date failed: %v", err)
	}

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

	err = dec.Parse(func(object model.RedisObject) bool {
		key := object.GetKey()
		timeStr := getLastElement(key)
		if timeStr == "" {
			return true
		}
		t, err := parseDate(timeStr)
		if err != nil {
			return true
		}
		if !t.Before(filterTime) {
			return true
		}

		filterAction(object)

		return true
	})
	if err != nil {
		return err
	}

	if action == "sum" {
		fmt.Printf("key count: %d, total size: %d(%s)\n", keyCount, totalSize, bytefmt.FormatSize(uint64(totalSize)))
	}

	return nil
}
