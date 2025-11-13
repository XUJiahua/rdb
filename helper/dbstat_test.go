package helper

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDBStatistics(t *testing.T) {
	err := os.MkdirAll("tmp", os.ModePerm)
	if err != nil {
		t.Errorf("create tmp directory failed: %v", err)
		return
	}
	defer func() {
		err := os.RemoveAll("tmp")
		if err != nil {
			t.Logf("remove tmp directory failed: %v", err)
		}
	}()

	// Test with memory.rdb which contains multiple keys
	srcRdb := filepath.Join("../cases", "memory.rdb")
	outputPath := filepath.Join("tmp", "dbstat.csv")

	outputFile, err := os.Create(outputPath)
	if err != nil {
		t.Errorf("create output file failed: %v", err)
		return
	}

	err = DBStatistics(srcRdb, outputFile)
	if err != nil {
		t.Errorf("DBStatistics failed: %v", err)
		return
	}

	// Close file before reading
	_ = outputFile.Close()

	// Verify output file was created and has content
	stat, err := os.Stat(outputPath)
	if err != nil {
		t.Errorf("output file not found: %v", err)
		return
	}

	if stat.Size() == 0 {
		t.Error("output file is empty")
		return
	}

	// Read and verify CSV structure
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Errorf("read output file failed: %v", err)
		return
	}

	contentStr := string(content)
	// Verify header exists
	if len(contentStr) < 10 {
		t.Errorf("output file too small: %s", contentStr)
		return
	}

	// Check that header contains expected columns
	expectedHeader := "database,total_size,size_readable,key_count,avg_key_size,avg_size_readable"
	if contentStr[:len(expectedHeader)] != expectedHeader {
		t.Errorf("unexpected header, got: %s", contentStr[:len(expectedHeader)])
		return
	}

	t.Logf("DBStatistics output:\n%s", contentStr)
}

func TestDBStatisticsWithOptions(t *testing.T) {
	err := os.MkdirAll("tmp", os.ModePerm)
	if err != nil {
		t.Errorf("create tmp directory failed: %v", err)
		return
	}
	defer func() {
		err := os.RemoveAll("tmp")
		if err != nil {
			t.Logf("remove tmp directory failed: %v", err)
		}
	}()

	srcRdb := filepath.Join("../cases", "memory.rdb")
	outputPath := filepath.Join("tmp", "dbstat_filtered.csv")

	outputFile, err := os.Create(outputPath)
	if err != nil {
		t.Errorf("create output file failed: %v", err)
		return
	}

	// Test with no-expired option
	err = DBStatistics(srcRdb, outputFile, WithNoExpiredOption())
	if err != nil {
		t.Errorf("DBStatistics with options failed: %v", err)
		return
	}

	_ = outputFile.Close()

	// Verify output exists
	stat, err := os.Stat(outputPath)
	if err != nil {
		t.Errorf("output file not found: %v", err)
		return
	}

	if stat.Size() == 0 {
		t.Error("output file is empty")
		return
	}

	t.Logf("DBStatistics with options completed successfully")
}

func TestDBStatisticsEmptyDB(t *testing.T) {
	err := os.MkdirAll("tmp", os.ModePerm)
	if err != nil {
		t.Errorf("create tmp directory failed: %v", err)
		return
	}
	defer func() {
		err := os.RemoveAll("tmp")
		if err != nil {
			t.Logf("remove tmp directory failed: %v", err)
		}
	}()

	srcRdb := filepath.Join("../cases", "empty_database.rdb")
	outputPath := filepath.Join("tmp", "dbstat_empty.csv")

	outputFile, err := os.Create(outputPath)
	if err != nil {
		t.Errorf("create output file failed: %v", err)
		return
	}

	err = DBStatistics(srcRdb, outputFile)
	if err != nil {
		t.Errorf("DBStatistics failed: %v", err)
		return
	}

	_ = outputFile.Close()

	// Verify output exists (should have header only)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Errorf("read output file failed: %v", err)
		return
	}

	t.Logf("Empty DB output:\n%s", string(content))
}
