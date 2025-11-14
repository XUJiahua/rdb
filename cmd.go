package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/hdt3213/rdb/helper"
)

const help = `
This is a tool to parse Redis' RDB files
Options:
  -c command, including: json/memory/aof/bigkey/prefix/prefixv2/fieldgroup/dbstat/flamegraph
  -o output file path
  -n number of result, using in command: bigkey/prefix/fieldgroup
  -port listen port for flame graph web service
  -sep separator for flamegraph, rdb will separate key by it, default value is ":".
		supporting multi separators: -sep sep1 -sep sep2
		for fieldgroup: field separator, default is ":"
  -field field indexes for grouping (1-based), e.g., "1" or "1,2,3"
  -regex using regex expression filter keys
  -exclude-regex using regex expression to exclude keys
  -no-expired filter expired keys

Examples:
parameters between '[' and ']' is optional
1. convert rdb to json
  rdb -c json -o dump.json dump.rdb
2. generate memory report
  rdb -c memory -o memory.csv dump.rdb
3. convert to aof file
  rdb -c aof -o dump.aof dump.rdb
4. get largest keys
  rdb -c bigkey [-o dump.aof] [-n 10] dump.rdb
5. get number and memory size by prefix
  rdb -c prefix [-n 10] [-max-depth 3] [-o prefix-report.csv] dump.rdb
6. draw flamegraph
  rdb -c flamegraph [-port 16379] [-sep :] dump.rdb
7. generate database statistics
  rdb -c dbstat [-o db-stats.csv] dump.rdb
8. group by field (for keys like "f1:i00018:xxx:90221900:2024111214")
  rdb -c fieldgroup -field 1 [-sep :] [-n 20] [-o field-report.csv] dump.rdb
  rdb -c fieldgroup -field 1,2 [-sep :] [-n 20] [-o field-report.csv] dump.rdb
`

type separators []string

func (s *separators) String() string {
	return strings.Join(*s, " ")
}

func (s *separators) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	var cmd string
	var output string
	var n int
	var port int
	var seps separators
	var regexExpr string
	var excludeRegexExpr string
	var noExpired bool
	var maxDepth int
	var filterDate string
	var action string
	var fieldIndexes string
	var err error
	flagSet.StringVar(&cmd, "c", "", "command for rdb: json")
	flagSet.StringVar(&output, "o", "", "output file path")
	flagSet.IntVar(&n, "n", 0, "")
	flagSet.IntVar(&maxDepth, "max-depth", 0, "max depth of prefix tree")
	flagSet.IntVar(&port, "port", 0, "listen port for web")
	flagSet.Var(&seps, "sep", "separator for flame graph")
	flagSet.StringVar(&regexExpr, "regex", "", "regex expression")
	flagSet.StringVar(&excludeRegexExpr, "exclude-regex", "", "exclude regex expression")
	flagSet.BoolVar(&noExpired, "no-expired", false, "filter expired keys")
	flagSet.StringVar(&filterDate, "filter-date", "", "keep keys before date, format: 20250612")
	flagSet.StringVar(&action, "action", "", "action for filter: print(default), sum")
	flagSet.StringVar(&fieldIndexes, "field", "", "field indexes for grouping, e.g., 1 or 1,2,3")
	_ = flagSet.Parse(os.Args[1:]) // ExitOnError
	src := flagSet.Arg(0)

	if cmd == "" {
		println(help)
		return
	}
	if src == "" {
		println("src file is required")
		return
	}

	var options []interface{}
	if regexExpr != "" {
		options = append(options, helper.WithRegexOption(regexExpr))
	}
	if excludeRegexExpr != "" {
		options = append(options, helper.WithExcludeRegexOption(excludeRegexExpr))
	}
	if noExpired {
		options = append(options, helper.WithNoExpiredOption())
	}

	var outputFile *os.File
	if output == "" {
		outputFile = os.Stdout
	} else {
		outputFile, err = os.Create(output)
		if err != nil {
			fmt.Printf("open output faild: %v", err)
		}
		defer func() {
			_ = outputFile.Close()
		}()
	}

	switch cmd {
	case "json":
		err = helper.ToJsons(src, output, options...)
	case "memory":
		err = helper.MemoryProfile(src, output, options...)
	case "aof":
		err = helper.ToAOF(src, output, options)
	case "bigkey":
		err = helper.FindBiggestKeys(src, n, outputFile, options...)
	case "prefix":
		err = helper.PrefixAnalyse(src, n, maxDepth, outputFile, options...)
	case "prefixv2":
		err = helper.PrefixV2Analyse(src, n, maxDepth, outputFile, options...)
	case "fieldgroup":
		if fieldIndexes == "" {
			fmt.Println("error: -field parameter is required for fieldgroup command")
			return
		}
		indexes, parseErr := helper.ParseFieldIndexes(fieldIndexes)
		if parseErr != nil {
			fmt.Printf("error: %v\n", parseErr)
			return
		}
		separator := ":"
		if len(seps) > 0 {
			separator = seps[0]
		}
		err = helper.FieldGroupAnalyse(src, indexes, separator, n, outputFile, options...)
	case "filter":
		err = helper.Filter(src, filterDate, action, outputFile, options...)
	case "dbstat":
		err = helper.DBStatistics(src, outputFile, options...)
	case "flamegraph":
		_, err = helper.FlameGraph(src, port, seps, options...)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			return
		}
		<-make(chan struct{})
	default:
		println("unknown command")
		return
	}
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}
}
