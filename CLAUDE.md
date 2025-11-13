# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Go-based Redis RDB (Redis Database) file parser for secondary development and memory analysis. It supports RDB versions 1-12 (Redis 7.2) and provides capabilities for:
- Parsing and decoding RDB files into structured data
- Converting RDB to JSON, AOF (Redis Serialization Protocol), or CSV
- Memory profiling and analysis
- Finding biggest keys and analyzing by prefix patterns
- Generating flame graphs for memory usage visualization
- Encoding/generating new RDB files

## Commands

### Building
```bash
# Build for current platform
go build -o rdb ./

# Cross-compile for multiple platforms
./build.sh

# Install globally
go install github.com/hdt3213/rdb@latest
```

### Testing
```bash
# Run all tests
go test ./...

# Test specific package
go test ./core
go test ./helper
go test ./memprofiler

# Run specific test
go test -run TestName ./package
```

### Running the CLI
The main entry point is `cmd.go` which provides various commands:

```bash
# Convert RDB to JSON
rdb -c json -o output.json dump.rdb

# Generate memory profile CSV
rdb -c memory -o memory.csv dump.rdb

# Convert to AOF
rdb -c aof -o dump.aof dump.rdb

# Find biggest N keys
rdb -c bigkey -n 10 dump.rdb

# Analyze by prefix (radix tree-based, hierarchical by delimiter)
rdb -c prefix -n 10 -max-depth 3 -o prefix-report.csv dump.rdb

# Analyze by prefix v2 (character-based, simple string prefix)
rdb -c prefixv2 -n 20 -max-depth 10 -o prefix-report.csv dump.rdb

# Generate database statistics (per-DB storage summary)
rdb -c dbstat -o db-stats.csv dump.rdb

# Generate flamegraph (starts web server)
rdb -c flamegraph -port 16379 -sep : dump.rdb

# Filter with regex
rdb -c json -o output.json -regex '^user:.*' dump.rdb

# Filter expired keys
rdb -c json -o output.json -no-expired dump.rdb
```

## Architecture

### Package Structure

**core/** - Low-level RDB encoding/decoding
- `decoder.go`: Core RDB file parser, handles opcodes and data structures
- `encoder.go`: RDB file generation
- Type-specific decoders: `string.go`, `list.go`, `hash.go`, `set.go`, `zset.go`, `stream.go`
- Compression: `ziplist.go`, `listpack.go`
- `utils.go`: Binary reading utilities

**parser/** - Public API wrapper around core decoder
- `portal.go`: Exports types and NewDecoder for external use

**encoder/** - Public API wrapper around core encoder
- `portal.go`: Exports Encoder and related functions

**model/** - Data structures for Redis objects
- `model.go`: Defines RedisObject interface and concrete types (StringObject, ListObject, HashObject, SetObject, ZSetObject, StreamObject)
- `stream.go`: Stream-specific data structures
- `detail.go`: Additional metadata structures

**helper/** - High-level operations and CLI command implementations
- `converter.go`: RDB to JSON conversion
- `memory.go`: Memory profiling to CSV
- `resp.go`: RDB to AOF/RESP conversion
- `bigkey.go`: Find largest keys
- `prefix.go`: Hierarchical prefix analysis using radix tree with delimiter-based grouping
- `prefixv2.go`: Simple character-based prefix analysis (groups by first N characters)
- `dbstat.go`: Per-database storage statistics (total size, key count, average key size)
- `radix.go`: Radix tree implementation for prefix analysis
- `flamegraph.go`: Generate flame graph data
- `filter.go`: Key filtering by date/regex
- `regex.go`: Regex filtering wrapper

**memprofiler/** - Memory usage estimation
- `memprofiler.go`: Main entry point for calculating Redis object memory
- Type-specific calculators: `hash.go`, `list.go`, `zset.go`, `stream.go`
- Estimates based on Redis internal data structure overhead

**d3flame/** - Flame graph web visualization
- `web.go`: HTTP server for interactive flame graphs
- `template.go`: HTML/JS templates for d3.js visualization

**lzf/** - LZF compression/decompression used by Redis

**bytefmt/** - Human-readable byte formatting (e.g., "2K", "1.1G")

**examples/** - Usage examples
- `decode/main.go`: Example of parsing RDB files
- `encode/main.go`: Example of generating RDB files

### Key Architectural Patterns

1. **Decoder Pattern**: `core.Decoder` reads RDB file sequentially, invoking callbacks for each Redis object parsed. Use `decoder.Parse(callback)` where callback receives `model.RedisObject`.

2. **Type System**: All Redis objects implement `model.RedisObject` interface. Type-assert to concrete types (`*model.StringObject`, `*model.HashObject`, etc.) to access specific fields.

3. **Memory Profiling**: `memprofiler` package estimates memory by calculating overhead for each data structure encoding (e.g., ziplist vs hash table) based on Redis internals.

4. **Prefix Analysis**: Two approaches available:
   - `prefix` command: Uses radix tree (`helper/radix.go`) to aggregate keys by hierarchical prefixes separated by delimiters (default `:`). The `max-depth` parameter controls delimiter depth levels (e.g., depth=2 for `User:123:`).
   - `prefixv2` command: Simple character-based grouping that truncates keys to first N characters. The `max-depth` parameter specifies character count (e.g., max-depth=10 groups `User:12345:abc` as `User:1234`).

5. **Options Pattern**: Helper functions accept `options ...interface{}` for features like regex filtering (`WithRegexOption`) and excluding expired keys (`WithNoExpiredOption`). These wrap the decoder with filtering logic.

## Development Notes

- The project uses `github.com/bytedance/sonic` for fast JSON encoding
- RDB format opcodes are defined in `core/decoder.go` (e.g., `opCodeSelectDB`, `opCodeExpireTimeMs`)
- Type codes map Redis internal encodings to string identifiers in `model/model.go`
- Test files use RDB samples from the `cases/` directory
- The main branch is `master`; current development branch is `prefixv2`
- GoReleaser config (`.goreleaser.yml`) handles multi-platform binary releases
