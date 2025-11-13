package helper

import (
	"fmt"
	"github.com/hdt3213/rdb/model"
	"regexp"
	"time"
)

type decoder interface {
	Parse(cb func(object model.RedisObject) bool) error
}

type regexDecoder struct {
	reg *regexp.Regexp
	dec decoder
}

func (d *regexDecoder) Parse(cb func(object model.RedisObject) bool) error {
	return d.dec.Parse(func(object model.RedisObject) bool {
		if d.reg.MatchString(object.GetKey()) {
			return cb(object)
		}
		return true
	})
}

// regexWrapper returns
func regexWrapper(d decoder, expr string) (*regexDecoder, error) {
	reg, err := regexp.Compile(expr)
	if err != nil {
		return nil, fmt.Errorf("illegal regex expression: %v", expr)
	}
	return &regexDecoder{
		dec: d,
		reg: reg,
	}, nil
}

// RegexOption enable regex filters
type RegexOption *string

// WithRegexOption creates a WithRegexOption from regex expression
func WithRegexOption(expr string) RegexOption {
	return &expr
}

// excludeRegexDecoder excludes keys matching the regex pattern
type excludeRegexDecoder struct {
	reg *regexp.Regexp
	dec decoder
}

func (d *excludeRegexDecoder) Parse(cb func(object model.RedisObject) bool) error {
	return d.dec.Parse(func(object model.RedisObject) bool {
		if !d.reg.MatchString(object.GetKey()) {
			return cb(object)
		}
		return true
	})
}

// excludeRegexWrapper returns a decoder that excludes keys matching the pattern
func excludeRegexWrapper(d decoder, expr string) (*excludeRegexDecoder, error) {
	reg, err := regexp.Compile(expr)
	if err != nil {
		return nil, fmt.Errorf("illegal exclude regex expression: %v", expr)
	}
	return &excludeRegexDecoder{
		dec: d,
		reg: reg,
	}, nil
}

// ExcludeRegexOption enable exclude regex filters
type ExcludeRegexOption *string

// WithExcludeRegexOption creates an ExcludeRegexOption from regex expression
func WithExcludeRegexOption(expr string) ExcludeRegexOption {
	return &expr
}

// noExpiredDecoder filter all expired keys
type noExpiredDecoder struct {
	dec decoder
}

func (d *noExpiredDecoder) Parse(cb func(object model.RedisObject) bool) error {
	now := time.Now()
	return d.dec.Parse(func(object model.RedisObject) bool {
		expiration := object.GetExpiration()
		if expiration == nil || expiration.After(now) {
			return cb(object)
		}
		return true
	})
}

// NoExpiredOption tells decoder to filter all expired keys
type NoExpiredOption bool

// WithNoExpiredOption tells decoder to filter all expired keys
func WithNoExpiredOption() NoExpiredOption {
	return NoExpiredOption(true)
}

func wrapDecoder(dec decoder, options ...interface{}) (decoder, error) {
	var regexOpt RegexOption
	var excludeRegexOpt ExcludeRegexOption
	var noExpiredOpt NoExpiredOption
	for _, opt := range options {
		switch o := opt.(type) {
		case RegexOption:
			regexOpt = o
		case ExcludeRegexOption:
			excludeRegexOpt = o
		case NoExpiredOption:
			noExpiredOpt = o
		}
	}
	if regexOpt != nil {
		var err error
		dec, err = regexWrapper(dec, *regexOpt)
		if err != nil {
			return nil, err
		}
	}
	if excludeRegexOpt != nil {
		var err error
		dec, err = excludeRegexWrapper(dec, *excludeRegexOpt)
		if err != nil {
			return nil, err
		}
	}
	if noExpiredOpt {
		dec = &noExpiredDecoder{
			dec: dec,
		}
	}
	return dec, nil
}
