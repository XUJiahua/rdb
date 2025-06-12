package helper

import (
	"reflect"
	"testing"
	"time"
)

func Test_parseDate(t *testing.T) {
	type args struct {
		date string
	}
	tests := []struct {
		name    string
		args    args
		want    time.Time
		wantErr bool
	}{
		{
			name: "month",
			args: args{date: "202506"},
			want: time.Date(2025, 6, 30, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "day",
			args: args{date: "20250612"},
			want: time.Date(2025, 6, 12, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "hour",
			args: args{date: "2025061215"},
			want: time.Date(2025, 6, 12, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "minute",
			args: args{date: "202506121504"},
			want: time.Date(2025, 6, 12, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "second",
			args: args{date: "20250612150405"},
			want: time.Date(2025, 6, 12, 0, 0, 0, 0, time.UTC),
		},
		{
			name:    "invalid",
			args:    args{date: "2025061"},
			wantErr: true,
		},
		{
			name:    "invalid2",
			args:    args{date: "2025061215040506"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseDate(tt.args.date)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseDate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseDate() got = %v, want %v", got, tt.want)
			}
		})
	}
}
