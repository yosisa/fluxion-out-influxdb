package main

import (
	"encoding/binary"
	"strings"

	influxdb "github.com/influxdb/influxdb/client"
	"github.com/yosisa/fluxion/buffer"
	"github.com/yosisa/fluxion/message"
	"github.com/yosisa/fluxion/plugin"
)

type Series influxdb.Series

func (s *Series) Size() int64 {
	return int64(binary.Size(s))
}

type Config struct {
	Server   string `toml:"server"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	Database string `toml:"database"`
	UseUDP   bool   `toml:"use_udp"`
	StripTag int    `toml:"strip_tag"`
}

type InfluxdbOutput struct {
	env    *plugin.Env
	conf   *Config
	client *influxdb.Client
}

func (p *InfluxdbOutput) Init(env *plugin.Env) (err error) {
	p.env = env
	p.conf = &Config{}
	if err = env.ReadConfig(p.conf); err != nil {
		return
	}
	return
}

func (p *InfluxdbOutput) Start() (err error) {
	p.client, err = influxdb.New(&influxdb.ClientConfig{
		Host:     p.conf.Server,
		Username: p.conf.User,
		Password: p.conf.Password,
		Database: p.conf.Database,
		IsUDP:    p.conf.UseUDP,
	})
	return
}

func (p *InfluxdbOutput) Encode(ev *message.Event) (buffer.Sizer, error) {
	tag := ev.Tag
	if p.conf.StripTag > 0 {
		parts := strings.SplitN(tag, ".", p.conf.StripTag+1)
		tag = parts[len(parts)-1]
	}

	s := &Series{
		Name:    tag,
		Columns: []string{"time"},
	}
	point := []interface{}{int64(float64(ev.Time.UnixNano()) * 1e-6)}
	for k, v := range ev.Record {
		s.Columns = append(s.Columns, k)
		point = append(point, v)
	}
	s.Points = append(s.Points, point)
	return s, nil
}

func (p *InfluxdbOutput) Write(l []buffer.Sizer) (int, error) {
	series := make([]*influxdb.Series, len(l))
	for i, v := range l {
		series[i] = (*influxdb.Series)(v.(*Series))
	}

	var err error
	if p.conf.UseUDP {
		err = p.client.WriteSeriesOverUDP(series)
	} else {
		err = p.client.WriteSeries(series)
	}
	if err != nil {
		return 0, err
	}
	return len(l), nil
}

func (p *InfluxdbOutput) Close() error {
	return nil
}

func main() {
	plugin.New("out-influxdb", func() plugin.Plugin {
		return &InfluxdbOutput{}
	}).Run()
}
