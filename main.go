package main

import (
	"encoding/binary"
	"strings"

	influxdb "github.com/influxdb/influxdb/client"
	"github.com/yosisa/fluxion/buffer"
	"github.com/yosisa/fluxion/event"
	"github.com/yosisa/fluxion/plugin"
)

type Series influxdb.Series

func (s *Series) Size() int64 {
	return int64(binary.Size(s))
}

type Config struct {
	Server   string `codec:"server"`
	User     string `codec:"user"`
	Password string `codec:"password"`
	Database string `codec:"database"`
	UseUDP   bool   `codec:"use_udp"`
	StripTag int    `codec:"strip_tag"`
}

type InfluxdbOutput struct {
	env    *plugin.Env
	conf   *Config
	client *influxdb.Client
}

func (p *InfluxdbOutput) Name() string {
	return "out-influxdb"
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

func (p *InfluxdbOutput) Encode(r *event.Record) (buffer.Sizer, error) {
	p.env.Log.Info(r.Time)
	tag := r.Tag
	if p.conf.StripTag > 0 {
		parts := strings.SplitN(tag, ".", p.conf.StripTag+1)
		tag = parts[len(parts)-1]
	}

	s := &Series{
		Name:    tag,
		Columns: []string{"time"},
	}
	point := []interface{}{int64(float64(r.Time.UnixNano()) * 1e-6)}
	for k, v := range r.Value {
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

func main() {
	plugin.New(func() plugin.Plugin {
		return &InfluxdbOutput{}
	}).Run()
}
