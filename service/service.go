// MIT License
//
// Copyright (c) 2018 Richard Hawkins
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Package service provides a simple service to listen for statsd event messages.
package service

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// State of datapup.
type State struct {
	Addr    string
	Metrics map[string]int
}

// New returns the state of the datapup app.
func New(addr string) *State {
	return &State{
		Addr:    addr,
		Metrics: make(map[string]int, 10),
	}
}

// Listen for datadog event metrics.
func (s State) Listen() {
	fmt.Printf("starting datapup service at %s\n", s.Addr)
	ServerAddr, err := net.ResolveUDPAddr("udp", s.Addr)
	check(err, true)

	conn, err := net.ListenUDP("udp", ServerAddr)
	check(err, true)
	defer conn.Close()

	buf := make([]byte, 1024)

	for {
		n, _, err := conn.ReadFromUDP(buf)
		check(err, false)
		data := string(buf[0:n])
		if err := s.handle(data); err != nil {
			check(err, false)
		}
	}
}

// handle data received.
func (s State) handle(data string) error {
	metrics := strings.Split(data, "\n")
	errors := []string{}
	for _, m := range metrics {
		if err := s.process(m); err != nil {
			errors = append(errors, fmt.Sprintf("error processing metric: \"%s\", %s", m, err.Error()))
		}
	}
	if len(errors) != 0 {
		return fmt.Errorf("%s", strings.Join(errors, "\n"))
	}
	return nil
}

// process the metric received.
// metric format: metric.name:value|type|@sample_rate|#tag1:value,tag2
// more info at https://help.datadoghq.com/hc/en-us/articles/206441345-Send-metrics-and-events-using-dogstatsd-and-the-shell
func (s State) process(metric string) error {
	parts := strings.Split(metric, "|")

	if len(parts) < 1 {
		return fmt.Errorf("invalid metric: \"%s\"", metric)
	}

	nameValue := strings.Split(parts[0], ":")
	if len(nameValue) < 2 {
		return fmt.Errorf("invalid name/value pair: \"%s\"", metric)
	}

	name := nameValue[0]
	value, err := strconv.ParseInt(nameValue[1], 10, 64)
	if err != nil {
		return fmt.Errorf("can not convert value to int: \"%s\", %s", metric, err)
	}

	if _, ok := s.Metrics[name]; !ok {
		s.Metrics[name] = 0
	}
	s.Metrics[name] += int(value)
	fmt.Printf("%s\t (total: %d)\n", metric, s.Metrics[name])
	return nil
}

// check if there is an error, if so print it.  If fatal also panic.
func check(err error, fatal bool) {
	if err != nil {
		fmt.Printf("error: %s\n", err)
		if fatal {
			panic("terminating due to error")
		}
	}
}
