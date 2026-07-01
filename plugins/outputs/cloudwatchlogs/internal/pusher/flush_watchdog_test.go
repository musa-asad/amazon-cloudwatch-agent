// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT

package pusher

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aws/amazon-cloudwatch-agent/tool/testutil"
	"github.com/aws/amazon-cloudwatch-agent/tool/util"
)

func startTestWatchdog(t *testing.T, initialTimeout, maxBackoff time.Duration) (*flushWatchdog, *testutil.LogSink, func()) {
	t.Helper()
	sink := testutil.NewLogSink()
	stopCh := make(chan struct{})
	w := newFlushWatchdog(Target{"G", "S", util.StandardLogGroupClass, -1}, sink, time.Second, stopCh)
	w.initialTimeout = initialTimeout
	w.maxBackoff = maxBackoff

	done := make(chan struct{})
	go func() {
		w.run()
		close(done)
	}()

	stop := func() {
		close(stopCh)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("watchdog goroutine leaked")
		}
	}
	return w, sink, stop
}

func warnLines(sink *testutil.LogSink) []string {
	var out []string
	for _, l := range sink.Lines() {
		if strings.Contains(l, "Force-flush for") {
			out = append(out, l)
		}
	}
	return out
}

func parseBackoff(line string) (time.Duration, bool) {
	const pre = "flush timer in "
	i := strings.Index(line, pre)
	if i < 0 {
		return 0, false
	}
	rest := line[i+len(pre):]
	j := strings.Index(rest, ";")
	if j < 0 {
		return 0, false
	}
	d, err := time.ParseDuration(rest[:j])
	if err != nil {
		return 0, false
	}
	return d, true
}

func waitUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}

func TestFlushWatchdog_HeartbeatWithinDeadlineNoWarn(t *testing.T) {
	initialTimeout := 40 * time.Millisecond
	w, sink, stop := startTestWatchdog(t, initialTimeout, 80*time.Millisecond)
	defer stop()

	deadline := time.Now().Add(8 * initialTimeout)
	for time.Now().Before(deadline) {
		w.heartbeat()
		time.Sleep(initialTimeout / 4)
	}

	require.Empty(t, warnLines(sink), "heartbeats within the deadline must keep the watchdog silent")
}

func TestFlushWatchdog_NoHeartbeatWarnsBacksOffAndCaps(t *testing.T) {
	initialTimeout := 20 * time.Millisecond
	capBackoff := 80 * time.Millisecond
	_, sink, stop := startTestWatchdog(t, initialTimeout, capBackoff)
	defer stop()

	require.True(t, waitUntil(3*time.Second, func() bool { return len(warnLines(sink)) >= 5 }),
		"expected sustained warnings while no heartbeats arrive")

	lines := warnLines(sink)
	expected := []time.Duration{
		20 * time.Millisecond,
		40 * time.Millisecond,
		80 * time.Millisecond,
		80 * time.Millisecond,
		80 * time.Millisecond,
	}
	for i, want := range expected {
		got, ok := parseBackoff(lines[i])
		require.True(t, ok, "warning %d not parseable: %s", i, lines[i])
		require.Equal(t, want, got, "warning %d backoff", i)
	}
}

func TestFlushWatchdog_HeartbeatAfterWarnResetsBackoff(t *testing.T) {
	initialTimeout := 20 * time.Millisecond
	capBackoff := 160 * time.Millisecond
	w, sink, stop := startTestWatchdog(t, initialTimeout, capBackoff)
	defer stop()

	require.True(t, waitUntil(3*time.Second, func() bool { return len(warnLines(sink)) >= 2 }),
		"expected backoff to grow while no heartbeats arrive")

	n := len(warnLines(sink))
	w.heartbeat()

	require.True(t, waitUntil(3*time.Second, func() bool {
		cur := warnLines(sink)
		for i := n; i < len(cur); i++ {
			if d, ok := parseBackoff(cur[i]); ok && d == initialTimeout {
				return true
			}
		}
		return false
	}), "a heartbeat after a warning must reset backoff to initialTimeout")
}

func TestFlushWatchdog_HeartbeatAfterStopNoPanicNoBlock(t *testing.T) {
	w, _, stop := startTestWatchdog(t, 20*time.Millisecond, 80*time.Millisecond)
	stop()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			w.heartbeat()
		}
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat() blocked after stopCh closed")
	}
}

func TestFlushWatchdog_LargeFlushTimeout(t *testing.T) {
	initialTimeout := 30 * time.Millisecond
	capBackoff := 40 * time.Millisecond
	_, sink, stop := startTestWatchdog(t, initialTimeout, capBackoff)
	defer stop()

	require.True(t, waitUntil(3*time.Second, func() bool { return len(warnLines(sink)) >= 3 }),
		"expected warnings for large flush timeout")

	lines := warnLines(sink)
	expected := []time.Duration{
		30 * time.Millisecond,
		40 * time.Millisecond,
		40 * time.Millisecond,
	}
	for i, want := range expected {
		got, ok := parseBackoff(lines[i])
		require.True(t, ok, "warning %d not parseable: %s", i, lines[i])
		require.Equal(t, want, got, "warning %d backoff", i)
	}
}
