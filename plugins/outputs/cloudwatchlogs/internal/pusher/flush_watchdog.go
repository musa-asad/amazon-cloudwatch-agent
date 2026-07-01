// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT

package pusher

import (
	"time"

	"github.com/influxdata/telegraf"
)

type flushWatchdog struct {
	target Target
	logger telegraf.Logger

	initialTimeout time.Duration
	maxBackoff     time.Duration

	heartbeatCh chan struct{}
	stopCh      <-chan struct{}
}

func newFlushWatchdog(target Target, logger telegraf.Logger, flushTimeout time.Duration, stopCh <-chan struct{}) *flushWatchdog {
	initial := max(3*flushTimeout, time.Second)
	return &flushWatchdog{
		target:         target,
		logger:         logger,
		initialTimeout: initial,
		maxBackoff:     max(4*flushTimeout, initial),
		heartbeatCh:    make(chan struct{}, 1),
		stopCh:         stopCh,
	}
}

func (w *flushWatchdog) heartbeat() {
	select {
	case w.heartbeatCh <- struct{}{}:
	default:
	}
}

func (w *flushWatchdog) run() {
	timer := time.NewTimer(w.initialTimeout)
	defer timer.Stop()
	backoff := w.initialTimeout

	for {
		select {
		case <-w.heartbeatCh:
			backoff = w.initialTimeout
			stopTimer(timer)
			timer.Reset(w.initialTimeout)

		case <-timer.C:
			w.logger.Warnf("Force-flush for (%v/%v) has not reset its flush timer in %s; the flush path may be stalled.",
				w.target.Group, w.target.Stream, backoff)
			backoff = watchdogBackoff(backoff, w.maxBackoff)
			timer.Reset(backoff)

		case <-w.stopCh:
			return
		}
	}
}

func watchdogBackoff(cur, maxDur time.Duration) time.Duration {
	next := cur * 2
	if next > maxDur {
		return maxDur
	}
	return next
}
