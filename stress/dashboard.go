package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor"
)

func dashHideCursor() { fmt.Print("\033[?25l") }
func dashShowCursor() { fmt.Print("\033[?25h") }
func dashClear()      { fmt.Print("\033[H\033[2J") }

func dashBar(cur, max, width int) string {
	if max <= 0 {
		max = 1
	}
	if cur < 0 {
		cur = 0
	}
	if cur > max {
		cur = max
	}
	filled := int(float64(cur) / float64(max) * float64(width))
	if filled < 0 {
		filled = 0
	}
	if filled > width {
		filled = width
	}
	return strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
}

func RenderDashboard(
	cfg Config,
	start time.Time,
	driver string,
	targetJobs int64,
	p Progress,
	rate float64,
	workers []*taskharbor.Worker,
	labels []string,
) {
	if len(workers) != len(labels) {
		return
	}

	dashClear()

	elapsed := time.Since(start)
	terminal := p.Done + p.DLQ

	totalWorkers := cfg.NumQueues * cfg.WorkersPerQueue
	theoryMaxInflight := totalWorkers * cfg.Concurrency

	doneLabel := "done="
	if p.DoneApprox {
		doneLabel = "done≈"
	}

	fmt.Printf(
		"TaskHarbor stress  driver=%s  elapsed=%s  rate=%.0f jobs/s\n",
		driver,
		elapsed.Round(time.Millisecond),
		rate,
	)

	fmt.Printf(
		"terminal=%d/%d  %s%d  dlq=%d  ready=%d  scheduled=%d  inflight=%d  now=%s\n",
		terminal, targetJobs,
		doneLabel, p.Done,
		p.DLQ, p.Ready, p.Scheduled, p.Inflight,
		p.Now.Round(time.Millisecond).Format(time.RFC3339Nano),
	)

	fmt.Printf(
		"queues=%d  workers/queue=%d  totalWorkers=%d  concurrency/worker=%d  maxInflight(theory)=%d\n\n",
		cfg.NumQueues, cfg.WorkersPerQueue, totalWorkers, cfg.Concurrency, theoryMaxInflight,
	)

	type line struct {
		label  string
		id     string
		active int
	}
	lines := make([]line, 0, len(workers))
	activeTotal := 0

	for i, w := range workers {
		a := int(w.Active())
		activeTotal += a
		lines = append(lines, line{
			label:  labels[i],
			id:     w.ID(),
			active: a,
		})
	}

	sort.Slice(lines, func(i, j int) bool { return lines[i].label < lines[j].label })

	fmt.Printf("active handlers: total=%d  max(theory)=%d\n", activeTotal, theoryMaxInflight)

	barW := 30
	for _, ln := range lines {
		fmt.Printf(
			"%-8s  %s  [%s]  %2d/%d\n",
			ln.label,
			ln.id,
			dashBar(ln.active, cfg.Concurrency, barW),
			ln.active,
			cfg.Concurrency,
		)
	}
}
