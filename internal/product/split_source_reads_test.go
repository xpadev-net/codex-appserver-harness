package product

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestSplitSourceReadsBoundsConcurrencyAndJoinsEveryRead(t *testing.T) {
	var active, peak, completed atomic.Int32
	entered := make(chan struct{}, 9)
	release := make(chan struct{})
	reads := make([]func(context.Context) error, 9)
	for i := range reads {
		reads[i] = func(context.Context) error {
			count := active.Add(1)
			for old := peak.Load(); count > old && !peak.CompareAndSwap(old, count); old = peak.Load() {
			}
			entered <- struct{}{}
			<-release
			completed.Add(1)
			active.Add(-1)
			return nil
		}
	}
	done := make(chan error, 1)
	go func() { done <- runSplitSourceReads(context.Background(), reads) }()
	for i := 0; i < 4; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("source observations were serialized")
		}
	}
	select {
	case <-entered:
		close(release)
		t.Fatal("source concurrency exceeded four")
	default:
	}
	close(release)
	select {
	case err := <-done:
		if err != nil || active.Load() != 0 || completed.Load() != 9 || peak.Load() != 4 {
			t.Fatal("incomplete bounded observation batch", err, active.Load(), completed.Load(), peak.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("source reads were not joined")
	}
}

func TestSplitSourceReadsFailureAndCancellationJoinSavesBeforeReturning(t *testing.T) {
	for _, mode := range []string{"read_failure", "parent_canceled"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sentinel := errors.New("source GET failed")
			entered := make(chan struct{}, 4)
			stopped := make(chan struct{}, 4)
			trigger := make(chan struct{})
			releaseSave := make(chan struct{})
			defer close(releaseSave)
			var started, saved atomic.Int32
			reads := make([]func(context.Context) error, 8)
			for i := range reads {
				reads[i] = func(read context.Context) error {
					started.Add(1)
					entered <- struct{}{}
					if i == 0 && mode == "read_failure" {
						<-trigger
						return sentinel
					}
					<-read.Done()
					stopped <- struct{}{}
					<-releaseSave
					saved.Add(1)
					return read.Err()
				}
			}
			done := make(chan error, 1)
			go func() { done <- runSplitSourceReads(ctx, reads) }()
			for i := 0; i < 4; i++ {
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("batch did not start four observations")
				}
			}
			expectedStops := 3
			if mode == "parent_canceled" {
				expectedStops = 4
				cancel()
			} else {
				close(trigger)
			}
			for i := 0; i < expectedStops; i++ {
				select {
				case <-stopped:
				case <-time.After(5 * time.Second):
					t.Fatal("observers did not receive cancellation")
				}
			}
			select {
			case <-done:
				t.Fatal("returned with detached source saves")
			default:
			}
			// Let the simulated final source saves finish, then require a joined result.
			for i := 0; i < expectedStops; i++ {
				releaseSave <- struct{}{}
			}
			select {
			case err := <-done:
				expected := sentinel
				if mode == "parent_canceled" {
					expected = context.Canceled
				}
				if !errors.Is(err, expected) || saved.Load() != int32(expectedStops) || started.Load() != 4 {
					t.Fatal("batch failure lost join or started queued observations", err, saved.Load(), started.Load())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("failed batch did not join")
			}
		})
	}
}
