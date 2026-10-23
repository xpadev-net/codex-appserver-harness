package kernelcli

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func TestNativeReportRequiresCompleteSourceAndNumericValues(t *testing.T) {
	for _, body := range []string{"data=[4,7,10]\nsum=21\naverage=7\n", "DATA = [4, 7, 10]\nSUM = 21.0\nAVERAGE = 7e0\n"} {
		if err := verifyNativeReport([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	for _, body := range []string{
		"data=[40,70,100]\nsum=210\naverage=70\n",
		"data=[4,7,10]\nsum=210\naverage=70\n",
		"sum=21\naverage=7\n",
		"data=[4,7]\nsum=21\naverage=7\n",
		"data=[4,7,10]\nsum=21suffix\naverage=7\n",
		"data=[4,7,10]\nsum=21/0\naverage=7\n",
		"data=[4,7,10]\nsum=21\nsum=210\naverage=7\n",
		"data=[4,7,10]\nsum=NaN\naverage=7\n",
	} {
		if err := verifyNativeReport([]byte(body)); err == nil {
			t.Fatal("incorrect source/arithmetic accepted", body)
		}
	}
}

func TestNativeScheduleRejectsSerializedTasksAndIntermediateMain(t *testing.T) {
	base := time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)
	interval := func(session string, start, stop int) nativeInterval {
		return nativeInterval{SessionID: session, Start: base.Add(time.Duration(start) * time.Second), Stop: base.Add(time.Duration(stop) * time.Second)}
	}
	valid := func() nativeSchedule {
		return nativeSchedule{MainID: "main", Initial: []domain.Task{{SessionID: "draft", PhaseID: "initial"}, {SessionID: "check", PhaseID: "initial"}}, Runs: []nativeInterval{
			interval("main", 0, 2), interval("draft", 3, 6), interval("check", 4, 7), interval("review", 8, 9), interval("fix", 10, 11), interval("fresh-review", 12, 13), interval("report", 14, 15), interval("main", 16, 17),
		}}
	}
	if err := verifyNativeSchedule(valid()); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*nativeSchedule){
		func(s *nativeSchedule) { s.Runs[2] = interval("check", 6, 7) },
		func(s *nativeSchedule) { s.Runs = append(s.Runs, interval("main", 9, 10)) },
		func(s *nativeSchedule) { s.Runs[7] = interval("main", 13, 14) },
		func(s *nativeSchedule) { s.Runs[1].Stop = time.Time{} },
		func(s *nativeSchedule) { s.Initial[1].PhaseID = "later" },
		func(s *nativeSchedule) { s.Runs = append(s.Runs, interval("draft", 7, 8)) },
	} {
		s := valid()
		change(&s)
		if err := verifyNativeSchedule(s); err == nil {
			t.Fatal("invalid schedule accepted")
		}
	}
	// A plan tool may start initial Tasks before the first main turn ends.
	s := valid()
	s.Runs[0].Stop = base.Add(5 * time.Second)
	if err := verifyNativeSchedule(s); err != nil {
		t.Fatal(err)
	}
}
func TestNativeVersionUsesActualConfiguredExecutable(t *testing.T) {
	for _, version := range []string{"codex-cli 0.163.0", "codex-cli 0.164.0"} {
		binary := filepath.Join(t.TempDir(), "native-version")
		if err := os.WriteFile(binary, []byte("#!/bin/sh\nprintf '%s\\n' '"+version+"'\n"), 0700); err != nil {
			t.Fatal(err)
		}
		actual, err := verifiedNativeVersion(context.Background(), binary)
		if actual != version {
			t.Fatal("reported a different configured version", actual, version)
		}
		if (err == nil) != (version == "codex-cli 0.163.0") {
			t.Fatal("version compatibility mismatch", err)
		}
	}
}
