package process

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestRSSHelper(t *testing.T) {
	role := os.Getenv("HARNESS_RSS_HELPER")
	if role == "" {
		return
	}
	memory := make([]byte, 16<<20)
	for i := 0; i < len(memory); i += os.Getpagesize() {
		memory[i] = 42
	}
	if role == "child" {
		fmt.Println("ready")
		time.Sleep(30 * time.Second)
	} else {
		program, _ := os.Executable()
		child := exec.Command(program, "-test.run=^TestRSSHelper$")
		child.Env = []string{"HARNESS_RSS_HELPER=child"}
		child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		pipe, err := child.StdoutPipe()
		if err != nil || child.Start() != nil {
			os.Exit(2)
		}
		scanner := bufio.NewScanner(pipe)
		if !scanner.Scan() || scanner.Text() != "ready" {
			_ = child.Process.Kill()
			_ = child.Wait()
			os.Exit(3)
		}
		fmt.Printf("ready %d\n", child.Process.Pid)
		_, _ = io.Copy(io.Discard, os.Stdin)
		_ = child.Process.Kill()
		_ = child.Wait()
	}
	runtime.KeepAlive(memory)
	os.Exit(0)
}

func TestTreeRSSIncludesChildInAnotherGroupAndRejectsReusedRoot(t *testing.T) {
	program, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(program, "-test.run=^TestRSSHelper$")
	cmd.Env = []string{"HARNESS_RSS_HELPER=parent"}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stdin.Close(); _ = cmd.Wait() })
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "ready ") {
		t.Fatal("RSS helper did not start", scanner.Err())
	}
	child, err := strconv.Atoi(strings.TrimPrefix(scanner.Text(), "ready "))
	if err != nil {
		t.Fatal(err)
	}
	_, childGroup, _, err := ReadStat(child)
	_, parentGroup, _, parentErr := ReadStat(cmd.Process.Pid)
	if err != nil || parentErr != nil || childGroup == parentGroup {
		t.Fatal("fixture child did not leave parent group", childGroup, parentGroup, err, parentErr)
	}
	p := Identify(cmd.Process.Pid)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var rss int64
	for ctx.Err() == nil {
		rss, err = TreeRSS(ctx, p)
		if err == nil {
			break
		}
	}
	if err != nil || rss < 32<<20 {
		t.Fatal("descendant memory missing", rss, err)
	}
	p.StartTicks++
	if _, err := TreeRSS(ctx, p); err == nil {
		t.Fatal("reused root accepted")
	}
	p = Identify(cmd.Process.Pid)
	p.BootID = "another-boot"
	if _, err := TreeRSS(ctx, p); err == nil {
		t.Fatal("different boot accepted")
	}
	cancel()
	if _, err := TreeRSS(ctx, Identify(cmd.Process.Pid)); err == nil {
		t.Fatal("cancelled observation accepted")
	}
	if _, err := TreeRSS(context.Background(), Identity{}); err == nil {
		t.Fatal("missing identity accepted")
	}
}

func TestRSSStatParserRejectsInvalidAndOverflowingPages(t *testing.T) {
	f := strings.Fields("S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 99 20 7")
	raw := func() string { return "42 (comm with ) parens) " + strings.Join(f, " ") }
	s, err := parseRSSStat(raw(), 4096)
	if err != nil || s.bytes != 7*4096 || s.parent != 1 || s.ticks != 99 {
		t.Fatal(s, err)
	}
	for _, value := range []string{"-1", "bad", strconv.FormatInt(math.MaxInt64, 10)} {
		f[21] = value
		if _, err := parseRSSStat(raw(), 4096); err == nil {
			t.Fatal("invalid RSS accepted", value)
		}
	}
	if _, err := parseRSSStat("42 (short) S", 4096); err == nil {
		t.Fatal("short stat accepted")
	}
}
