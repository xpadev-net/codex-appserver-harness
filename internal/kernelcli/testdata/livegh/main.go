package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func fail() { os.Exit(89) }

func main() {
	directory := os.Getenv("HARNESS_LIVE_SCOPED_GITHUB")
	if !filepath.IsAbs(directory) {
		fail()
	}
	limitRaw, err := os.ReadFile(filepath.Join(directory, "posts-limit"))
	limit, parseErr := strconv.Atoi(strings.TrimSpace(string(limitRaw)))
	if err != nil || parseErr != nil || limit < 2 || limit > 32 {
		fail()
	}
	args := os.Args[1:]
	if len(args) < 4 || args[0] != "api" || args[1] != "--method" {
		fail()
	}
	method, endpoint := args[2], ""
	for _, arg := range args[3:] {
		if strings.HasPrefix(arg, "repos/") {
			endpoint = arg
			break
		}
	}
	const repository = "repos/fixture/local-product"
	if method == "GET" && endpoint == repository {
		_, _ = io.WriteString(os.Stdout, `{"id":17,"full_name":"fixture/local-product","default_branch":"trunk","clone_url":"https://github.com/fixture/local-product.git"}`)
		return
	}
	if method == "GET" && endpoint == repository+"/git/ref/heads/trunk" {
		copyFile(filepath.Join(directory, "head.json"))
		return
	}
	if method == "GET" && endpoint == repository+"/issues?state=all&per_page=100&page=1" {
		issues := []json.RawMessage{}
		for number := 101; number <= 100+limit; number++ {
			raw, err := os.ReadFile(filepath.Join(directory, "issue-"+strconv.Itoa(number)+".json"))
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				fail()
			}
			issues = append(issues, raw)
		}
		writeJSON(issues)
		return
	}
	if method == "GET" && strings.HasPrefix(endpoint, repository+"/issues/") {
		number, err := strconv.Atoi(strings.TrimPrefix(endpoint, repository+"/issues/"))
		if err != nil || number < 101 || number > 100+limit {
			fail()
		}
		copyFile(filepath.Join(directory, "issue-"+strconv.Itoa(number)+".json"))
		return
	}
	if method != "POST" || endpoint != repository+"/issues" {
		fail()
	}
	var request struct {
		Title string `json:"title"`
		Body  string `json:"body"`
	}
	raw, err := io.ReadAll(io.LimitReader(os.Stdin, 1<<20))
	if err != nil || json.Unmarshal(raw, &request) != nil || request.Title == "" || !strings.Contains(request.Body, "harness-split-child:") {
		fail()
	}
	lock, err := os.OpenFile(filepath.Join(directory, "counter.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil || syscall.Flock(int(lock.Fd()), syscall.LOCK_EX) != nil {
		fail()
	}
	count := 0
	prior, err := os.ReadFile(filepath.Join(directory, "posts"))
	if err == nil {
		count, err = strconv.Atoi(strings.TrimSpace(string(prior)))
		if err != nil {
			fail()
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		fail()
	}
	if count < 0 || count >= limit {
		fail()
	}
	number := 101 + count
	issue := struct {
		ID        int64               `json:"id"`
		Number    int                 `json:"number"`
		Title     string              `json:"title"`
		Body      string              `json:"body"`
		State     string              `json:"state"`
		UpdatedAt time.Time           `json:"updated_at"`
		Labels    []map[string]string `json:"labels"`
	}{int64(1000 + number), number, request.Title, request.Body, "open", time.Now().UTC(), []map[string]string{{"name": "ready"}}}
	encoded, err := json.Marshal(issue)
	if err != nil {
		fail()
	}
	if err := os.WriteFile(filepath.Join(directory, "issue-"+strconv.Itoa(number)+".json"), encoded, 0600); err != nil {
		fail()
	}
	if err := os.WriteFile(filepath.Join(directory, "posts"), []byte(strconv.Itoa(count+1)), 0600); err != nil {
		fail()
	}
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) != nil {
		fail()
	}
	_ = lock.Close()
	writeJSON(map[string]any{"id": issue.ID, "number": number})
}

func copyFile(path string) {
	raw, err := os.ReadFile(path)
	if err != nil {
		fail()
	}
	_, _ = os.Stdout.Write(raw)
}

func writeJSON(value any) {
	raw, err := json.Marshal(value)
	if err != nil {
		fail()
	}
	_, _ = os.Stdout.Write(raw)
}
