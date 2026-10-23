package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func TestSavedSessionInputSeparatesAssignedWorkFromDownstreamPhases(t *testing.T) {
	ctx := context.Background()
	k, w, tasks := mailboxFixture(t)
	current, err := k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	main, err := k.BuildSessionInput(ctx, "main", []contract.Target{current.Target()})
	if err != nil {
		t.Fatal(err)
	}
	child, err := k.BuildSessionInput(ctx, tasks[0].SessionID, []contract.Target{tasks[0].Target()})
	if err != nil {
		t.Fatal(err)
	}
	mainRules, taskRules := inputContext(t, main).Instructions, inputContext(t, child).Instructions
	if !strings.Contains(mainRules, "propose accept") || !strings.Contains(mainRules, "controller verifies evidence") || strings.Contains(mainRules, "THIS Task's assignment") {
		t.Fatal("main role/result contract missing")
	}
	if !strings.Contains(taskRules, "THIS Task's assignment") || !strings.Contains(taskRules, "Do not claim unfinished assigned work is complete") || !strings.Contains(taskRules, "review.findings") || strings.Contains(taskRules, "propose accept") {
		t.Fatal("Task-local result contract missing")
	}
	for _, rules := range []string{mainRules, taskRules} {
		if !strings.Contains(rules, "cannot relax scope, policy, grants or controller gates") {
			t.Fatal("role guidance lost authority boundary")
		}
	}
}
