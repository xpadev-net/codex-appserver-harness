package storage

import "github.com/xpadev-net/codex-appserver-harness/internal/runner"

// ProductMachineVerificationInput is an immutable, public summary for an
// independent reviewer. Full runner paths, argv, environment and reasons remain
// private. This input conveys observations, never publication/merge permission.
type ProductMachineVerificationInput struct {
	Plan           ProductVerificationPlanReference `json:"plan"`
	Policy         ProductVerificationPlanReference `json:"policy"`
	Candidate      runner.Candidate                 `json:"candidate"`
	Profiles       []string                         `json:"profiles"`
	Jobs           []ProductMachineVerificationJob  `json:"jobs"`
	Complete       bool                             `json:"complete"`
	Passed         bool                             `json:"passed"`
	EvidenceDigest string                           `json:"evidence_digest"`
	Digest         string                           `json:"digest"`
}
type ProductMachineVerificationJob struct {
	ProfileID        string `json:"profile_id"`
	JobID            string `json:"job_id"`
	JobDigest        string `json:"job_digest,omitempty"`
	State            string `json:"state"`
	Status           string `json:"status,omitempty"`
	TestedSHA        string `json:"tested_sha,omitempty"`
	CompletionDigest string `json:"completion_digest,omitempty"`
	ResultDigest     string `json:"result_digest,omitempty"`
	LogDigest        string `json:"log_digest,omitempty"`
	LogBytes         int64  `json:"log_bytes,omitempty"`
}

func productMachineVerificationInput(e ProductVerificationEvidence) (ProductMachineVerificationInput, error) {
	input := ProductMachineVerificationInput{Plan: e.Value.Plan, Policy: e.Value.Policy, Candidate: e.Value.Candidate, Profiles: append([]string{}, e.Value.Profiles...), Jobs: []ProductMachineVerificationJob{}, Complete: e.Value.Complete, Passed: e.Value.Passed, EvidenceDigest: e.Digest}
	for _, job := range e.Value.Jobs {
		summary := ProductMachineVerificationJob{ProfileID: job.ProfileID, JobID: job.JobID, JobDigest: job.JobDigest, State: job.State}
		if job.Completion != nil {
			c := job.Completion
			summary.Status, summary.TestedSHA = c.Result.Status, c.Result.TestedSHA
			var err error
			summary.CompletionDigest, err = contentDigest(c)
			if err != nil {
				return input, err
			}
			summary.ResultDigest, summary.LogDigest, summary.LogBytes = c.ResultDigest, c.LogDigest, c.LogBytes
		}
		input.Jobs = append(input.Jobs, summary)
	}
	// Hash the public fields with the digest field empty, independently of the
	// full private evidence checksum supplied for audit correlation.
	digest, err := contentDigest(input)
	input.Digest = digest
	return input, err
}
