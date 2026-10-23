package kernelcli

import (
	"context"
	"encoding/base64"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type ArtifactExcerpt struct {
	Artifact   domain.Artifact `json:"artifact"`
	Offset     int64           `json:"offset"`
	NextOffset int64           `json:"next_offset"`
	Done       bool            `json:"done"`
	Encoding   string          `json:"encoding"`
	Data       string          `json:"data"`
}

func artifactExcerpt(ctx context.Context, k *storage.KernelStore, id string, offset int64, limit int) (ArtifactExcerpt, error) {
	a, data, err := k.ReadOperatorArtifact(ctx, id, offset, limit)
	out := ArtifactExcerpt{Artifact: a, Offset: offset, NextOffset: offset + int64(len(data)), Encoding: "utf8", Data: string(data)}
	if err != nil {
		return out, err
	}
	out.Done = out.NextOffset == a.Bytes
	if !utf8.Valid(data) {
		out.Encoding = "base64"
		out.Data = base64.StdEncoding.EncodeToString(data)
	}
	return out, nil
}

type MailboxPage struct {
	Entries []storage.MailboxEntry `json:"entries"`
	Cursor  int64                  `json:"cursor"`
	More    bool                   `json:"more"`
}

func mailboxPage(ctx context.Context, k *storage.KernelStore, session string, cursor int64, limit int) (MailboxPage, error) {
	out := MailboxPage{Entries: []storage.MailboxEntry{}, Cursor: cursor}
	entries, err := k.Mailbox(ctx, session, cursor, limit+1)
	if err != nil {
		return out, err
	}
	if len(entries) > limit {
		entries = entries[:limit]
		out.More = true
	}
	out.Entries = entries
	if len(entries) > 0 {
		out.Cursor = entries[len(entries)-1].DeliverySequence
	}
	return out, nil
}
