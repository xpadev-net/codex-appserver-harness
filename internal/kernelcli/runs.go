package kernelcli

import (
	"context"
	"encoding/base64"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type RunOutputExcerpt struct {
	Entry      storage.RunHistoryEntry `json:"entry"`
	Offset     int64                   `json:"offset"`
	NextOffset int64                   `json:"next_offset"`
	Done       bool                    `json:"done"`
	Encoding   string                  `json:"encoding"`
	Data       string                  `json:"data"`
}

func runOutputExcerpt(ctx context.Context, k *storage.KernelStore, id string, offset int64, limit int) (RunOutputExcerpt, error) {
	entry, data, err := k.InspectRunOutput(ctx, id, offset, limit)
	if err != nil {
		return RunOutputExcerpt{}, err
	}
	out := RunOutputExcerpt{Entry: entry, Offset: offset, NextOffset: offset + int64(len(data)), Encoding: "utf8", Data: string(data)}
	out.Done = out.NextOffset == entry.Run.FinalBytes
	if !utf8.Valid(data) {
		out.Encoding = "base64"
		out.Data = base64.StdEncoding.EncodeToString(data)
	}
	return out, nil
}
