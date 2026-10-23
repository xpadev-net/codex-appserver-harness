package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func TestOperatorArtifactReadsHistoricalBytesWithoutGrantingCurrentAccess(t *testing.T) {
	ctx := context.Background()
	k, w, request := artifactFixture(t)
	data := bytes.Repeat([]byte("historical report\n"), 10000)
	a, err := k.ImportArtifact(ctx, request, data)
	if err != nil {
		t.Fatal(err)
	}
	direction, err := k.RequestWaveDirection(ctx, WaveDirectionRequest{Key: "new-scope", Target: w.Target(), Instruction: "new scoped research"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.ApplyWaveDirection(ctx, direction.ID); err != nil {
		t.Fatal(err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = k.ReadArtifact(ctx, request.SessionID, current.Target(), a.ID, 0, 10); !errors.Is(err, ErrArtifactScope) {
		t.Fatal("model gained old scope access", err)
	}
	manifest, chunk, err := k.ReadOperatorArtifact(ctx, a.ID, 32000, 8192)
	if err != nil || manifest.Target() != w.Target() || !bytes.Equal(chunk, data[32000:32000+8192]) {
		t.Fatal(manifest, len(chunk), err)
	}
	if _, _, err = k.ReadArtifact(ctx, request.SessionID, current.Target(), a.ID, 0, 10); !errors.Is(err, ErrArtifactScope) {
		t.Fatal("operator read changed model grant", err)
	}
	for _, bounds := range []struct {
		offset int64
		limit  int
	}{{-1, 10}, {0, 0}, {0, contract.MaxInline + 1}, {a.Bytes + 1, 1}} {
		if _, _, err = k.ReadOperatorArtifact(ctx, a.ID, bounds.offset, bounds.limit); err == nil {
			t.Fatal("invalid excerpt accepted", bounds)
		}
	}
	_, chunk, err = k.ReadOperatorArtifact(ctx, a.ID, a.Bytes, 1)
	if err != nil || len(chunk) != 0 {
		t.Fatal(chunk, err)
	}
	// Corruption after the requested excerpt must be caught before returning it.
	data[len(data)-1] ^= 1
	if err = os.WriteFile(filepath.Join(k.Root, "objects", a.ID), data, 0600); err != nil {
		t.Fatal(err)
	}
	_, chunk, err = k.ReadOperatorArtifact(ctx, a.ID, 0, 10)
	if !errors.Is(err, ErrArtifactIntegrity) || chunk != nil {
		t.Fatal("tail corruption returned unverified excerpt", chunk, err)
	}
}
func TestOperatorArtifactRejectsRedirectedAndMissingObjects(t *testing.T) {
	for _, mode := range []string{"symlink", "hardlink", "missing", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, _, request := artifactFixture(t)
			a, err := k.ImportArtifact(ctx, request, []byte("private report"))
			if err != nil {
				t.Fatal(err)
			}
			object := filepath.Join(k.Root, "objects", a.ID)
			if mode != "canceled" {
				if err = os.Remove(object); err != nil {
					t.Fatal(err)
				}
			}
			outside := filepath.Join(t.TempDir(), "outside")
			if err = os.WriteFile(outside, []byte("private report"), 0600); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "symlink":
				err = os.Symlink(outside, object)
			case "hardlink":
				err = os.Link(outside, object)
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, chunk, err := k.ReadOperatorArtifact(ctx, a.ID, 0, 10); err == nil || chunk != nil {
				t.Fatal("unavailable object accepted", chunk, err)
			}
		})
	}
}
