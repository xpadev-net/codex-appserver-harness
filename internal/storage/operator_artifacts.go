package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// ReadOperatorArtifact is a private-root operator observation port, never a
// model tool. Historical committed bytes remain inspectable without changing
// grants or making the artifact eligible for any current Task/result.
// Hashing and excerpt capture share one bounded-buffer pass over the same bytes.
func (k *KernelStore) ReadOperatorArtifact(ctx context.Context, id string, offset int64, limit int) (domain.Artifact, []byte, error) {
	var a domain.Artifact
	if offset < 0 || limit < 1 || limit > contract.MaxInline {
		return a, nil, errors.New("invalid Artifact excerpt bounds")
	}
	a, err := k.Artifact(ctx, id)
	if err != nil {
		return a, nil, err
	}
	if a.State != "committed" {
		return a, nil, ErrArtifactUnavailable
	}
	if offset > a.Bytes {
		return a, nil, errors.New("Artifact excerpt begins beyond content")
	}
	if a.Bytes > k.ControllerArtifactBytes || a.Bytes == int64(^uint64(0)>>1) {
		return a, nil, ErrArtifactIntegrity
	}
	root, err := k.openArtifactRoot()
	if err != nil {
		return a, nil, err
	}
	defer root.Close()
	f, err := root.OpenFile(a.ID, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return a, nil, err
	}
	defer f.Close()
	if err = privateArtifactFile(f); err != nil {
		return a, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return a, nil, err
	}
	if info.Size() != a.Bytes {
		return a, nil, ErrArtifactIntegrity
	}
	end := a.Bytes
	if int64(limit) < end-offset {
		end = offset + int64(limit)
	}
	data := make([]byte, 0, end-offset)
	hash := sha256.New()
	reader := io.LimitReader(f, a.Bytes+1)
	buf := make([]byte, 32<<10)
	var total int64
	for {
		if err = ctx.Err(); err != nil {
			return a, nil, err
		}
		n, readErr := reader.Read(buf)
		if n > 0 {
			_, _ = hash.Write(buf[:n])
			first, last := max(offset, total), min(end, total+int64(n))
			if first < last {
				data = append(data, buf[first-total:last-total]...)
			}
			total += int64(n)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return a, nil, readErr
		}
	}
	if total != a.Bytes || fmt.Sprintf("%x", hash.Sum(nil)) != a.Digest {
		return a, nil, ErrArtifactIntegrity
	}
	return a, data, nil
}
