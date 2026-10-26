package storage

import (
	"context"
	"path/filepath"
	"testing"
)

func TestExistingKernelAddsOnlyDeclaredRecordIndexes(t *testing.T) {
	k, err := OpenKernel(filepath.Join(t.TempDir(), "private", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	path := k.Root + "/state.db"
	if _, err := k.DB.Exec("DROP INDEX records_work_item_kind; DROP INDEX records_kind_work_item; DROP INDEX records_split_create_wave;"); err != nil {
		t.Fatal(err)
	}
	if err := k.Close(); err != nil {
		t.Fatal(err)
	}
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	for _, name := range []string{"records_work_item_kind", "records_kind_work_item", "records_split_create_wave"} {
		var count int
		if err := k.DB.QueryRowContext(context.Background(), "SELECT count(*) FROM sqlite_master WHERE type='index' AND name=?", name).Scan(&count); err != nil || count != 1 {
			t.Fatalf("declared performance index %s missing: count=%d err=%v", name, count, err)
		}
	}
}
