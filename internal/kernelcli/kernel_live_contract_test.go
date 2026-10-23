package kernelcli

import (
	"context"
	"os"
	"path/filepath"
	"testing"
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
