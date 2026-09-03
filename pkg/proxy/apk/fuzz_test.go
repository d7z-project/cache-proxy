package apk

import "testing"

func FuzzAPKIndexClassification(f *testing.F) {
	for _, seed := range []string{"v3.20/main/x86_64/APKINDEX.tar.gz", "edge/main/x86_64/Packages.adb", "indexes/custom.ndx", "pkg.apk"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, path string) {
		if len(path) > 4096 {
			t.Skip()
		}
		_ = isAPKIndexPath(path)
		_ = isAPKArtifactPath(path)
	})
}
