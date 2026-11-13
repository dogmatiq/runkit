package pages

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
)

// pagesVersion identifies the pages that this server renders, by hashing their
// templates, stylesheet and scripts.
//
// A page records the version that rendered it. When the page connects for live
// updates, the server tells it the version that it serves now. If they differ,
// the server has restarted with different pages, and the page reloads itself.
var pagesVersion = hashFiles(templateFiles, staticFiles)

// hashFiles returns a short hash of the paths and contents of every file in
// the given file systems.
func hashFiles(fileSystems ...fs.FS) string {
	h := sha256.New()

	for _, fsys := range fileSystems {
		if err := fs.WalkDir(
			fsys,
			".",
			func(path string, d fs.DirEntry, err error) error {
				if err != nil || d.IsDir() {
					return err
				}

				data, err := fs.ReadFile(fsys, path)
				if err != nil {
					return err
				}

				fmt.Fprintf(h, "%s\x00%d\x00", path, len(data))
				h.Write(data)

				return nil
			},
		); err != nil {
			panic(err)
		}
	}

	return hex.EncodeToString(h.Sum(nil))[:12]
}
