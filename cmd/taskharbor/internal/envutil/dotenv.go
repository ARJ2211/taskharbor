package envutil

import (
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
)

/*
LoadRepoDotenv walks up from startDir until it finds go.mod, then loads .env from that directory.

If not found, it does nothing and returns nil.
*/
func LoadRepoDotenv(startDir string) error {
	dir, err := filepath.Abs(startDir)
	if err != nil {
		return err
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			envPath := filepath.Join(dir, ".env")
			if _, err := os.Stat(envPath); err == nil {
				return godotenv.Load(envPath)
			}
			return nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return nil
		}
		dir = parent
	}
}
