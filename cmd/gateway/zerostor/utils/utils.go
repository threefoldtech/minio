package utils

import (
	"log"
	"os"
)

// Exists checks if a dir exists or not
func Exists(name string) (bool, error) {
	_, err := os.Stat(name)
	log.Printf("%s", err)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}

func RemoveFile(path string) error {
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return nil
}
