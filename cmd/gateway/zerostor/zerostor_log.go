package zerostor

import (
	"bytes"

	"github.com/sirupsen/logrus"
)

var (
	zosLogPrefix = []byte("4::") //4 is for message for operator
)

//ZOSLogFormatter log formatter for zos
type ZOSLogFormatter struct {
	Default logrus.Formatter
	Error   logrus.Formatter
}

//Format formats the log messages for zos
func (l *ZOSLogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	formatter := l.Default
	if entry.Level <= logrus.ErrorLevel {
		formatter = l.Error
	}

	line, err := formatter.Format(entry)
	if err != nil {
		return line, err
	}

	if entry.Level <= logrus.ErrorLevel {
		return bytes.Join([][]byte{zosLogPrefix, line}, nil), nil
	}

	return line, nil
}
