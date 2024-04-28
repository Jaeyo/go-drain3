package drain3

import (
	"fmt"
	"strings"
)

type LogCluster struct {
	ClusterId         int64
	LogTemplateTokens []string
	Size              int64
}

func NewLogCluster(clusterId int64, logTemplateTokens []string) *LogCluster {
	return &LogCluster{
		ClusterId:         clusterId,
		LogTemplateTokens: logTemplateTokens,
		Size:              1,
	}
}

func (l *LogCluster) GetTemplate() string {
	return strings.Join(l.LogTemplateTokens, " ")
}

func (l *LogCluster) String() string {
	return fmt.Sprintf("ID=%-5d : size=%-10d: %s", l.ClusterId, l.Size, l.GetTemplate())
}
