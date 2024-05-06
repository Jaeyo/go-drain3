package drain3

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDrain(t *testing.T) {
	drain, err := NewDrain(WithExtraDelimiter([]string{"_"}))
	require.NoError(t, err)

	miner := NewTemplateMiner(drain, NewMemoryPersistence())

	logs := []string{
		"[ProducerStateManager partition=__consumer_offsets-48] Writing producer snapshot at offset 4339939698 (kafka.log.ProducerStateManager)",
		"[Log partition=__consumer_offsets-48, dir=/home1/irteam/apps/data/kafka/kafka-logs] Rolled new log segment at offset 4339939698 in 3 ms. (kafka.log.Log)",
		"[Log partition=__consumer_offsets-48, dir=/home1/irteam/apps/data/kafka/kafka-logs] Deleting segment files LogSegment(baseOffset=0, size=0, lastModifiedTime=1645674584000, largestRecordTimestamp=None) (kafka.log.Log)",
		"Deleted log /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000000000000000.log.deleted. (kafka.log.LogSegment)",
		"Deleted offset index /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000000000000000.index.deleted. (kafka.log.LogSegment)",
		"Deleted time index /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000000000000000.timeindex.deleted. (kafka.log.LogSegment)",
		"[Log partition=__consumer_offsets-48, dir=/home1/irteam/apps/data/kafka/kafka-logs] Deleting segment files LogSegment(baseOffset=2147429227, size=0, lastModifiedTime=1710735195000, largestRecordTimestamp=None) (kafka.log.Log)",
		"Deleted log /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000002147429227.log.deleted. (kafka.log.LogSegment)",
		"Deleted offset index /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000002147429227.index.deleted. (kafka.log.LogSegment)",
		"Deleted time index /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000002147429227.timeindex.deleted. (kafka.log.LogSegment)",
		"[ProducerStateManager partition=__consumer_offsets-49] Writing producer snapshot at offset 4339698 (kafka.log.ProducerStateManager)",
		"[Log partition=__consumer_offsets-48, dir=/home1/irteam/apps/data/kafka/kafka-logs] Deleting segment files LogSegment(baseOffset=4294790577, size=2703, lastModifiedTime=1711832815000, largestRecordTimestamp=Some(1710827112244)) (kafka.log.Log)",
		"[Log partition=__consumer_offsets-48, dir=/home1/irteam/apps/data/kafka/kafka-logs] Deleting segment files LogSegment(baseOffset=4338631022, size=641, lastModifiedTime=1711849197000, largestRecordTimestamp=Some(1711849197921)) (kafka.log.Log)",
		"Deleted log /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000004294790577.log.deleted. (kafka.log.LogSegment)",
		"Deleted log /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000004338631022.log.deleted. (kafka.log.LogSegment)",
		"Deleted offset index /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000004294790577.index.deleted. (kafka.log.LogSegment)",
		"Deleted offset index /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000004338631022.index.deleted. (kafka.log.LogSegment)",
		"Deleted time index /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000004294790577.timeindex.deleted. (kafka.log.LogSegment)",
		"Deleted time index /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000004338631022.timeindex.deleted. (kafka.log.LogSegment)",
		"[Log partition=__consumer_offsets-48, dir=/home1/irteam/apps/data/kafka/kafka-logs] Deleting segment files LogSegment(baseOffset=4339285360, size=104857589, lastModifiedTime=1711865580000, largestRecordTimestamp=Some(1711865580112)) (kafka.log.Log)",
		"Deleted log /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000004339285360.log.deleted. (kafka.log.LogSegment)",
		"Deleted offset index /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000004339285360.index.deleted. (kafka.log.LogSegment)",
		"Deleted time index /home1/irteam/apps/data/kafka/kafka-logs/__consumer_offsets-48/00000000004339285360.timeindex.deleted. (kafka.log.LogSegment)",
		"[Log partition=__consumer_offsets-49, dir=/home1/irteam/apps/data/kafka/kafka-logs] Rolled new log segment at offset 432939698 in 2 ms. (kafka.log.Log)",
	}

	ctx := context.Background()
	for _, log := range logs {
		_, _, template, _, err := miner.AddLogMessage(ctx, log)
		require.NoError(t, err)

		params := miner.ExtractParameters(template, log)
		require.NotNil(t, params)
	}

	clusters := miner.drain.GetClusters()
	require.Equal(t, 5, len(clusters))
}
