package drain3

import (
	"fmt"
	"strings"
	"testing"
)

func countTree(n *Node, d *Drain) (nodes, ids, stale int) {
	nodes = 1
	for _, id := range n.ClusterIds {
		ids++
		if _, ok := d.IdToCluster.Peek(id); !ok {
			stale++
		}
	}
	for _, child := range n.KeyToChildNode {
		a, b, c := countTree(child, d)
		nodes += a
		ids += b
		stale += c
	}
	return
}

func TestEvictionBoundsPrefixTree(t *testing.T) {
	d, err := NewDrain(WithMaxCluster(10), WithDepth(4), WithSimTh(1.0))
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 1000; i++ {
		if _, _, err := d.AddLogMessage("event " + strings.Repeat("word ", i)); err != nil {
			t.Fatal(err)
		}
	}
	nodes, ids, stale := countTree(d.RootNode, d)
	t.Logf("active=%d nodes=%d indexed_ids=%d stale_ids=%d", len(d.GetClusters()), nodes, ids, stale)
	if stale != 0 || nodes > 1+len(d.GetClusters())*int(d.MaxNodeDepth) {
		t.Fatalf("evicted clusters retain prefix-tree nodes: active=%d nodes=%d stale_ids=%d", len(d.GetClusters()), nodes, stale)
	}
}

func TestEvictedLargeMessagesDoNotRemainInTree(t *testing.T) {
	d, err := NewDrain(WithMaxCluster(10), WithDepth(4), WithSimTh(1.0))
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 100; i++ {
		// A real prefix token can keep the backing bytes of an entire old message alive.
		line := fmt.Sprintf("event %s%s", strings.Repeat("word ", i), strings.Repeat("x", 128*1024))
		if _, _, err := d.AddLogMessage(line); err != nil {
			t.Fatal(err)
		}
	}
	_, _, stale := countTree(d.RootNode, d)
	if stale != 0 {
		t.Fatalf("%d stale tree entries survive eviction", stale)
	}
}

func TestEvictionCleansSharedLeafAndKeepsLiveMatches(t *testing.T) {
	d, err := NewDrain(WithMaxCluster(3), WithDepth(4), WithSimTh(1.0))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{"event alpha", "event beta", "event gamma", "event delta", "event epsilon"} {
		if _, _, err := d.AddLogMessage(line); err != nil {
			t.Fatal(err)
		}
	}
	_, ids, stale := countTree(d.RootNode, d)
	if ids != 3 || stale != 0 {
		t.Fatalf("ids=%d stale=%d", ids, stale)
	}
	for _, line := range []string{"event gamma", "event delta", "event epsilon"} {
		if cluster, err := d.Match(line, SearchStrategyNever); err != nil || cluster == nil {
			t.Fatalf("lost live match %q: %v", line, err)
		}
	}
}

func TestSnapshotRestoresEvictionCleanup(t *testing.T) {
	d, err := NewDrain(WithMaxCluster(10), WithDepth(4), WithSimTh(1.0))
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 100; i++ {
		if _, _, err := d.AddLogMessage("event " + strings.Repeat("word ", i)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := d.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := NewDrain()
	if err != nil {
		t.Fatal(err)
	}
	if err = restored.UnmarshalJSON(snapshot); err != nil {
		t.Fatal(err)
	}
	for i := 101; i <= 200; i++ {
		if _, _, err := restored.AddLogMessage("event " + strings.Repeat("word ", i)); err != nil {
			t.Fatal(err)
		}
	}
	nodes, ids, stale := countTree(restored.RootNode, restored)
	if nodes > 21 || ids != 10 || stale != 0 {
		t.Fatalf("nodes=%d ids=%d stale=%d", nodes, ids, stale)
	}
}

func TestSnapshotPrunesLegacyStaleNodes(t *testing.T) {
	d, err := NewDrain(WithMaxCluster(2), WithDepth(4), WithSimTh(1.0))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = d.AddLogMessage("event alpha"); err != nil {
		t.Fatal(err)
	}
	legacyLeaf := NewNode()
	legacyLeaf.ClusterIds = []int64{99999}
	d.RootNode.KeyToChildNode["legacy-stale-token-count"] = legacyLeaf
	snapshot, err := d.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := NewDrain()
	if err != nil {
		t.Fatal(err)
	}
	if err = restored.UnmarshalJSON(snapshot); err != nil {
		t.Fatal(err)
	}
	if _, ok := restored.RootNode.KeyToChildNode["legacy-stale-token-count"]; ok {
		t.Fatal("legacy branch was retained")
	}
	_, _, stale := countTree(restored.RootNode, restored)
	if stale != 0 {
		t.Fatalf("stale=%d", stale)
	}
}

func TestEvictionPreservesInsertionLRUOrder(t *testing.T) {
	d, err := NewDrain(WithMaxCluster(3), WithDepth(4), WithSimTh(1.0))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{"shared alpha", "shared beta", "shared gamma", "other event"} {
		if _, _, err := d.AddLogMessage(line); err != nil {
			t.Fatal(err)
		}
	}
	// Inserting into a shared leaf has historically touched its existing
	// clusters. The third cluster is therefore evicted before the first two.
	for _, id := range []int64{1, 2, 4} {
		if !d.IdToCluster.Contains(id) {
			t.Fatalf("live cluster %d was evicted", id)
		}
	}
	if d.IdToCluster.Contains(3) {
		t.Fatal("expected the third cluster to be evicted")
	}
}

func TestPrunedExactPathAllowsWildcardFallback(t *testing.T) {
	d, err := NewDrain(WithMaxCluster(2), WithDepth(4), WithMaxChildren(2), WithSimTh(0.6))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{"alpha stable value", "beta stable value", "gamma stable value", "unrelated event"} {
		if _, _, err := d.AddLogMessage(line); err != nil {
			t.Fatal(err)
		}
	}
	if d.IdToCluster.Contains(1) {
		t.Fatal("the exact alpha cluster should have been evicted")
	}
	cluster, update, err := d.AddLogMessage("alpha stable value")
	if err != nil {
		t.Fatal(err)
	}
	// The removed exact path no longer shadows the live wildcard path.
	// Earlier versions retained that path and created a new specific cluster.
	if cluster.ClusterId != 2 || cluster.GetTemplate() != "<*> stable value" || update != ClusterUpdateTypeNone {
		t.Fatalf("expected existing wildcard cluster, got cluster=%v update=%v", cluster, update)
	}
}
