package drain3

import (
	"encoding/json"
	"errors"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/jaeyo/go-drain3/util"
	"strconv"
	"strings"
	"unicode"
)

type ClusterUpdateType int

const (
	ClusterUpdateTypeNone ClusterUpdateType = iota
	ClusterUpdateTypeCreated
	ClusterUpdateTypeTemplateChanged
)

type SearchStrategy int

const (
	SearchStrategyNever SearchStrategy = iota
	SearchStrategyFallback
	SearchStrategyAlways
)

type Drain struct {
	LogClusterDepth          int64
	MaxNodeDepth             int64
	SimTh                    float64
	MaxChildren              int64
	RootNode                 *Node
	MaxClusters              int
	ExtraDelimiters          []string
	ParamStr                 string
	ParametrizeNumericTokens bool

	IdToCluster     *lru.Cache[int64, *LogCluster] `json:"-"`
	ClustersCounter int64
}

type optionFn func(*Drain)

func WithDepth(depth int64) optionFn {
	return func(drain *Drain) {
		drain.LogClusterDepth = depth
	}
}

func WithSimTh(simTh float64) optionFn {
	return func(drain *Drain) {
		drain.SimTh = simTh
	}
}

func WithMaxChildren(maxChildren int64) optionFn {
	return func(drain *Drain) {
		drain.MaxChildren = maxChildren
	}
}

func WithMaxCluster(maxClusters int) optionFn {
	return func(drain *Drain) {
		drain.MaxClusters = maxClusters
	}
}

func WithExtraDelimiter(extraDelimiter []string) optionFn {
	return func(drain *Drain) {
		drain.ExtraDelimiters = extraDelimiter
	}
}

func NewDrain(options ...optionFn) (*Drain, error) {
	drain := &Drain{
		LogClusterDepth:          4,
		SimTh:                    0.4,
		MaxChildren:              100,
		RootNode:                 NewNode(),
		MaxClusters:              1000,
		ExtraDelimiters:          []string{},
		ParamStr:                 "<*>",
		ParametrizeNumericTokens: true,

		ClustersCounter: 0,
	}

	for _, option := range options {
		option(drain)
	}

	if drain.LogClusterDepth < 3 {
		return nil, errors.New("depth argument must be at least 3")
	}

	drain.MaxNodeDepth = drain.LogClusterDepth - 2 // max depth of a prefix tree node, starting from zero

	l, err := lru.New[int64, *LogCluster](drain.MaxClusters)
	if err != nil {
		return nil, fmt.Errorf("failed to create lru-cache: %w", err)
	}
	drain.IdToCluster = l

	return drain, nil
}

func (d *Drain) AddLogMessage(content string) (*LogCluster, ClusterUpdateType, error) {
	contentTokens := d.getContentAsTokens(content)

	matchCluster, err := d.treeSearch(d.RootNode, contentTokens, d.SimTh, false)
	if err != nil {
		return nil, ClusterUpdateTypeNone, fmt.Errorf("failed to tree search: %w", err)
	}

	updateType := ClusterUpdateTypeNone

	if matchCluster == nil {
		// match no existing log cluster
		d.ClustersCounter++
		clusterId := d.ClustersCounter
		matchCluster = NewLogCluster(clusterId, contentTokens)
		d.IdToCluster.Add(clusterId, matchCluster)
		d.addSeqToPrefixTree(d.RootNode, matchCluster)
		updateType = ClusterUpdateTypeCreated
	} else {
		// add the new log message to the existing cluster
		newTemplateTokens, err := d.createTemplate(contentTokens, matchCluster.LogTemplateTokens)
		if err != nil {
			return nil, ClusterUpdateTypeNone, fmt.Errorf("failed to create template: %w", err)
		}

		if util.IsSliceEqual(newTemplateTokens, matchCluster.LogTemplateTokens) {
			updateType = ClusterUpdateTypeNone
		} else {
			matchCluster.LogTemplateTokens = newTemplateTokens
			updateType = ClusterUpdateTypeTemplateChanged
		}

		matchCluster.Size++

		// touch cluster to update its state in the cache
		d.IdToCluster.Get(matchCluster.ClusterId)
	}

	return matchCluster, updateType, nil
}

func (d *Drain) getContentAsTokens(content string) []string {
	content = strings.TrimSpace(content)
	for _, delimiter := range d.ExtraDelimiters {
		content = strings.ReplaceAll(content, delimiter, " ")
	}
	return strings.Split(content, " ")
}

func (d *Drain) treeSearch(rootNode *Node, tokens []string, simTh float64, includeParams bool) (*LogCluster, error) {
	// at first level, children are grouped by token (word) count
	tokenCount := len(tokens)
	currentNode, exist := rootNode.KeyToChildNode[fmt.Sprint(tokenCount)]

	// no template with same token count yet
	if !exist {
		return nil, nil
	}

	// handle case of empty log string - return the single cluster in that group
	if tokenCount == 0 {
		logCluster, exist := d.IdToCluster.Get(currentNode.ClusterIds[0])
		if !exist {
			return nil, nil
		}
		return logCluster, nil
	}

	// find the leaf node for this log - a path of nodes matching the first N tokens (N=tree depth)
	currentNodeDepth := int64(1)
	for _, token := range tokens {
		// at max depth
		if currentNodeDepth >= d.MaxNodeDepth {
			break
		}

		// this is last token
		if currentNodeDepth == int64(tokenCount) {
			break
		}

		currentNode, exist = currentNode.KeyToChildNode[token]
		if !exist { // no exact next token exist, try wildcard node
			currentNode, exist = currentNode.KeyToChildNode[d.ParamStr]
		}
		if !exist { // no wildcard node exist
			return nil, nil
		}

		currentNodeDepth += 1
	}

	return d.fastMatch(currentNode.ClusterIds, tokens, simTh, includeParams)
}

func (d *Drain) fastMatch(clusterIds []int64, tokens []string, simTh float64, includeParams bool) (*LogCluster, error) {
	// find the best match for a log message (represented as tokens) verses a list of clusters
	// :param clusterIds: list of clusters to match against (represented by their IDs)
	// :param tokens: the log message, separated to tokens.
	// :param simTh: minimum required similarity threshold (nil will be returned in no clusters reached it)
	// :param includeParams: consider tokens matched to wildcard parameters in similarity threshold
	// :return: best match cluster or nil

	var matchCluster *LogCluster

	maxSim := float64(-1)
	maxParamCount := int64(-1)
	var maxCluster *LogCluster

	for _, clusterId := range clusterIds {
		// try to retrieve cluster from cache with bypassing eviction algorithm as we are only testing candidates for a match
		cluster, exist := d.IdToCluster.Get(clusterId)
		if !exist {
			continue
		}

		currentSim, paramCount, err := d.getSeqDistance(cluster.LogTemplateTokens, tokens, includeParams)
		if err != nil {
			return nil, fmt.Errorf("failed to get sequence distance: %w", err)
		}

		if currentSim > maxSim || (currentSim == maxSim && paramCount > maxParamCount) {
			maxSim = currentSim
			maxParamCount = paramCount
			maxCluster = cluster
		}
	}

	if maxSim >= simTh {
		matchCluster = maxCluster
	}

	return matchCluster, nil
}

func (d *Drain) getSeqDistance(seq1 []string, seq2 []string, includeParams bool) (float64, int64, error) {
	// seq1 is a template, seq2 is the log to match
	if len(seq1) != len(seq2) {
		return 0, 0, fmt.Errorf("seq1 length %d not equals to seq2 lengtrh %d", len(seq1), len(seq2))
	}

	// sequences are empty - full match
	if len(seq1) == 0 {
		return 1, 0, nil
	}

	simTokens := int64(0)
	paramCount := int64(0)

	for i := 0; i < len(seq1); i++ {
		token1 := seq1[i]
		token2 := seq2[i]

		if token1 == d.ParamStr {
			paramCount++
			continue
		}

		if token1 == token2 {
			simTokens++
		}
	}

	if includeParams {
		simTokens += paramCount
	}

	retVal := float64(simTokens) / float64(len(seq1))
	return retVal, paramCount, nil
}

func (d *Drain) addSeqToPrefixTree(rootNode *Node, cluster *LogCluster) {
	tokenCount := len(cluster.LogTemplateTokens)
	tokenCountStr := fmt.Sprint(tokenCount)
	firstLayerNode, exist := rootNode.KeyToChildNode[tokenCountStr]
	if !exist {
		firstLayerNode = NewNode()
		rootNode.KeyToChildNode[tokenCountStr] = firstLayerNode
	}

	currentNode := firstLayerNode

	// handle case of empty log string
	if tokenCount == 0 {
		currentNode.ClusterIds = []int64{cluster.ClusterId}
		return
	}

	currentDepth := int64(1)
	for _, token := range cluster.LogTemplateTokens {
		// if at max depth or this is last token in template - add current log cluster to the leaf node
		if currentDepth >= d.MaxNodeDepth || currentDepth >= int64(tokenCount) {
			// clean up stale clusters before adding a new one.
			newClusterIds := []int64{}
			for _, clusterId := range currentNode.ClusterIds {
				if _, exist := d.IdToCluster.Get(clusterId); exist {
					newClusterIds = append(newClusterIds, clusterId)
				}
			}
			newClusterIds = append(newClusterIds, cluster.ClusterId)
			currentNode.ClusterIds = newClusterIds
			break
		}

		// if token not matched in this layer of existing tree.
		if _, existForToken := currentNode.KeyToChildNode[token]; !existForToken {
			if d.ParametrizeNumericTokens && hasNumbers(token) {
				if node, exist := currentNode.KeyToChildNode[d.ParamStr]; !exist {
					newNode := NewNode()
					currentNode.KeyToChildNode[d.ParamStr] = newNode
					currentNode = newNode
				} else {
					currentNode = node
				}
			} else {
				if node, exist := currentNode.KeyToChildNode[d.ParamStr]; exist {
					if int64(len(currentNode.KeyToChildNode)) < d.MaxChildren {
						newNode := NewNode()
						currentNode.KeyToChildNode[token] = newNode
						currentNode = newNode
					} else {
						currentNode = node
					}
				} else {
					if int64(len(currentNode.KeyToChildNode)+1) < d.MaxChildren {
						newNode := NewNode()
						currentNode.KeyToChildNode[token] = newNode
						currentNode = newNode
					} else if int64(len(currentNode.KeyToChildNode)+1) == d.MaxChildren {
						newNode := NewNode()
						currentNode.KeyToChildNode[d.ParamStr] = newNode
						currentNode = newNode
					} else {
						currentNode = currentNode.KeyToChildNode[d.ParamStr]
					}
				}
			}
		} else {
			// if the token is matched
			currentNode = currentNode.KeyToChildNode[token]
		}

		currentDepth++
	}
}

func (d *Drain) createTemplate(seq1, seq2 []string) ([]string, error) {
	if len(seq1) != len(seq2) {
		return nil, fmt.Errorf("seq1 length %d not equals to seq2 length %d", len(seq1), len(seq2))
	}
	retVal := make([]string, len(seq2))
	copy(retVal, seq2)

	for i := 0; i < len(seq1); i++ {
		if seq1[i] != seq2[i] {
			retVal[i] = d.ParamStr
		}
	}

	return retVal, nil
}

func (d *Drain) Match(content string, strategy SearchStrategy) (*LogCluster, error) {
	// match log message against an already existing cluster.
	// match shall be perfect (sim_th=1.0)
	// new cluster will be created as a result of this call, nor any cluster modifications.

	// param content: log message to match
	// param strategy: when to perform full cluster search
	// (1) "never" is the fastest, will always perform a tree search [O(log(n))] but might produce false negatives (wrong mismatches) on some edge cases;
	// (2) "fallback" will perform a linear search [O(n)] among all clusters with the same token count, but only in case tree search found no match
	// it should not have false negatives, however tree-search may find a non-optimal match with more wildcard parameters than necessary;
	// (3) "always" is the slowest. it will select the best among all known clusters, be always evaluating all clusters with the same token count, and selecting the cluster with perfect all token match and least count of wildcard matches.
	// return: matched cluster of nil if no match found

	requiredSimTh := 1.0
	contentTokens := d.getContentAsTokens(content)

	// consider for future imporovement:
	// it is possible to implement a recursive tree_search (first try exact token match and fallback to wildcard match).
	// this will be both accurate and more efficient than the linear full search
	// also fast match can be optimized when exact match is required by early quitting on less than exact cluster matches.

	fullSearch := func() (*LogCluster, error) {
		allIds := d.getClustersIdsForSeqLen(len(contentTokens))
		cluster, err := d.fastMatch(allIds, contentTokens, requiredSimTh, true)
		if err != nil {
			return nil, fmt.Errorf("failed to fast match: %w", err)
		}

		return cluster, nil
	}

	if strategy == SearchStrategyAlways {
		return fullSearch()
	}

	matchCluster, err := d.treeSearch(d.RootNode, contentTokens, requiredSimTh, true)
	if err != nil {
		return nil, fmt.Errorf("failed to tree search: %w", err)
	} else if matchCluster != nil {
		return matchCluster, nil
	}

	if strategy == SearchStrategyNever {
		return nil, nil
	}

	return fullSearch()
}

func (d *Drain) getClustersIdsForSeqLen(seqLen int) []int64 {
	// return all clusters with the specified count of tokens

	var appendClusterRecursive func(node *Node, idListToFil *[]int64)
	appendClusterRecursive = func(node *Node, idListToFill *[]int64) {
		*idListToFill = append(*idListToFill, node.ClusterIds...)
		for _, childNode := range node.KeyToChildNode {
			appendClusterRecursive(childNode, idListToFill)
		}
	}

	currentNode, exist := d.RootNode.KeyToChildNode[strconv.Itoa(seqLen)]

	// no template with same token count
	if !exist {
		return []int64{}
	}

	target := []int64{}
	appendClusterRecursive(currentNode, &target)
	return target
}

func (d *Drain) GetClusters() []*LogCluster {
	return d.IdToCluster.Values()
}

func (d *Drain) PrintTree(maxClusters int) {
	d.printNode("root", d.RootNode, 0, maxClusters)
}

func (d *Drain) printNode(token string, node *Node, depth int, maxClusters int) {
	outStr := strings.Repeat("\t", depth)
	if depth == 0 {
		outStr += fmt.Sprintf("<%s>", token)
	} else if depth == 1 {
		outStr += fmt.Sprintf("<L=%s>", token)
	} else {
		outStr += fmt.Sprintf("\"%s\"", token)
	}

	if len(node.ClusterIds) > 0 {
		outStr += fmt.Sprintf(" (cluster_count=%d)", len(node.ClusterIds))
	}

	fmt.Println(outStr)

	for token, child := range node.KeyToChildNode {
		d.printNode(token, child, depth+1, maxClusters)
	}

	for _, clusterId := range node.ClusterIds[:min(len(node.ClusterIds), maxClusters)] {
		cluster, exist := d.IdToCluster.Get(clusterId)
		if !exist {
			continue
		}

		outStr := strings.Repeat("\t", depth+1) + fmt.Sprintf("%v", cluster)
		fmt.Println(outStr)
	}
}

func (d *Drain) MarshalJSON() ([]byte, error) {
	clusters := []*LogCluster{}
	clusters = append(clusters, d.IdToCluster.Values()...)

	return json.Marshal(&SerializableDrain{
		LogClusterDepth:          d.LogClusterDepth,
		MaxNodeDepth:             d.MaxNodeDepth,
		SimTh:                    d.SimTh,
		MaxChildren:              d.MaxChildren,
		RootNode:                 d.RootNode,
		MaxClusters:              d.MaxClusters,
		ExtraDelimiters:          d.ExtraDelimiters,
		ParamStr:                 d.ParamStr,
		ParametrizeNumericTokens: d.ParametrizeNumericTokens,

		Clusters:        clusters,
		ClustersCounter: d.ClustersCounter,
	})
}

func (d *Drain) UnmarshalJSON(data []byte) error {
	var forJson SerializableDrain
	err := json.Unmarshal(data, &forJson)
	if err != nil {
		return err
	}

	l, _ := lru.New[int64, *LogCluster](forJson.MaxClusters)
	for _, cluster := range forJson.Clusters {
		l.Add(cluster.ClusterId, cluster)
	}

	d.LogClusterDepth = forJson.LogClusterDepth
	d.MaxNodeDepth = forJson.MaxNodeDepth
	d.SimTh = forJson.SimTh
	d.MaxChildren = forJson.MaxChildren
	d.RootNode = forJson.RootNode
	d.MaxClusters = forJson.MaxClusters
	d.ExtraDelimiters = forJson.ExtraDelimiters
	d.ParamStr = forJson.ParamStr
	d.ParametrizeNumericTokens = forJson.ParametrizeNumericTokens
	d.IdToCluster = l
	d.ClustersCounter = forJson.ClustersCounter

	return nil
}

type SerializableDrain struct {
	LogClusterDepth          int64
	MaxNodeDepth             int64
	SimTh                    float64
	MaxChildren              int64
	RootNode                 *Node
	MaxClusters              int
	ExtraDelimiters          []string
	ParamStr                 string
	ParametrizeNumericTokens bool

	Clusters        []*LogCluster
	ClustersCounter int64
}

type Node struct {
	KeyToChildNode map[string]*Node
	ClusterIds     []int64
}

func NewNode() *Node {
	return &Node{
		KeyToChildNode: map[string]*Node{},
		ClusterIds:     []int64{},
	}
}

func hasNumbers(s string) bool {
	for _, char := range s {
		if unicode.IsDigit(char) {
			return true
		}
	}
	return false
}
