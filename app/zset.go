package main

import (
	"math/rand"
	"sync"
)

const (
	zsetSkipListMaxLevel = 32
	zsetSkipListP        = 0.25
)

type zskipistLevel struct {
	forward *zskiplistNode
	span    int
}

type zskiplistNode struct {
	member   string
	score    float64
	backward *zskiplistNode
	level    []zskipistLevel
}

func newZSkipListNode(level int, score float64, member string) *zskiplistNode {
	return &zskiplistNode{
		member: member,
		score:  score,
		level:  make([]zskipistLevel, level),
	}
}

type zskiplist struct {
	header *zskiplistNode
	tail   *zskiplistNode
	length int
	level  int
}

func newZSkipList() *zskiplist {
	header := newZSkipListNode(zsetSkipListMaxLevel, 0, "")
	return &zskiplist{
		header: header,
		level:  1,
	}
}

func randomLevel() int {
	lvl := 1
	for float64(rand.Int31()&0xffff) < (zsetSkipListP * 0xffff) {
		lvl++
		if lvl >= zsetSkipListMaxLevel {
			break
		}
	}
	return lvl
}

func (zsl *zskiplist) insert(score float64, member string) *zskiplistNode {

	update := make([]*zskiplistNode, zsetSkipListMaxLevel)
	rank := make([]int, zsetSkipListMaxLevel)

	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		if i == zsl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.member < member)) {

			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}

	lvl := randomLevel()
	if lvl > zsl.level {
		for i := zsl.level; i < lvl; i++ {

			rank[i] = 0
			update[i] = zsl.header
			update[i].level[i].span = zsl.length

		}
		zsl.level = lvl
	}

	x = newZSkipListNode(lvl, score, member)
	for i := 0; i < lvl; i++ {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x

		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	for i := lvl; i < zsl.level; i++ {
		update[i].level[i].span++
	}

	if update[0] == zsl.header {
		x.backward = nil
	} else {
		x.backward = update[0]
	}

	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		zsl.tail = x
	}

	zsl.length++

	return x
}

func (zsl *zskiplist) deleteNode(x *zskiplistNode, update []*zskiplistNode) {
	for i := 0; i < zsl.level; i++ {
		if update[i].level[i].forward == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].forward = x.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		zsl.tail = x.backward
	}
	for zsl.level > 1 && zsl.header.level[zsl.level-1].forward == nil {
		zsl.level--
	}
	zsl.length--
}

func (zsl *zskiplist) delete(score float64, member string) bool {

	update := make([]*zskiplistNode, zsetSkipListMaxLevel)
	x := zsl.header

	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.member < member)) {

			x = x.level[i].forward
		}
		update[i] = x
	}

	x = x.level[0].forward
	if x != nil && x.score == score && x.member == member {
		zsl.deleteNode(x, update)
		return true
	}

	return false

}

// ZSet combines a dictionary (for O(1) score lookup) and a skiplist (for ordered operations)
type ZSet struct {
	dict sync.Map
	sl   *zskiplist
}

func NewZSet() *ZSet {
	return &ZSet{
		sl: newZSkipList(),
	}
}

// Add inserts or updates a member in the sorted set
// Returns 1 if added, 0 if updated
func (zs *ZSet) Add(score float64, member string) int {

	if oldScoreVal, ok := zs.dict.Load(member); ok {
		oldScore := oldScoreVal.(float64)
		if oldScore == score {
			return 0
		}

		zs.sl.delete(oldScore, member)
		zs.sl.insert(score, member)
		zs.dict.Store(member, score)
		return 0
	}

	zs.dict.Store(member, score)
	zs.sl.insert(score, member)
	return 1

}

func (zsl *zskiplist) getRank(score float64, member string) int {

	rank := 0
	x := zsl.header

	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.member <= member)) {

			rank += x.level[i].span
			x = x.level[i].forward
		}

		if x != nil && x.member == member {
			return rank
		}
	}

	return 0
}

// Rank returns the 0-based rank of memeber in sorted set, or -1 if not found
func (zs *ZSet) Rank(member string) (int, bool) {

	val, exists := zs.dict.Load(member)
	if !exists {
		return -1, false
	}
	score := val.(float64)

	rank1Based := zs.sl.getRank(score, member)
	if rank1Based == 0 {
		return -1, false
	}

	return rank1Based - 1, true
}
