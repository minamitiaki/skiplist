/*
 * @Author: chenjingwei
 * @Date: 2020-04-20 11:45:00
 * @Last Modified by: chenjingwei
 * @Last Modified time: 2020-04-30 21:17:00
 * @Description: a trimmed version of redis skiplist, descend by value
 */

package util

import "fmt"

const (
	MaxSkipLevel  = 32
	SkipLevelProb = 0.25
)

type UserData interface {
	GetID() int64
	GetScore() uint64
}

type SkipLevel struct {
	forward *SkipNode
	span    int32 //到下一个节点的跨度
}

type SkipNode struct {
	k     UserData    //节点对象
	v     uint64      //节点分数
	back  *SkipNode   //后退指针
	level []SkipLevel //前进指针
}

func (self *SkipNode) Key() int64 {
	if self == nil {
		return 0
	}
	return self.k.GetID()
}

func (self *SkipNode) Value() UserData {
	if self == nil {
		return nil
	}
	return self.k
}

func (self *SkipNode) Iter(max int32) *Iter {
	return MakeIter(self, max)
}

type Iter struct {
	cur     *SkipNode
	tmp     *SkipNode
	counter int32
	max     int32
}

func (self *Iter) Count() int32 {
	if self == nil {
		return 0
	}
	return self.max
}

func (self *Iter) Next() bool {
	if self == nil {
		return false
	}

	if self.cur == nil {
		return false
	}

	if len(self.cur.level) == 0 {
		return false
	}

	if self.counter == self.max {
		return false
	}

	self.tmp = self.cur
	self.cur = self.cur.level[0].forward
	self.counter++
	return true
}

func (self *Iter) clearTmp() {
	self.tmp = nil
}

func (self *Iter) Scan() UserData {
	if self == nil {
		return nil
	}

	if self.tmp == nil {
		return nil
	}
	defer self.clearTmp()
	return self.tmp.k
}

func MakeIter(cur *SkipNode, max int32) *Iter {
	if cur == nil {
		return nil
	}

	rlt := &Iter{}
	rlt.cur = cur
	rlt.counter = 0
	rlt.max = max
	rlt.tmp = nil
	return rlt
}

type SkipList struct {
	header   *SkipNode
	tail     *SkipNode
	length   int32 //节点数量
	curLevel int32 //层数

	searchMap map[int64]uint64 //ID <--> v, 本不属于跳表,为了其它系统使用方便而引入的反查表
	//todo:自定义的比较函数
}

func (self *SkipList) GetTop() *Iter {
	return MakeIter(self.header.level[0].forward, self.length)
}

//内部函数：查找沿途的父节点与跨度
func (self *SkipList) find(v uint64, id int64) (*SkipNode, [MaxSkipLevel]*SkipNode, [MaxSkipLevel]int32) {
	var parent [MaxSkipLevel]*SkipNode
	var span [MaxSkipLevel]int32

	x := self.header
	for i := self.curLevel - 1; i >= 0; i-- {
		if i == self.curLevel-1 {
			span[i] = 0
		} else {
			span[i] = span[i+1]
		}
		for x.level[i].forward != nil {
			if x.level[i].forward.v < v {
				break
			}

			if x.level[i].forward.v == v && x.level[i].forward.k.GetID() >= id {
				break
			}
			span[i] += x.level[i].span
			x = x.level[i].forward
		}

		parent[i] = x
	}

	if x.level[0].forward != nil && x.level[0].forward.k.GetID() == id {
		return x.level[0].forward, parent, span
	}
	return nil, parent, span
}

func (self *SkipList) GetNodeByKey(id int64) *SkipNode {
	v, ok := self.searchMap[id]
	if !ok {
		return nil
	}

	r, _, _ := self.find(v, id)
	return r
}

//input: [min, max]
//output: descend by value
func (self *SkipList) GetNodesByScore(min uint64, max uint64) *Iter {
	if self.length == 0 {
		return nil
	}

	if min > max {
		min, max = max, min
	}

	_, maxParent, maxSpan := self.find(max, 0)
	span := self.length
	if min > 0 {
		_, _, minSpan := self.find(min-1, 0)
		span = minSpan[0]
	}

	first := maxParent[0].level[0].forward
	if first == nil {
		return nil
	}

	if first.v < min {
		return nil
	}

	return MakeIter(first, span-maxSpan[0])
}

func (self *SkipList) GetRankByKey(id int64) int32 {
	v, ok := self.searchMap[id]
	if !ok {
		return 0
	}

	r, _, span := self.find(v, id)
	if r == nil {
		return 0
	}

	return span[0] + 1
}

func (self *SkipList) GetNodeByRank(rank int32) *SkipNode {
	var span int32
	x := self.header
	for i := self.curLevel - 1; i >= 0; i-- {
		for ; x.level[i].forward != nil && (span+x.level[i].span) <= rank; x = x.level[i].forward {
			span += x.level[i].span
		}
		if span == rank {
			return x
		}
	}

	return nil
}

//input: [b, e]
//output: descend by value
func (self *SkipList) GetNodesByRank(b int32, e int32) *Iter {
	if b > e {
		b, e = e, b
	}

	if b < 1 {
		b = 1
	}

	x := self.GetNodeByRank(b)
	if x == nil {
		return nil
	}

	len := e - b + 1
	return MakeIter(x, len)
}

func (self *SkipList) Insert(k UserData) (*SkipNode, error) {
	ID := k.GetID()
	if ID <= 0 {
		return nil, fmt.Errorf("ID must bigger than zero %d", ID)
	}

	v := k.GetScore()
	r, update, rank := self.find(v, k.GetID())
	if r != nil {
		return nil, fmt.Errorf("already exist, can't insert %d", ID)
	}

	level := getRandomLevel()

	if level > self.curLevel {
		for i := self.curLevel; i < level; i++ {
			rank[i] = 0
			update[i] = self.header
			update[i].level[i].span = self.length
		}

		self.curLevel = level
	}

	n := createSkipNode(level, v, k)

	for i := 0; i < int(level); i++ {
		n.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = n
		n.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	for i := level; i < self.curLevel; i++ {
		update[i].level[i].span++
	}

	if update[0] == self.header {
		n.back = nil
	} else {
		n.back = update[0]
	}

	if n.level[0].forward != nil {
		n.level[0].forward.back = n
	} else {
		self.tail = n
	}

	self.length++

	self.searchMap[ID] = v

	return n, nil
}

func (self *SkipList) deleteNode(x *SkipNode, update [MaxSkipLevel]*SkipNode) {
	for i := 0; i < int(self.curLevel); i++ {
		if update[i].level[i].forward == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].forward = x.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}

	if x.level[0].forward != nil {
		x.level[0].forward.back = x.back
	} else {
		self.tail = x.back
	}

	for i := self.curLevel; i > 1; i-- {
		if self.header.level[i-1].forward == nil {
			self.curLevel--
		}
	}
	self.length--

	delete(self.searchMap, x.k.GetID())
}

func (self *SkipList) Delete(id int64) int32 {
	v, ok := self.searchMap[id]
	if !ok {
		return 0
	}

	r, update, _ := self.find(v, id)
	if r == nil {
		delete(self.searchMap, id)
		return 0
	}

	self.deleteNode(r, update)
	return 1
}

func (self *SkipList) Update(k UserData) error {
	self.Delete(k.GetID())
	_, err := self.Insert(k)
	return err
}

func (self *SkipList) Len() int32 {
	return self.length
}

func createSkipNode(level int32, v uint64, k UserData) *SkipNode {
	node := &SkipNode{}
	node.k = k
	node.v = v
	node.level = make([]SkipLevel, level)
	return node
}

func CreateSkipList() *SkipList {
	list := &SkipList{}
	list.curLevel = 1
	list.length = 0

	//初始化表头节点
	list.header = createSkipNode(MaxSkipLevel, 0, nil)
	list.header.back = nil

	list.tail = nil

	list.searchMap = make(map[int64]uint64)
	return list
}

func getRandomLevel() int32 {
	var level int32 = 1
	for {
		if level == MaxSkipLevel {
			break
		}
		if Randf() < SkipLevelProb {
			level++
		} else {
			break
		}
	}
	return level
}
