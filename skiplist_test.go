/*
 * @Author: chenjingwei
 * @Date: 2020-04-22 10:49:09
 * @Last Modified by: chenjingwei
 * @Last Modified time: 2020-04-22 15:34:01
 */

package util

import (
	//

	"testing"

	"gitlab.papasg.com/library/lib/hrglog"

	"github.com/stretchr/testify/assert"
)

type User struct {
	ID    int64
	Score uint64
}

func (self *User) GetID() int64 {
	return self.ID
}

func (self *User) GetScore() uint64 {
	return self.Score
}

//基础功能测试
func Test_Skiplist1(t *testing.T) {
	l := CreateSkipList()
	var user User
	user.ID = 1
	user.Score = 100
	_, _ = l.Insert(&user)
	rlt := l.GetTop()
	assert.Equal(t, true, rlt.Next())
	n := rlt.Scan()
	assert.Equal(t, int64(1), n.GetID())
	assert.Equal(t, uint64(100), n.GetScore())
	user.Score = 90
	_ = l.Update(&user)
	rlt = l.GetTop()
	assert.Equal(t, true, rlt.Next())
	n = rlt.Scan()
	assert.Equal(t, uint64(90), n.GetScore())
	l.Delete(1)
	assert.Equal(t, (*Iter)(nil), l.GetTop())
	var test *Iter
	assert.Equal(t, false, test.Next())
}

//API测试
func Test_Skiplist2(t *testing.T) {
	l := CreateSkipList()
	_, _ = l.Insert(&User{ID: 1, Score: 100})
	_, _ = l.Insert(&User{ID: 2, Score: 50})
	_, _ = l.Insert(&User{ID: 3, Score: 100})
	rlt := l.GetNodesByScore(50, 100)
	assert.Equal(t, true, rlt.Next())
	user := rlt.Scan()
	assert.Equal(t, int64(1), user.GetID())
	assert.Equal(t, true, rlt.Next())
	user = rlt.Scan()
	assert.Equal(t, int64(3), user.GetID())
	assert.Equal(t, true, rlt.Next())
	user = rlt.Scan()
	assert.Equal(t, int64(2), user.GetID())
	assert.Equal(t, false, rlt.Next())
	rlt = l.GetNodesByScore(0, 200)
	assert.Equal(t, true, rlt.Next())
	user = rlt.Scan()
	assert.Equal(t, int64(1), user.GetID())
	assert.Equal(t, true, rlt.Next())
	user = rlt.Scan()
	assert.Equal(t, int64(3), user.GetID())
	assert.Equal(t, true, rlt.Next())
	user = rlt.Scan()
	assert.Equal(t, int64(2), user.GetID())
	assert.Equal(t, false, rlt.Next())
	rlt = l.GetNodesByScore(120, 60)
	assert.Equal(t, true, rlt.Next())
	user = rlt.Scan()
	assert.Equal(t, int64(1), user.GetID())
	assert.Equal(t, true, rlt.Next())
	user = rlt.Scan()
	assert.Equal(t, int64(3), user.GetID())
	assert.Equal(t, false, rlt.Next())
	rlt = l.GetNodesByScore(30, 70)
	assert.Equal(t, true, rlt.Next())
	user = rlt.Scan()
	assert.Equal(t, int64(2), user.GetID())
	assert.Equal(t, false, rlt.Next())
	rlt = l.GetNodesByScore(30, 40)
	assert.Equal(t, false, rlt.Next())
	rlt = l.GetNodesByScore(120, 110)
	assert.Equal(t, false, rlt.Next())
}

func Test_Skiplist3(t *testing.T) {
	l := CreateSkipList()
	_, _ = l.Insert(&User{ID: 1, Score: 100})
	_, _ = l.Insert(&User{ID: 2, Score: 50})
	_, _ = l.Insert(&User{ID: 3, Score: 100})
	rlt := l.GetNodesByRank(1, l.Len())
	assert.Equal(t, true, rlt.Next())
	user := rlt.Scan()
	assert.Equal(t, int64(1), user.GetID())
	assert.Equal(t, true, rlt.Next())
	user = rlt.Scan()
	assert.Equal(t, int64(3), user.GetID())
	assert.Equal(t, true, rlt.Next())
	user = rlt.Scan()
	assert.Equal(t, int64(2), user.GetID())
	assert.Equal(t, false, rlt.Next())
	rlt = l.GetNodesByRank(1, 2)
	assert.Equal(t, true, rlt.Next())
	user = rlt.Scan()
	assert.Equal(t, int64(1), user.GetID())
	assert.Equal(t, true, rlt.Next())
	user = rlt.Scan()
	assert.Equal(t, int64(3), user.GetID())
	assert.Equal(t, false, rlt.Next())
	rlt = l.GetNodesByRank(2, 3)
	assert.Equal(t, true, rlt.Next())
	user = rlt.Scan()
	assert.Equal(t, int64(3), user.GetID())
	assert.Equal(t, true, rlt.Next())
	user = rlt.Scan()
	assert.Equal(t, int64(2), user.GetID())
	assert.Equal(t, false, rlt.Next())
	rlt = l.GetNodesByRank(2, 2)
	assert.Equal(t, true, rlt.Next())
	user = rlt.Scan()
	assert.Equal(t, int64(3), user.GetID())
	assert.Equal(t, false, rlt.Next())
	rlt = l.GetNodesByRank(2, 1)
	assert.Equal(t, true, rlt.Next())
	user = rlt.Scan()
	assert.Equal(t, int64(1), user.GetID())
	assert.Equal(t, true, rlt.Next())
	user = rlt.Scan()
	assert.Equal(t, int64(3), user.GetID())
	assert.Equal(t, false, rlt.Next())
}

func Test_Skiplist4(t *testing.T) {
	l := CreateSkipList()
	_, _ = l.Insert(&User{ID: 1, Score: 100})
	_, _ = l.Insert(&User{ID: 2, Score: 50})
	_, _ = l.Insert(&User{ID: 3, Score: 100})
	_, _ = l.Insert(&User{ID: 4, Score: 70})
	_, _ = l.Insert(&User{ID: 5, Score: 30})
	rlt := l.GetRankByKey(2)
	assert.Equal(t, int32(4), rlt)
}

func Test_Skiplist5(t *testing.T) {
	l := CreateSkipList()
	_, _ = l.Insert(&User{ID: 1, Score: 100})
	_, _ = l.Insert(&User{ID: 2, Score: 50})
	_, _ = l.Insert(&User{ID: 3, Score: 100})
	_, _ = l.Insert(&User{ID: 4, Score: 70})
	_, _ = l.Insert(&User{ID: 5, Score: 30})
	rlt := l.GetNodeByKey(4)
	assert.Equal(t, uint64(70), rlt.v)
	rlt = l.GetNodeByRank(4)
	assert.Equal(t, uint64(50), rlt.v)
}

//性能测试 单纯插入10w次:0.07秒
func Test_Skiplist6(t *testing.T) {
	l := CreateSkipList()
	total := 100000
	for i := 1; i <= total; i++ {
		_, _ = l.Insert(&User{ID: int64(i), Score: uint64(total - i)})
	}
	assert.Equal(t, int32(total), l.length)

	n := l.GetTop()
	count := 0
	for n.Next() {
		user := n.Scan()
		count++
		expect := uint64(total - count)
		assert.Equal(t, expect, user.GetScore())
		assert.Equal(t, expect, l.searchMap[user.GetID()])
	}
	hrglog.Infof("level:%d", l.curLevel)
}

//性能测试 单纯更新10w次:0.09(0.16 - 0.07)秒
func Test_Skiplist7(t *testing.T) {
	l := CreateSkipList()
	total := 100000
	for i := 1; i <= total; i++ {
		_, _ = l.Insert(&User{ID: int64(i), Score: uint64(total - i)})
	}
	for i := 1; i <= total; i++ {
		_ = l.Update(&User{ID: int64(i), Score: uint64(i)})
	}
	assert.Equal(t, int32(total), l.length)
	n := l.GetTop()
	count := total
	for n.Next() {
		user := n.Scan()
		assert.Equal(t, int64(count), user.GetID())
		count--
	}
	hrglog.Infof("level:%d", l.curLevel)
}

//性能测试 单纯查找10w次:0.05(0.12 - 0.07)秒
func Test_Skiplist8(t *testing.T) {
	l := CreateSkipList()
	total := 100000
	for i := 1; i <= total; i++ {
		_, _ = l.Insert(&User{ID: int64(i), Score: uint64(total - i)})
	}
	assert.Equal(t, int32(total), l.length)
	for i := 1; i <= total; i++ {
		rlt := l.GetNodeByKey(int64(i))
		assert.Equal(t, int64(i), rlt.k.GetID())
	}
	hrglog.Infof("level:%d", l.curLevel)
}
