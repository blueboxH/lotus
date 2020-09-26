package sectorstorage

import (
	"errors"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/gomodule/redigo/redis"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

var RedisClient *redis.Pool
var sectorNumPerWorker int
var redisPrefix string
var workerSectorStatesRedisPrefix = "workerSectorStates:"
var workerDoingSectorRedisPrefix = "workerDoingSector:"
var SchedulerHt schedulerHt = schedulerHt{}
var DoingSectors map[abi.SectorNumber]sealtasks.TaskType = make(map[abi.SectorNumber]sealtasks.TaskType)

type workerSectorStates map[abi.SectorNumber]string

func InitRedis() {
	log.Debug("start init redis...")
	host, db := getRedisPath()
	auth := ""

	if RedisClient != nil {
		return
	}
	RedisClient = &redis.Pool{
		MaxIdle:     100,
		MaxActive:   4000,
		IdleTimeout: 180 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", host, redis.DialPassword(auth), redis.DialDatabase(db))
			if nil != err {
				log.Error("create redis pool error: %v", err)
				return nil, err
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

}

func IsConnError(err error) bool {
	var needNewConn bool

	if err == nil {
		return false
	}

	if err == io.EOF {
		needNewConn = true
	}
	if strings.Contains(err.Error(), "use of closed network connection") {
		needNewConn = true
	}
	if strings.Contains(err.Error(), "connect: connection refused") {
		needNewConn = true
	}
	return needNewConn
}

func getRedisPath() (string, int) {
	lotusPath := os.Getenv("LOTUS_REDIS_PATH")
	if lotusPath == "" {
		log.Fatal("env LOTUS_REDIS_PATH not found, example: 'export LOTUS_REDIS_PATH=192.168.14.13:6379-0-t06071-15' 0 -> db name, t06071 -> minerId, 15 -> max num")
	}
	s := strings.Split(lotusPath, "-")
	redisPrefix = s[2] + ":"
	sectorNumPerWorker, _ = strconv.Atoi(s[3])
	db, _ := strconv.Atoi(s[1])
	return s[0], db
}

func getRedisPrefix() string {
	if redisPrefix != "" {
		return redisPrefix
	}
	_, _ = getRedisPath()
	return redisPrefix
}

// 在pool加入TestOnBorrow方法来去除扫描坏连接
func redo(command string, opt ...interface{}) (interface{}, error) {

	rd := RedisClient.Get()
	defer rd.Close()

	var conn redis.Conn
	var err error
	var maxretry = 3
	var needNewConn bool

	resp, err := rd.Do(command, opt...)
	needNewConn = IsConnError(err)
	if needNewConn == false {
		return resp, err
	} else {
		conn, err = RedisClient.Dial()
	}

	for index := 0; index < maxretry; index++ {
		if conn == nil && index+1 > maxretry {
			return resp, err
		}
		if conn == nil {
			conn, err = RedisClient.Dial()
		}
		if err != nil {
			continue
		}

		resp, err := conn.Do(command, opt...)
		needNewConn = IsConnError(err)
		if needNewConn == false {
			return resp, err
		} else {
			conn, err = RedisClient.Dial()
		}
	}

	conn.Close()
	return "", errors.New("redis error")
}

type schedulerHt struct {
	//sectorNumPerWorker int
	//
	//workers map[string]*workerState
	//
	//sectorCache map[abi.SectorNumber]string  // 对 apht 以及 p1 阶段的 sector 所在的机器进行缓存
	//
	//sectorLastHost map[abi.SectorNumber]string // 任务上个阶段的 hostname
}

func (sh schedulerHt) getSectorNumPerWorker() int {
	return sectorNumPerWorker
}

func (sh schedulerHt) getWorkerMaxSectorNum(hostname string) int {
	if sh.getSectorNumPerWorker() == 0 {
		// 总开关关掉
		return 0
	}

	workerMaxSectorNum, err := redis.Int(redo("hget", getRedisPrefix()+"workerMaxSectorNum", hostname))
	if err != nil {
		return 0
	}

	return workerMaxSectorNum

}

func (sh schedulerHt) setWorkerMaxSectorNum(hostname string, num int) {
	_, err := redo("hset", getRedisPrefix()+"workerMaxSectorNum", hostname, num)
	if err != nil {
		log.Debug(err)
	}
}

func (sh schedulerHt) getWorkerSectorStates(hostname string) workerSectorStates {
	stringMap, err := redis.StringMap(redo("hgetall", getRedisPrefix()+workerSectorStatesRedisPrefix+hostname))
	states := make(workerSectorStates)
	if err == nil {
		for key, value := range stringMap {
			parseInt, err := strconv.ParseInt(key, 10, 64)
			if err != nil {
				log.Error("getWorkerSectorStates %s has error field %s", getRedisPrefix()+workerSectorStatesRedisPrefix+hostname, key)
				continue
			}
			states[abi.SectorNumber(parseInt)] = value
		}
	}

	return states
}

func (sh schedulerHt) getWorkerSectorLen(hostname string) int {
	stateLen, err := redis.Int(redo("HLEN", getRedisPrefix()+workerSectorStatesRedisPrefix+hostname))
	if err != nil {
		log.Debug(err)
		return 0
	}

	return stateLen
}

func (sh schedulerHt) getWorkerSectorState(hostname string, number abi.SectorNumber) string {
	taskType, err := redis.String(redo("hget", getRedisPrefix()+workerSectorStatesRedisPrefix+hostname, number))
	if err != nil {
		return ""
	}
	return taskType
}

func (sh schedulerHt) setWorkerSectorState(hostname string, number abi.SectorNumber, taskType sealtasks.TaskType, status string) {
	//_, err := redo("hset", getRedisPrefix()+workerSectorStatesRedisPrefix+hostname, number, taskType.Short())
	_, err := redo("hset", getRedisPrefix()+workerSectorStatesRedisPrefix+hostname, number, taskType.Short()+"-"+status) // todo
	if err != nil {
		log.Debug(err)
	}
}

func (sh schedulerHt) delWorkerSectorState(hostname string, number abi.SectorNumber) {
	_, err := redo("hdel", getRedisPrefix()+workerSectorStatesRedisPrefix+hostname, number)
	if err != nil {
		log.Debug(err)
	}
}

func (sh schedulerHt) GetWorkerDoingSector(taskType sealtasks.TaskType, number abi.SectorNumber) []byte {
	res, err := redis.Bytes(redo("hget", getRedisPrefix()+workerDoingSectorRedisPrefix+taskType.Short(), number))
	if err != nil {
		return []byte{}
	}
	return res
}

func (sh schedulerHt) setWorkerDoingSector(taskType sealtasks.TaskType, number abi.SectorNumber, res []byte) {
	_, err := redo("hset", getRedisPrefix()+workerDoingSectorRedisPrefix+taskType.Short(), number, res)
	if err != nil {
		log.Debug(err)
	}
}

func (sh schedulerHt) DelWorkerDoingSector(taskType sealtasks.TaskType, number abi.SectorNumber) {
	_, err := redo("hdel", getRedisPrefix()+workerDoingSectorRedisPrefix+taskType.Short(), number)
	if err != nil {
		log.Debug(err)
	}
}

func (sh schedulerHt) pSethave(hostname string) bool {
	have, err := redis.Bool(redo("SISMEMBER", getRedisPrefix()+"pWorker", hostname))
	if err != nil {
		log.Debug(err)
		return false
	}
	return have
}

func (sh schedulerHt) addToPSet(hostname string) {
	_, err := redo("SADD", getRedisPrefix()+"pWorker", hostname)
	if err != nil {
		log.Debug(err)
	}
}

func (sh schedulerHt) delPSet(hostname string) {
	_, err := redo("SREM", getRedisPrefix()+"pWorker", hostname)
	if err != nil {
		log.Debug(err)
	}
}

func (sh schedulerHt) getAllPSet() []string {
	values, err := redis.Strings(redo("SMEMBERS", getRedisPrefix()+"pWorker"))
	if err != nil {
		log.Debug(err)
		return []string{}
	}
	return values
}

func (sh schedulerHt) addToCSet(hostname string) {
	_, err := redo("SADD", getRedisPrefix()+"cWorker", hostname)
	if err != nil {
		log.Debug(err)
	}
}

func (sh schedulerHt) delCSet(hostname string) {
	_, err := redo("SREM", getRedisPrefix()+"cWorker", hostname)
	if err != nil {
		log.Debug(err)
	}
}

func (sh schedulerHt) AddToRSet(hostname string) {
	_, err := redo("SADD", getRedisPrefix()+"rWorker", hostname)
	if err != nil {
		log.Debug(err)
	}
}

func (sh schedulerHt) DelRSet(hostname string) {
	_, err := redo("SREM", getRedisPrefix()+"rWorker", hostname)
	if err != nil {
		log.Debug(err)
	}
}

func (sh schedulerHt) canDoNewSector(hostname string) bool {
	b, err := redis.Bool(redo("hget", getRedisPrefix()+"workers", hostname))
	if err != nil { // 默认返回true
		return true
	}
	return b
}

func (sh schedulerHt) setCanDoNewSector(hostname string, can bool) {
	_, err := redo("hset", getRedisPrefix()+"workers", hostname, can)
	if err != nil {
		log.Debug(err)
	}
}

func (sh schedulerHt) getSectorCache(sectorNumber abi.SectorNumber) string {
	hostname, err := redis.String(redo("hget", getRedisPrefix()+"sectorCache", sectorNumber))
	if err != nil {
		return ""
	}
	return hostname
}

func (sh schedulerHt) setSectorCache(sectorNumber abi.SectorNumber, hostname string) {
	_, err := redo("hset", getRedisPrefix()+"sectorCache", sectorNumber, hostname)
	if err != nil {
		log.Debug(err)
	}
}

func (sh schedulerHt) deleteSectorCache(sectorNumber abi.SectorNumber) {
	_, err := redo("hdel", getRedisPrefix()+"sectorCache", sectorNumber)
	if err != nil {
		log.Info(err)
	}
}

func (sh schedulerHt) getSectorLastHost(sectorNumber abi.SectorNumber) string {
	hostname, err := redis.String(redo("hget", getRedisPrefix()+"sectorLastHost", sectorNumber))
	if err != nil {
		log.Info(err)
		return ""
	}
	return hostname
}

func (sh schedulerHt) setSectorLastHost(sectorNumber abi.SectorNumber, hostname string) {
	_, err := redo("hset", getRedisPrefix()+"sectorLastHost", sectorNumber, hostname)
	if err != nil {
		log.Info(err)
	}
}

func (sh schedulerHt) deleteSectorLastHost(sectorNumber abi.SectorNumber) {
	_, err := redo("hdel", getRedisPrefix()+"sectorLastHost", sectorNumber)
	if err != nil {
		log.Info(err)
	}
}

//func (sh schedulerHt) SetTicketValue(value abi.SealRandomness, epoch abi.ChainEpoch) {
//	_, err := redo("hset", getRedisPrefix()+"ticketValue", value, epoch)
//	if err != nil {
//		log.Info(err)
//	}
//}
//
//func (sh schedulerHt) DelTicketValue(value abi.SealRandomness) {
//	_, err := redo("hdel", getRedisPrefix()+"ticketValue", value)
//	if err != nil {
//		log.Info(err)
//	}
//}
//
//func (sh schedulerHt) GetTicketValue(value abi.SealRandomness) abi.ChainEpoch {
//	epoch, err := redis.Int64(redo("hget", getRedisPrefix()+"ticketValue", value))
//	if err != nil {
//		log.Info(err)
//		return 0
//	}
//	return abi.ChainEpoch(epoch)
//}
//
func (sh schedulerHt) SetTicketValue(sectorNumber abi.SectorNumber, value []byte) {
	_, err := redo("hset", getRedisPrefix()+"ticketValue", sectorNumber, value)
	if err != nil {
		log.Info(err)
	}
}

func (sh schedulerHt) DelTicketValue(sectorNumber abi.SectorNumber) {
	_, err := redo("hdel", getRedisPrefix()+"ticketValue", sectorNumber)
	if err != nil {
		log.Info(err)
	}
}

func (sh schedulerHt) GetTicketValue(sectorNumber abi.SectorNumber) []byte {
	value, err := redis.Bytes(redo("hget", getRedisPrefix()+"ticketValue", sectorNumber))
	if err != nil {
		return []byte{}
	}
	return value
}

func (sh schedulerHt) filterMaxNum(hostname string, sector abi.SectorID, taskType sealtasks.TaskType) bool {

	if taskType != sealtasks.TTPreCommit1 && taskType != sealtasks.TTAddPieceHT {
		return true
	}

	if sh.getWorkerMaxSectorNum(hostname) <= 0 {
		return true
	}

	stateType := sh.getWorkerSectorState(hostname, sector.Number)
	if stateType != "" {
		log.Debugf("sector %s %s at worker %s get %s from HandingSectors, select this worker",
			sector, taskType.Short(), hostname, stateType)
		return true
	}

	canDoNewSector := sh.canDoNewSector(hostname)
	states := sh.getWorkerSectorStates(hostname)
	if canDoNewSector && sh.getSectorCache(sector.Number) == "" && sh.getWorkerSectorLen(hostname) < sh.getWorkerMaxSectorNum(hostname) {
		// 机器为 active 状态, 那么上次的任务已经全部完成, 那么可以添加 新 的任务 (没有被cache过)
		return true
	}

	log.Debugf("filter sector %s %s failed at worker %s maxSerctorNum %d, avtive %t, HandingSectors %v",
		sector, taskType.Short(), hostname, sh.getWorkerMaxSectorNum(hostname), canDoNewSector, states)
	return false
}

func (sh schedulerHt) afterTaskFinish(sector abi.SectorID, taskType sealtasks.TaskType, hostname string) {
	log.Infof("sector %s %s task done at host %s", sector, taskType.Short(), hostname)
	if (sealtasks.TTAddPieceHT == taskType || sealtasks.TTPreCommit1 == taskType || sealtasks.TTPreCommit2 == taskType) && sh.getWorkerMaxSectorNum(hostname) > 0 {
		SchedulerHt.setWorkerSectorState(hostname, sector.Number, taskType, "finish")
	}

	// sector cache
	if sealtasks.TTAddPieceHT == taskType { // apht 阶段完成加入 map
		log.Infof("cache log: sector %s %s => host %s", sector, taskType.Short(), hostname)
		sh.setSectorCache(sector.Number, hostname)
	}

	if sealtasks.TTFinalize == taskType {
		// FIL 阶段清除 map
		sh.deleteSectorCache(sector.Number)
		log.Infof("sector: %s delete sectorCache at FIN", sector)
	}

	if sealtasks.TTPreCommit2 == taskType && sh.getWorkerMaxSectorNum(hostname) > 0 {
		// c2 阶段更新机器任务数据
		sh.delWorkerSectorState(hostname, sector.Number)
		log.Debugf("AfterPC2 do: worker %s delete sector %s HandingSector", hostname, sector)
		if sh.getWorkerSectorLen(hostname) == 0 {
			log.Debugf("host %s worker active from %t to true", hostname, sh.canDoNewSector(hostname))
			sh.setCanDoNewSector(hostname, true)
		}

		// 智能 pledge
		todoNum := 0

		for _, host := range sh.getAllPSet() {
			if !sh.canDoNewSector(host) {
				continue
			}

			wtn := sh.getWorkerMaxSectorNum(host) - sh.getWorkerSectorLen(host)
			if wtn > 0 {
				todoNum = todoNum + wtn
				log.Debugf("worker %s has %d sector to pledge", host, wtn)
			}
		}

		if todoNum > len(UnScheduling) {
			schedNum := todoNum - len(UnScheduling)
			log.Infof("auto pledge %d sector, todo num %d, UnScheduling num %d", schedNum, todoNum, len(UnScheduling))
			_, err := redo("set", getRedisPrefix()+"plegeNum", schedNum)
			if err != nil {
				log.Errorf("cache plege num error: %v", err)
			}
		}
	}
}

func (sh schedulerHt) afterScheduled(sector abi.SectorID, taskType sealtasks.TaskType, hostname string) {

	// add current to state
	if (sealtasks.TTAddPieceHT == taskType || sealtasks.TTPreCommit1 == taskType || sealtasks.TTPreCommit2 == taskType) && sh.getWorkerMaxSectorNum(hostname) > 0 {

		sh.setWorkerSectorState(hostname, sector.Number, taskType, "scheduled")
		log.Debugf("afterScheduled do: worker %s add sector %s %s to HandingSector", hostname, sector, taskType.Short())
		if sh.getWorkerSectorLen(hostname) >= sh.getWorkerMaxSectorNum(hostname) {
			log.Debugf("host %s worker active from %t to false", hostname, sh.canDoNewSector(hostname))
			sh.setCanDoNewSector(hostname, false)
		}

		if sealtasks.TTPreCommit1 == taskType { // todo: 错误情况, 有没有更合理的处理方式
			lastTaskType := sh.getWorkerSectorState(hostname, sector.Number)
			if (sealtasks.TTPreCommit1.Short() == lastTaskType || sealtasks.TTPreCommit2.Short() == lastTaskType) && sh.getWorkerSectorLen(hostname) < sh.getWorkerMaxSectorNum(hostname) {
				log.Debugf("host %s worker active from %t to true, because of it last taskType is %s and handing sector %v", hostname, sh.canDoNewSector(hostname), lastTaskType, sh.getWorkerSectorStates(hostname))
				sh.setCanDoNewSector(hostname, true)
			}
		}

	}

	// 维护 已添加却未调度map

	if sealtasks.TTPreCommit1 == taskType || sealtasks.TTAddPieceHT == taskType { // p1 或者 apht 说明已经进入了调用
		delete(UnScheduling, sector.Number)
	}

	if sealtasks.TTPreCommit1 == taskType && sh.getSectorCache(sector.Number) == "" { // 这里针对官方交易, p1开始调度, 却没有cache, 加入map
		log.Infof("cache log: sector %s %s => host %s", sector, taskType.Short(), hostname)
		sh.setSectorCache(sector.Number, hostname)
	}
}

// worker 在开始做p1 p2 等耗时任务时候, 将任务类型值写入redis, 表示开始, 结束时删除此值, 另外, 将运行结果值写入redis
func isFinished(sector abi.SectorID, taskType sealtasks.TaskType) (cacheRes []byte, finish func(res []byte)) {
	hostname, _ := os.Hostname()
	SchedulerHt.setWorkerSectorState(hostname, sector.Number, taskType, "running")
	cacheRes = SchedulerHt.GetWorkerDoingSector(taskType, sector.Number)

	if len(cacheRes) > 0 {
		log.Infof("sector %s %s is done , will skip it", sector, taskType)
	}

	DoingSectors[sector.Number] = taskType

	return cacheRes, func(res []byte) {
		if len(res) > 0 {
			SchedulerHt.setWorkerDoingSector(taskType, sector.Number, res)
			log.Infof("sector %s %s finish, result cache to redis: %s", sector, taskType, res)
		}
		delete(DoingSectors, sector.Number)
		SchedulerHt.setWorkerSectorState(hostname, sector.Number, taskType, "finish")
	}
}

func Exists(path string) bool {
	_, err := os.Stat(path) //os.Stat获取文件信息
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}
