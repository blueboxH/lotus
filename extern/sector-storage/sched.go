package sectorstorage

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type schedPrioCtxKey int

var SchedPriorityKey schedPrioCtxKey
var DefaultSchedPriority = 0
var SelectorTimeout = 5 * time.Second
var InitWait = 3 * time.Second

// ============================= mod ===========================
var htSchedTasks = []sealtasks.TaskType{sealtasks.TTFinalize, sealtasks.TTCommit1, sealtasks.TTPreCommit2, sealtasks.TTPreCommit1} // 列表顺序不能打乱
var UnScheduling = make(map[abi.SectorNumber]struct{})                                                                             // 已添加却未调度给P worker 的列表
var APHTSets = make(map[abi.SectorNumber]struct{})                                                                                 // 已添加却未调度APHT的上去编号
// ============================= mod ===========================

var (
	SchedWindows = 2
)

func getPriority(ctx context.Context) int {
	sp := ctx.Value(SchedPriorityKey)
	if p, ok := sp.(int); ok {
		return p
	}

	return DefaultSchedPriority
}

func WithPriority(ctx context.Context, priority int) context.Context {
	return context.WithValue(ctx, SchedPriorityKey, priority)
}

const mib = 1 << 20

type WorkerAction func(ctx context.Context, w Worker) error

type WorkerSelector interface {
	Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, a *workerHandle) (bool, error) // true if worker is acceptable for performing a task

	Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) // true if a is preferred over b
}

type scheduler struct {
	spt abi.RegisteredSealProof

	workersLk sync.RWMutex
	workers   map[WorkerID]*workerHandle

	schedule       chan *workerRequest
	windowRequests chan *schedWindowRequest
	workerChange   chan struct{} // worker added / changed/freed resources
	workerDisable  chan workerDisableReq

	// owned by the sh.runSched goroutine
	schedQueue  *requestQueue
	openWindows []*schedWindowRequest
	htSchedMap  map[string]map[sealtasks.TaskType]map[abi.SectorID]*workerRequest
	workTracker *workTracker

	info chan func(interface{})

	closing  chan struct{}
	closed   chan struct{}
	testSync chan struct{} // used for testing
}

type workerHandle struct {
	workerRpc Worker

	info storiface.WorkerInfo

	preparing *activeResources
	active    *activeResources

	lk sync.Mutex

	wndLk         sync.Mutex
	activeWindows []*schedWindow

	enabled bool

	// for sync manager goroutine closing
	cleanupStarted bool
	closedMgr      chan struct{}
	closingMgr     chan struct{}
}

type schedWindowRequest struct {
	worker WorkerID

	done chan *schedWindow
}

type schedWindow struct {
	allocated activeResources
	todo      []*workerRequest
}

type workerDisableReq struct {
	activeWindows []*schedWindow
	wid           WorkerID
	done          func()
}

type activeResources struct {
	memUsedMin uint64
	memUsedMax uint64
	gpuUsed    bool
	cpuUse     uint64

	cond *sync.Cond
}

type workerRequest struct {
	sector   abi.SectorID
	taskType sealtasks.TaskType
	priority int // larger values more important
	sel      WorkerSelector

	prepare WorkerAction
	work    WorkerAction

	start time.Time

	index int // The index of the item in the heap.

	indexHeap int
	ret       chan<- workerResponse
	ctx       context.Context
}

type workerResponse struct {
	err error
}

func newScheduler(spt abi.RegisteredSealProof) *scheduler {
	InitRedis()
	return &scheduler{
		spt: spt,

		workers: map[WorkerID]*workerHandle{},

		schedule:       make(chan *workerRequest),
		windowRequests: make(chan *schedWindowRequest, 20),
		workerChange:   make(chan struct{}, 20),
		workerDisable:  make(chan workerDisableReq),

		schedQueue: &requestQueue{},

		workTracker: &workTracker{
			done:    map[storiface.CallID]struct{}{},
			running: map[storiface.CallID]trackedWork{},
		},
		htSchedMap: make(map[string]map[sealtasks.TaskType]map[abi.SectorID]*workerRequest),

		info: make(chan func(interface{})),

		closing: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (sh *scheduler) Schedule(ctx context.Context, sector abi.SectorID, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	select {
	case sh.schedule <- &workerRequest{
		sector:   sector,
		taskType: taskType,
		priority: getPriority(ctx),
		sel:      sel,

		prepare: prepare,
		work:    work,

		start: time.Now(),

		ret: ret,
		ctx: ctx,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp.err
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *workerRequest) respond(err error) {
	select {
	case r.ret <- workerResponse{err: err}:
	case <-r.ctx.Done():
		log.Warnf("request got cancelled before we could respond")
	}
}

type SchedDiagRequestInfo struct {
	Sector   abi.SectorID
	TaskType sealtasks.TaskType
	Priority int
}

// ==========================================      mod     ===================================
type workerState struct {
	Hostname       string
	CanDoNerSector bool // 决定会不会对这台机器分配 新任务, 为false 时候, 只能添加已经被这台机器处理过的任务
	HandingSectors workerSectorStates
}

// ==========================================      mod     ===================================
type SchedDiagInfo struct {
	Requests    []SchedDiagRequestInfo
	OpenWindows []string
	// ==========================================      mod     ===================================
	WorkerStates []*workerState
	HtShedMap    map[string]workerSectorStates
}

// ==========================================      mod     ===================================


func (sh *scheduler) runSched() {
	defer close(sh.closed)

	iw := time.After(InitWait)
	var initialised bool

	for {
		var doSched bool
		var toDisable []workerDisableReq

		select {
		case <-sh.workerChange:
			doSched = true
		case dreq := <-sh.workerDisable:
			toDisable = append(toDisable, dreq)
			doSched = true
		case req := <-sh.schedule:
			// ==========================================      mod     ===================================
			sh.pushWorkerRequest(req)
			// ==========================================      mod     ===================================
			doSched = true
			if sh.testSync != nil {
				sh.testSync <- struct{}{}
			}
		case req := <-sh.windowRequests:
			sh.openWindows = append(sh.openWindows, req)
			doSched = true
		case ireq := <-sh.info:
			ireq(sh.diag())

		case <-iw:
			initialised = true
			iw = nil
			doSched = true
		case <-sh.closing:
			sh.schedClose()
			return
		}

		if doSched && initialised {
			// First gather any pending tasks, so we go through the scheduling loop
			// once for every added task
		loop:
			for {
				select {
				case <-sh.workerChange:
				case dreq := <-sh.workerDisable:
					toDisable = append(toDisable, dreq)
				case req := <-sh.schedule:
					// ==========================================      mod     ===================================
					log.Debugf("================ get sector %s %s at loop", req.sector, req.taskType.Short())
					sh.pushWorkerRequest(req)
					// ==========================================      mod     ===================================

					if sh.testSync != nil {
						sh.testSync <- struct{}{}
					}
				case req := <-sh.windowRequests:
					sh.openWindows = append(sh.openWindows, req)
				default:
					break loop
				}
			}

			for _, req := range toDisable {
				for _, window := range req.activeWindows {
					for _, request := range window.todo {
						// ==========================================      mod     ===================================
						log.Debugf("================ get sector %s %s at disable", request.sector, request.taskType.Short())
						sh.pushWorkerRequest(req)
						// ==========================================      mod     ===================================

					}
				}

				openWindows := make([]*schedWindowRequest, 0, len(sh.openWindows))
				for _, window := range sh.openWindows {
					if window.worker != req.wid {
						openWindows = append(openWindows, window)
					}
				}
				sh.openWindows = openWindows

				sh.workersLk.Lock()
				sh.workers[req.wid].enabled = false
				sh.workersLk.Unlock()

				req.done()
			}

			sh.tryHtSched() // 由于3秒判断一次, 所以这里就不对两种类型进行判断
			sh.trySched()
		}

	}
}

// ==========================================      mod     ===================================
func (sh *scheduler) pushWorkerRequest(req *workerRequest) {

	// p1 p2 c1 且已经缓存过, 存入自己维护的map, c2 也自己维护
	cacheHostname := SchedulerHt.getSectorCache(req.sector.Number)
	htSched := false
	for _, task := range htSchedTasks {
		if task == req.taskType {
			htSched = true
			break
		}
	}

	if htSched && cacheHostname != "" {
		shedMap := sh.htSchedMap[cacheHostname]
		if shedMap == nil {
			shedMap = make(map[sealtasks.TaskType]map[abi.SectorID]*workerRequest)
			sh.htSchedMap[cacheHostname] = shedMap
		}
		taskMap := shedMap[req.taskType]
		if taskMap == nil {
			taskMap = make(map[abi.SectorID]*workerRequest)
			sh.htSchedMap[cacheHostname][req.taskType] = taskMap
		}
		taskMap[req.sector] = req
		log.Debugf("add sector %s %s to htSchedMap", req.sector, req.taskType.Short())

	} else {
		sh.schedQueue.Push(req)
		log.Debugf("add sector %s %s to schedQueue", req.sector, req.taskType.Short())
	}
}

// ==========================================      mod     ===================================

func (sh *scheduler) diag() SchedDiagInfo {
	var out SchedDiagInfo

	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ {
		task := (*sh.schedQueue)[sqi]

		out.Requests = append(out.Requests, SchedDiagRequestInfo{
			Sector:   task.sector,
			TaskType: task.taskType,
			Priority: task.priority,
		})
	}

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	for _, window := range sh.openWindows {
		out.OpenWindows = append(out.OpenWindows, uuid.UUID(window.worker).String())
	}
	// ==========================================      mod     ===================================
	set := SchedulerHt.getAllPSet()
	if len(set) > 0 {
		out.WorkerStates = make([]*workerState, len(set))
		i := 0
		for _, hostname := range set {
			state := workerState{
				Hostname:       hostname,
				CanDoNerSector: SchedulerHt.canDoNewSector(hostname),
				HandingSectors: SchedulerHt.getWorkerSectorStates(hostname),
			}
			out.WorkerStates[i] = &state
			i++
		}

	}
	out.HtShedMap = make(map[string]workerSectorStates)
	for hostname, schedMap := range sh.htSchedMap {
		state := make(workerSectorStates)
		out.HtShedMap[hostname] = state

		for taskType, taskMap := range schedMap {
			for sectorID, _ := range taskMap {
				state[sectorID.Number] = taskType.Short()
			}
		}
	}

	// ==========================================      mod     ===================================

	return out
}

func (sh *scheduler) trySched() {
	/*
		This assigns tasks to workers based on:
		- Task priority (achieved by handling sh.schedQueue in order, since it's already sorted by priority)
		- Worker resource availability
		- Task-specified worker preference (acceptableWindows array below sorted by this preference)
		- Window request age

		1. For each task in the schedQueue find windows which can handle them
		1.1. Create list of windows capable of handling a task
		1.2. Sort windows according to task selector preferences
		2. Going through schedQueue again, assign task to first acceptable window
		   with resources available
		3. Submit windows with scheduled tasks to workers

	*/

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	windows := make([]schedWindow, len(sh.openWindows))
	acceptableWindows := make([][]int, sh.schedQueue.Len())

	log.Debugf("SCHED %d queued; %d open windows", sh.schedQueue.Len(), len(windows))

	if len(sh.openWindows) == 0 {
		// nothing to schedule on
		return
	}

	// Step 1
	concurrency := len(sh.openWindows)
	throttle := make(chan struct{}, concurrency)

	var wg sync.WaitGroup
	wg.Add(sh.schedQueue.Len())

	for i := 0; i < sh.schedQueue.Len(); i++ {
		throttle <- struct{}{}

		go func(sqi int) {
			defer wg.Done()
			defer func() {
				<-throttle
			}()

			task := (*sh.schedQueue)[sqi]
			needRes := ResourceTable[task.taskType][sh.spt]

			task.indexHeap = sqi
			for wnd, windowRequest := range sh.openWindows {
				worker, ok := sh.workers[windowRequest.worker]
				if !ok {
					log.Errorf("worker referenced by windowRequest not found (worker: %s)", windowRequest.worker)
					// TODO: How to move forward here?
					continue
				}

				// ==========================================      mod     ===================================
				// 进行数量的控制
				if !SchedulerHt.filterMaxNum(worker.info.Hostname, task.sector, task.taskType) {
					continue
				}
				// ==========================================      mod     ===================================

				// TODO: allow bigger windows
				if !windows[wnd].allocated.canHandleRequest(needRes, windowRequest.worker, "schedAcceptable", worker.info.Resources) {
					continue
				}

				rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout)
				ok, err := task.sel.Ok(rpcCtx, task.taskType, sh.spt, worker)
				cancel()
				if err != nil {
					log.Errorf("trySched(1) req.sel.Ok error: %+v", err)
					continue
				}

				if !ok {
					continue
				}

				acceptableWindows[sqi] = append(acceptableWindows[sqi], wnd)
			}

			if len(acceptableWindows[sqi]) == 0 {
				return
			}

			// Pick best worker (shuffle in case some workers are equally as good)
			rand.Shuffle(len(acceptableWindows[sqi]), func(i, j int) {
				acceptableWindows[sqi][i], acceptableWindows[sqi][j] = acceptableWindows[sqi][j], acceptableWindows[sqi][i] // nolint:scopelint
			})
			sort.SliceStable(acceptableWindows[sqi], func(i, j int) bool {
				wii := sh.openWindows[acceptableWindows[sqi][i]].worker // nolint:scopelint
				wji := sh.openWindows[acceptableWindows[sqi][j]].worker // nolint:scopelint

				if wii == wji {
					// for the same worker prefer older windows
					return acceptableWindows[sqi][i] < acceptableWindows[sqi][j] // nolint:scopelint
				}

				wi := sh.workers[wii]
				wj := sh.workers[wji]

				rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout)
				defer cancel()

				r, err := task.sel.Cmp(rpcCtx, task.taskType, wi, wj)
				if err != nil {
					log.Error("selecting best worker: %s", err)
				}
				return r
			})
		}(i)
	}

	wg.Wait()

	//log.Debugf("SCHED windows: %+v", windows)
	log.Debugf("SCHED Acceptable win: %+v", acceptableWindows)

	// Step 2
	scheduled := 0

	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ {
		task := (*sh.schedQueue)[sqi]
		needRes := ResourceTable[task.taskType][sh.spt]

		selectedWindow := -1
		for _, wnd := range acceptableWindows[task.indexHeap] {
			wid := sh.openWindows[wnd].worker
			wr := sh.workers[wid].info.Resources

			log.Debugf("SCHED try assign sqi:%d sector %d to window %d", sqi, task.sector.Number, wnd)

			// TODO: allow bigger windows
			if !windows[wnd].allocated.canHandleRequest(needRes, wid, "schedAssign", wr) {
				continue
			}

			// ==========================================      mod     ===================================
			hostname := sh.workers[wid].info.Hostname
			if !SchedulerHt.filterMaxNum(hostname, task.sector, task.taskType) {
				continue
			}
			log.Infof("trySched SCHED ASSIGNED sector %d taskType %s to host %s", task.sector, task.taskType.Short(), hostname)
			SchedulerHt.afterScheduled(task.sector, task.taskType, hostname)
			// ==========================================      mod     ===================================
			windows[wnd].allocated.add(wr, needRes)
			// TODO: We probably want to re-sort acceptableWindows here based on new
			//  workerHandle.utilization + windows[wnd].allocated.utilization (workerHandle.utilization is used in all
			//  task selectors, but not in the same way, so need to figure out how to do that in a non-O(n^2 way), and
			//  without additional network roundtrips (O(n^2) could be avoided by turning acceptableWindows.[] into heaps))

			selectedWindow = wnd
			break
		}

		if selectedWindow < 0 {
			// all windows full
			continue
		}

		windows[selectedWindow].todo = append(windows[selectedWindow].todo, task)

		sh.schedQueue.Remove(sqi)
		sqi--
		scheduled++
	}

	// Step 3

	if scheduled == 0 {
		return
	}

	scheduledWindows := map[int]struct{}{}
	for wnd, window := range windows {
		if len(window.todo) == 0 {
			// Nothing scheduled here, keep the window open
			continue
		}

		scheduledWindows[wnd] = struct{}{}

		window := window // copy
		select {
		case sh.openWindows[wnd].done <- &window:
		default:
			log.Error("expected sh.openWindows[wnd].done to be buffered")
		}
	}

	// Rewrite sh.openWindows array, removing scheduled windows
	newOpenWindows := make([]*schedWindowRequest, 0, len(sh.openWindows)-len(scheduledWindows))
	for wnd, window := range sh.openWindows {
		if _, scheduled := scheduledWindows[wnd]; scheduled {
			// keep unscheduled windows open
			continue
		}

		newOpenWindows = append(newOpenWindows, window)
	}

	sh.openWindows = newOpenWindows
}

// ==========================================      mod     ===================================
func (sh *scheduler) tryHtSched() {

	finLock := Exists("/filecoin/finLock")

	var unSelectWindows []*schedWindowRequest
	// 遍历所有机器, 查看机器状态取出适合做的任务
	for _, openWindow := range sh.openWindows {
		// 遍历所有c1, 全部添加到任务队列
		worker, ok := sh.workers[openWindow.worker]
		if !ok {
			log.Errorf("worker referenced by windowRequest not found (worker: %d)", openWindow.worker)
			// TODO: How to move forward here?
			continue
		}
		hostname := worker.info.Hostname
		requestQueueMap := sh.htSchedMap[hostname]
		schedWindow := schedWindow{
			allocated: *worker.active,
		}

		for _, schedTask := range htSchedTasks {
			needRes := ResourceTable[schedTask][sh.spt]
			if len(requestQueueMap[schedTask]) <= 0 {
				continue
			}

			if schedTask == sealtasks.TTFinalize && finLock {
				log.Infof("TTFinalize Locked, skip it")
				continue
			}

			log.Debugf("start filter %s %s task, task len %d", hostname, schedTask.Short(), len(requestQueueMap[schedTask]))
			for sector, task := range requestQueueMap[schedTask] {
				// TODO: allow bigger windows
				if !schedWindow.allocated.canHandleRequest(needRes, openWindow.worker, "htschedAssign", worker.info.Resources) {
					continue
				}

				rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout)
				ok, err := task.sel.Ok(rpcCtx, task.taskType, sh.spt, worker)
				cancel()
				if err != nil {
					log.Errorf("tryHtSched req.sel.Ok error: %+v", err)
					continue
				}

				if !ok {
					continue
				}

				schedWindow.todo = append(schedWindow.todo, task)

				schedWindow.allocated.add(worker.info.Resources, needRes)
				log.Infof("worker %s after sched Resources %v, worker Resources %v", hostname, schedWindow.allocated, worker.active)
				delete(requestQueueMap[schedTask], sector)
				log.Infof("tryHtSched SCHED ASSIGNED sector %d taskType %s to host %s", sector, task.taskType.Short(), hostname)
				SchedulerHt.afterScheduled(task.sector, task.taskType, hostname)

				if schedTask == sealtasks.TTPreCommit2 {
					// 由于p2 并行, 一次取一个
					break
				}
			}

		}

		if len(schedWindow.todo) > 0 {
			select {
			case openWindow.done <- &schedWindow:
				log.Infof("host %s add %d task to window", hostname, len(schedWindow.todo))
			default:
				log.Error("expected sh.openWindows[wnd].done to be buffered")
			}
		} else {
			unSelectWindows = append(unSelectWindows, openWindow)
		}
	}

	sh.openWindows = unSelectWindows

}

// ==========================================      mod     ===================================


func (sh *scheduler) schedClose() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()
	log.Debugf("closing scheduler")

	for i, w := range sh.workers {
		sh.workerCleanup(i, w)
	}
}

func (sh *scheduler) Info(ctx context.Context) (interface{}, error) {
	ch := make(chan interface{}, 1)

	sh.info <- func(res interface{}) {
		ch <- res
	}

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (sh *scheduler) Close(ctx context.Context) error {
	close(sh.closing)
	select {
	case <-sh.closed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
