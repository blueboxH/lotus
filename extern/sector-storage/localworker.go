package sectorstorage

import (
	"bytes"
	"context"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/elastic/go-sysinfo"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-state-types/abi"
	storage2 "github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

var pathTypes = []stores.SectorFileType{stores.FTUnsealed, stores.FTSealed, stores.FTCache}

type WorkerConfig struct {
	SealProof abi.RegisteredSealProof
	TaskTypes []sealtasks.TaskType
}

type LocalWorker struct {
	scfg       *ffiwrapper.Config
	storage    stores.Store
	localStore *stores.Local
	sindex     stores.SectorIndex

	acceptTasks map[sealtasks.TaskType]struct{}
}

func NewLocalWorker(wcfg WorkerConfig, store stores.Store, local *stores.Local, sindex stores.SectorIndex) *LocalWorker {
	acceptTasks := map[sealtasks.TaskType]struct{}{}
	for _, taskType := range wcfg.TaskTypes {
		acceptTasks[taskType] = struct{}{}
	}

	return &LocalWorker{
		scfg: &ffiwrapper.Config{
			SealProofType: wcfg.SealProof,
		},
		storage:    store,
		localStore: local,
		sindex:     sindex,

		acceptTasks: acceptTasks,
	}
}

type localWorkerPathProvider struct {
	w  *LocalWorker
	op stores.AcquireMode
}

func (l *localWorkerPathProvider) AcquireSector(ctx context.Context, sector abi.SectorID, existing stores.SectorFileType, allocate stores.SectorFileType, sealing stores.PathType) (stores.SectorPaths, func(), error) {

	paths, storageIDs, err := l.w.storage.AcquireSector(ctx, sector, l.w.scfg.SealProofType, existing, allocate, sealing, l.op)
	if err != nil {
		return stores.SectorPaths{}, nil, err
	}

	releaseStorage, err := l.w.localStore.Reserve(ctx, sector, l.w.scfg.SealProofType, allocate, storageIDs, stores.FSOverheadSeal)
	if err != nil {
		return stores.SectorPaths{}, nil, xerrors.Errorf("reserving storage space: %w", err)
	}

	log.Debugf("acquired sector %d (e:%d; a:%d): %v", sector, existing, allocate, paths)

	return paths, func() {
		releaseStorage()

		for _, fileType := range pathTypes {
			if fileType&allocate == 0 {
				continue
			}

			sid := stores.PathByType(storageIDs, fileType)

			if err := l.w.sindex.StorageDeclareSector(ctx, stores.ID(sid), sector, fileType, l.op == stores.AcquireMove); err != nil {
				log.Errorf("declare sector error: %+v", err)
			}
		}
	}, nil
}

func (l *LocalWorker) sb() (ffiwrapper.Storage, error) {
	return ffiwrapper.New(&localWorkerPathProvider{w: l}, l.scfg)
}

func (l *LocalWorker) NewSector(ctx context.Context, sector abi.SectorID) error {
	sb, err := l.sb()
	if err != nil {
		return err
	}

	return sb.NewSector(ctx, sector)
}

func (l *LocalWorker) AddPiece(ctx context.Context, sector abi.SectorID, epcs []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, r io.Reader) (abi.PieceInfo, error) {
	sb, err := l.sb()
	if err != nil {
		return abi.PieceInfo{}, err
	}

	return sb.AddPiece(ctx, sector, epcs, sz, r)
}

func (l *LocalWorker) Fetch(ctx context.Context, sector abi.SectorID, fileType stores.SectorFileType, ptype stores.PathType, am stores.AcquireMode) error {
	_, done, err := (&localWorkerPathProvider{w: l, op: am}).AcquireSector(ctx, sector, fileType, stores.FTNone, ptype)
	if err != nil {
		return err
	}
	done()
	return nil
}

func (l *LocalWorker) SealPreCommit1(ctx context.Context, sector abi.SectorID, ticket abi.SealRandomness, pieces []abi.PieceInfo) (out storage2.PreCommit1Out, err error) {
	// ============================= mod ===========================
	sep := []byte("--")
	cacheRes, finish := isFinished(sector, sealtasks.TTPreCommit1)
	SchedulerHt.SetTicketValue(sector.Number, ticket)
	defer func() {
		SchedulerHt.DelTicketValue(sector.Number)
	}()
	if len(cacheRes) > 0 {
		split := bytes.Split(cacheRes, sep)
		if len(split) == 3 { // 有3个的时候,
			return split[2], nil
		} else {
			log.Infof("sector %s %s get cacheRes from redis decode error, redo it: %s", sector, sealtasks.TTPreCommit1.Short(), cacheRes)
		}
	}
	// ============================= mod ===========================
	{
		// cleanup previous failed attempts if they exist
		if err := l.storage.Remove(ctx, sector, stores.FTSealed, true); err != nil {
			return nil, xerrors.Errorf("cleaning up sealed data: %w", err)
		}

		if err := l.storage.Remove(ctx, sector, stores.FTCache, true); err != nil {
			return nil, xerrors.Errorf("cleaning up cache data: %w", err)
		}
	}

	sb, err := l.sb()
	if err != nil {
		return nil, err
	}

	// ============================= mod ===========================
	out, err = sb.SealPreCommit1(ctx, sector, ticket, pieces)
	time.Sleep(time.Duration(1) * time.Minute) // todo delete
	defer finish(bytes.Join([][]byte{out, ticket}, sep))

	return out, err
	// ============================= mod ===========================
}

func (l *LocalWorker) SealPreCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage2.PreCommit1Out) (cids storage2.SectorCids, err error) {
	// ============================= mod ===========================
	sep := "--"

	cacheRes, finish := isFinished(sector, sealtasks.TTPreCommit2)
	if len(cacheRes) > 0 {
		split := strings.Split(string(cacheRes), sep)
		//split := bytes.Split(cacheRes, sep)
		log.Infof("sector %s PC2 split %v", sector, split)
		if len(split) == 2 {
			unsealed, err1 := cid.Decode(split[0])
			sealed, err2 := cid.Decode(split[1])
			if err1 == nil && err2 == nil {
				return storage2.SectorCids{Unsealed: unsealed, Sealed: sealed}, nil
			} else {
				log.Infof("sector %s %s get cacheRes from redis decode cid error, redo it: %s, err1: %v, err2 %v", sector, sealtasks.TTPreCommit2.Short(), cacheRes, err1, err2)
			}
		} else {
			log.Infof("sector %s %s get cacheRes from redis decode error, redo it: %s", sector, sealtasks.TTPreCommit2.Short(), cacheRes)
		}
	}
	// ============================= mod ===========================
	sb, err := l.sb()
	if err != nil {
		return storage2.SectorCids{}, err
	}

	// ============================= mod ===========================
	cids, err = sb.SealPreCommit2(ctx, sector, phase1Out)
	time.Sleep(time.Duration(1) * time.Minute) // todo delete
	var cache string
	if err == nil {
		cache = cids.Unsealed.String() + sep + cids.Sealed.String()
		//cache = strings.Join([][]byte{cids.Unsealed.Bytes(), cids.Sealed.Bytes()}, sep)
		log.Infof("sector %s PC2 cache %v", sector, cache)
	}
	defer finish([]byte(cache))
	return cids, err
	// ============================= mod ===========================
}

func (l *LocalWorker) SealCommit1(ctx context.Context, sector abi.SectorID, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage2.SectorCids) (output storage2.Commit1Out, err error) {
	sb, err := l.sb()
	if err != nil {
		return nil, err
	}

	return sb.SealCommit1(ctx, sector, ticket, seed, pieces, cids)
}

func (l *LocalWorker) SealCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage2.Commit1Out) (proof storage2.Proof, err error) {
	// ============================= mod ===========================
	cacheRes, finish := isFinished(sector, sealtasks.TTCommit2)
	if len(cacheRes) > 0 {
		return cacheRes, nil
	}
	// ============================= mod ===========================
	sb, err := l.sb()
	if err != nil {
		return nil, err
	}

	// ============================= mod ===========================
	proof, err = sb.SealCommit2(ctx, sector, phase1Out)
	time.Sleep(time.Duration(1) * time.Minute) // todo delete
	defer finish(proof)
	return proof, err
	// ============================= mod ===========================
}

func (l *LocalWorker) FinalizeSector(ctx context.Context, sector abi.SectorID, keepUnsealed []storage2.Range) error {
	sb, err := l.sb()
	if err != nil {
		return err
	}

	if err := sb.FinalizeSector(ctx, sector, keepUnsealed); err != nil {
		return xerrors.Errorf("finalizing sector: %w", err)
	}

	if len(keepUnsealed) == 0 {
		if err := l.storage.Remove(ctx, sector, stores.FTUnsealed, true); err != nil {
			return xerrors.Errorf("removing unsealed data: %w", err)
		}
	}
	// ============================= mod ===========================
	unsealed := stores.FTNone
	if len(keepUnsealed) != 0 {
		unsealed = stores.FTUnsealed
	}
	return l.localStore.SendSectorToMiner(ctx, sector, l.scfg.SealProofType, stores.FTCache|stores.FTSealed|unsealed)
	// ============================= mod ===========================
}

func (l *LocalWorker) ReleaseUnsealed(ctx context.Context, sector abi.SectorID, safeToFree []storage2.Range) error {
	return xerrors.Errorf("implement me")
}

func (l *LocalWorker) Remove(ctx context.Context, sector abi.SectorID) error {
	var err error

	if rerr := l.storage.Remove(ctx, sector, stores.FTSealed, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (sealed): %w", rerr))
	}
	if rerr := l.storage.Remove(ctx, sector, stores.FTCache, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (cache): %w", rerr))
	}
	if rerr := l.storage.Remove(ctx, sector, stores.FTUnsealed, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (unsealed): %w", rerr))
	}

	return err
}

func (l *LocalWorker) MoveStorage(ctx context.Context, sector abi.SectorID, types stores.SectorFileType) error {
	if err := l.storage.MoveStorage(ctx, sector, l.scfg.SealProofType, types); err != nil {
		return xerrors.Errorf("moving sealed data to storage: %w", err)
	}

	return nil
}

func (l *LocalWorker) UnsealPiece(ctx context.Context, sector abi.SectorID, index storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize, randomness abi.SealRandomness, cid cid.Cid) error {
	sb, err := l.sb()
	if err != nil {
		return err
	}

	if err := sb.UnsealPiece(ctx, sector, index, size, randomness, cid); err != nil {
		return xerrors.Errorf("unsealing sector: %w", err)
	}

	if err := l.storage.RemoveCopies(ctx, sector, stores.FTSealed); err != nil {
		return xerrors.Errorf("removing source data: %w", err)
	}

	if err := l.storage.RemoveCopies(ctx, sector, stores.FTCache); err != nil {
		return xerrors.Errorf("removing source data: %w", err)
	}

	return nil
}

func (l *LocalWorker) ReadPiece(ctx context.Context, writer io.Writer, sector abi.SectorID, index storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize) (bool, error) {
	sb, err := l.sb()
	if err != nil {
		return false, err
	}

	return sb.ReadPiece(ctx, writer, sector, index, size)
}

func (l *LocalWorker) TaskTypes(context.Context) (map[sealtasks.TaskType]struct{}, error) {
	return l.acceptTasks, nil
}

func (l *LocalWorker) Paths(ctx context.Context) ([]stores.StoragePath, error) {
	return l.localStore.Local(ctx)
}

func (l *LocalWorker) Info(context.Context) (storiface.WorkerInfo, error) {
	hostname, err := os.Hostname() // TODO: allow overriding from config
	if err != nil {
		panic(err)
	}

	gpus, err := ffi.GetGPUDevices()
	if err != nil {
		log.Errorf("getting gpu devices failed: %+v", err)
	}

	h, err := sysinfo.Host()
	if err != nil {
		return storiface.WorkerInfo{}, xerrors.Errorf("getting host info: %w", err)
	}

	mem, err := h.Memory()
	if err != nil {
		return storiface.WorkerInfo{}, xerrors.Errorf("getting memory info: %w", err)
	}

	return storiface.WorkerInfo{
		Hostname: hostname,
		Resources: storiface.WorkerResources{
			MemPhysical: mem.Total,
			MemSwap:     mem.VirtualTotal,
			MemReserved: mem.VirtualUsed + mem.Total - mem.Available, // TODO: sub this process
			CPUs:        uint64(runtime.NumCPU()),
			GPUs:        gpus,
		},
	}, nil
}

func (l *LocalWorker) Closing(ctx context.Context) (<-chan struct{}, error) {
	return make(chan struct{}), nil
}

func (l *LocalWorker) Close() error {
	return nil
}

var _ Worker = &LocalWorker{}
