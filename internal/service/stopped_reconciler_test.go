package service

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-storage-provider/internal/db"
)

type memoryContractDB struct {
	mx   sync.Mutex
	bags map[string]db.StoredBag
}

func newMemoryContractDB(bags ...db.StoredBag) *memoryContractDB {
	m := &memoryContractDB{bags: make(map[string]db.StoredBag, len(bags))}
	for _, bag := range bags {
		m.bags[bag.ContractAddr] = cloneStoredBag(bag)
	}
	return m
}

func (m *memoryContractDB) SetContract(bag db.StoredBag) error {
	m.mx.Lock()
	defer m.mx.Unlock()
	m.bags[bag.ContractAddr] = cloneStoredBag(bag)
	return nil
}

func (m *memoryContractDB) GetContract(addr string) (db.StoredBag, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	bag, ok := m.bags[addr]
	if !ok {
		return db.StoredBag{}, db.ErrNotFound
	}
	return cloneStoredBag(bag), nil
}

func (m *memoryContractDB) ListContracts() ([]db.StoredBag, error) {
	m.mx.Lock()
	defer m.mx.Unlock()
	bags := make([]db.StoredBag, 0, len(m.bags))
	for _, bag := range m.bags {
		bags = append(bags, cloneStoredBag(bag))
	}
	return bags, nil
}

func cloneStoredBag(bag db.StoredBag) db.StoredBag {
	bag.BagID = append([]byte(nil), bag.BagID...)
	if bag.ContractInfo != nil {
		info := *bag.ContractInfo
		bag.ContractInfo = &info
	}
	return bag
}

func testContractAddr(seed byte) string {
	data := make([]byte, 32)
	data[len(data)-1] = seed
	return address.NewAddress(0, 0, data).String()
}

func TestReserveContract_AtomicallyReservesCapacity(t *testing.T) {
	activeAddr := testContractAddr(1)
	firstAddr := testContractAddr(2)
	secondAddr := testContractAddr(3)
	xdb := newMemoryContractDB(
		db.StoredBag{ContractAddr: activeAddr, Status: db.StoredBagStatusActive, Size: 40},
		db.StoredBag{ContractAddr: firstAddr, Status: db.StoredBagStatusStopped},
		db.StoredBag{ContractAddr: secondAddr, Status: db.StoredBagStatusStopped},
	)
	svc := &Service{db: xdb, spaceAllocated: 100}

	type result struct {
		started bool
		err     error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for i, addr := range []string{firstAddr, secondAddr} {
		i, addr := i, addr
		go func() {
			<-start
			_, started, err := svc.reserveContract(addr, []byte{byte(i + 1)}, 60, &db.ContractInfo{})
			results <- result{started: started, err: err}
		}()
	}
	close(start)

	started := 0
	noSpace := 0
	for range 2 {
		res := <-results
		if res.started {
			started++
		}
		if errors.Is(res.err, ErrNoSpace) {
			noSpace++
		} else if res.err != nil {
			t.Fatalf("unexpected reserve error: %v", res.err)
		}
	}

	if started != 1 || noSpace != 1 {
		t.Fatalf("expected one reservation and one no-space result, got started=%d no_space=%d", started, noSpace)
	}

	bags, err := xdb.ListContracts()
	if err != nil {
		t.Fatal(err)
	}
	var reserved uint64
	for _, bag := range bags {
		if bag.Status == db.StoredBagStatusActive || bag.Status == db.StoredBagStatusAdded {
			reserved += bag.Size
		}
	}
	if reserved != 100 {
		t.Fatalf("expected exactly 100 bytes reserved, got %d", reserved)
	}
}

func TestReserveContract_StartsWorkerOnlyOnce(t *testing.T) {
	addr := testContractAddr(4)
	xdb := newMemoryContractDB(db.StoredBag{
		ContractAddr: addr,
		Status:       db.StoredBagStatusStopped,
	})
	svc := &Service{db: xdb, spaceAllocated: 100}

	type result struct {
		bag     db.StoredBag
		started bool
		err     error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			<-start
			bag, shouldStart, err := svc.reserveContract(addr, []byte{1, 2, 3}, 60, &db.ContractInfo{})
			results <- result{bag: bag, started: shouldStart, err: err}
		}()
	}
	close(start)

	started := 0
	for range 2 {
		res := <-results
		if res.err != nil {
			t.Fatalf("reserve failed: %v", res.err)
		}
		if res.started {
			started++
		}
		if res.bag.Status != db.StoredBagStatusAdded || res.bag.Size != 60 {
			t.Fatalf("unexpected reserved bag: %+v", res.bag)
		}
	}
	if started != 1 {
		t.Fatalf("expected one worker start, got %d", started)
	}
}

func TestAvailableStorageSpace_LegacyAddedContractReservesRemainder(t *testing.T) {
	bags := []db.StoredBag{{
		ContractAddr: testContractAddr(5),
		Status:       db.StoredBagStatusAdded,
	}}
	if got := availableStorageSpace(bags, 100); got != 0 {
		t.Fatalf("available space = %d, want 0", got)
	}
}

func TestStoredBag_UnmarshalsLegacyStoppedRecord(t *testing.T) {
	legacy := []byte(`{"b":"AQID","s":0,"a":"contract","t":2,"i":null}`)

	var bag db.StoredBag
	if err := json.Unmarshal(legacy, &bag); err != nil {
		t.Fatalf("unmarshal legacy record: %v", err)
	}
	if bag.Status != db.StoredBagStatusStopped {
		t.Fatalf("status = %d, want stopped", bag.Status)
	}
	if bag.StopReason != db.StoredBagStopReasonUnknown {
		t.Fatalf("unexpected stop reason for legacy record: %+v", bag)
	}
}

type reconcileFetchStub struct {
	mx    sync.Mutex
	calls []string
	err   error
}

func (s *reconcileFetchStub) FetchStorageInfo(_ context.Context, contractAddr *address.Address, _ uint64) (*StorageInfo, error) {
	s.mx.Lock()
	s.calls = append(s.calls, contractAddr.String())
	s.mx.Unlock()
	return &StorageInfo{Status: "resolving"}, s.err
}

func TestReconcileStoppedContracts_SelectsOnlyRetryableContracts(t *testing.T) {
	legacyAddr := testContractAddr(10)
	lowBalanceAddr := testContractAddr(11)
	terminalAddr := testContractAddr(13)
	activeAddr := testContractAddr(14)

	xdb := newMemoryContractDB(
		db.StoredBag{ContractAddr: legacyAddr, Status: db.StoredBagStatusStopped},
		db.StoredBag{ContractAddr: lowBalanceAddr, Status: db.StoredBagStatusStopped, StopReason: db.StoredBagStopReasonLowBalance},
		db.StoredBag{ContractAddr: terminalAddr, Status: db.StoredBagStatusStopped, StopReason: db.StoredBagStopReasonInvalidContract},
		db.StoredBag{ContractAddr: activeAddr, Status: db.StoredBagStatusActive},
	)
	svc := &Service{db: xdb}
	fetcher := &reconcileFetchStub{}

	if err := svc.reconcileStoppedContracts(context.Background(), fetcher, 0); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	fetcher.mx.Lock()
	defer fetcher.mx.Unlock()
	if len(fetcher.calls) != 2 {
		t.Fatalf("expected 2 fetch calls, got %d: %v", len(fetcher.calls), fetcher.calls)
	}
	seen := map[string]bool{}
	for _, addr := range fetcher.calls {
		seen[addr] = true
	}
	if !seen[legacyAddr] || !seen[lowBalanceAddr] {
		t.Fatalf("expected legacy and low-balance contracts, got %v", fetcher.calls)
	}
}

func TestRecordStoppedReconcileFailure_ClassifiesReason(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantReason db.StoredBagStopReason
	}{
		{name: "low balance", err: ErrLowBalance, wantReason: db.StoredBagStopReasonLowBalance},
		{name: "no space", err: ErrNoSpace, wantReason: db.StoredBagStopReasonNoSpace},
		{name: "temporary", err: context.DeadlineExceeded, wantReason: db.StoredBagStopReasonTemporaryError},
		{name: "low rate", err: ErrTooLowRate, wantReason: db.StoredBagStopReasonLowRate},
		{name: "provider removed", err: ErrProviderRemoved, wantReason: db.StoredBagStopReasonProviderRemoved},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr := testContractAddr(byte(20 + i))
			xdb := newMemoryContractDB(db.StoredBag{ContractAddr: addr, Status: db.StoredBagStatusStopped})
			svc := &Service{db: xdb}

			if err := svc.recordStoppedReconcileFailure(addr, tt.err); err != nil {
				t.Fatalf("record failure: %v", err)
			}
			bag, err := xdb.GetContract(addr)
			if err != nil {
				t.Fatal(err)
			}
			if bag.StopReason != tt.wantReason {
				t.Fatalf("reason = %q, want %q", bag.StopReason, tt.wantReason)
			}
		})
	}
}

func TestMarkContractStopped_PersistsReason(t *testing.T) {
	addr := testContractAddr(40)
	xdb := newMemoryContractDB(db.StoredBag{
		ContractAddr: addr,
		Status:       db.StoredBagStatusActive,
	})
	svc := &Service{db: xdb}

	if err := svc.markContractStopped(
		addr,
		[]byte{1, 2, 3},
		&db.ContractInfo{MaxSpan: 60, PerMB: "1"},
		db.StoredBagStopReasonLowBalance,
	); err != nil {
		t.Fatalf("mark stopped: %v", err)
	}

	bag, err := xdb.GetContract(addr)
	if err != nil {
		t.Fatal(err)
	}
	if bag.Status != db.StoredBagStatusStopped || bag.StopReason != db.StoredBagStopReasonLowBalance {
		t.Fatalf("unexpected stopped bag: %+v", bag)
	}
}
