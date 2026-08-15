package service

import (
	"testing"
	"time"

	"github.com/xssnick/tonutils-storage-provider/internal/db"
)

func TestNextDropRetryDelay_StartsAtInitialDelay(t *testing.T) {
	if got := nextDropRetryDelay(0); got != dropRetryInitialDelay {
		t.Fatalf("first delay = %v, want %v", got, dropRetryInitialDelay)
	}
}

func TestNextDropRetryDelay_GrowsAndCaps(t *testing.T) {
	delay := nextDropRetryDelay(0)
	prev := delay
	for range dropRetryAttempts {
		delay = nextDropRetryDelay(delay)
		if delay < prev {
			t.Fatalf("delay went down from %v to %v", prev, delay)
		}
		prev = delay
	}

	if delay != dropRetryMaxDelay {
		t.Fatalf("capped delay = %v, want %v", delay, dropRetryMaxDelay)
	}
}

func TestNextDropRetryDelay_TotalWaitCoversAnHour(t *testing.T) {
	var total time.Duration
	delay := time.Duration(0)
	for range dropRetryAttempts {
		delay = nextDropRetryDelay(delay)
		total += delay
	}

	if total < time.Hour {
		t.Fatalf("total retry window = %v, want at least an hour", total)
	}
}

func TestBagUsedByAnotherContract_CountsAddedContracts(t *testing.T) {
	self := testContractAddr(1)
	other := testContractAddr(2)
	list := []db.StoredBag{
		{ContractAddr: self, BagID: []byte{9}, Status: db.StoredBagStatusActive},
		{ContractAddr: other, BagID: []byte{9}, Status: db.StoredBagStatusAdded},
	}

	if got := bagUsedByAnotherContract(list, []byte{9}, self); got != other {
		t.Fatalf("used by = %q, want %q", got, other)
	}
}

func TestBagUsedByAnotherContract_IgnoresStoppedAndOtherBags(t *testing.T) {
	self := testContractAddr(1)
	list := []db.StoredBag{
		{ContractAddr: testContractAddr(2), BagID: []byte{9}, Status: db.StoredBagStatusStopped},
		{ContractAddr: testContractAddr(3), BagID: []byte{8}, Status: db.StoredBagStatusActive},
	}

	if got := bagUsedByAnotherContract(list, []byte{9}, self); got != "" {
		t.Fatalf("used by = %q, want empty", got)
	}
}

func TestBagUsedByAnotherContract_IgnoresSelf(t *testing.T) {
	self := testContractAddr(1)
	list := []db.StoredBag{
		{ContractAddr: self, BagID: []byte{9}, Status: db.StoredBagStatusActive},
	}

	if got := bagUsedByAnotherContract(list, []byte{9}, self); got != "" {
		t.Fatalf("used by = %q, want empty", got)
	}
}
