package service

import (
	"testing"

	"github.com/xssnick/tonutils-storage-provider/internal/db"
)

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
