package service

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/ton/wallet"
)

type fakeNodeKey struct{}

// fakeLiteClient pins contexts to nodes 1..nodes in order, like the pool going to the next best node
type fakeLiteClient struct {
	ton.LiteClient
	nodes uint32
}

func (c *fakeLiteClient) StickyContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, fakeNodeKey{}, uint32(1))
}

func (c *fakeLiteClient) StickyContextNextNode(ctx context.Context) (context.Context, error) {
	next := c.StickyNodeID(ctx) + 1
	if next > c.nodes {
		return ctx, liteclient.ErrNoNodesLeft
	}
	return context.WithValue(ctx, fakeNodeKey{}, next), nil
}

func (c *fakeLiteClient) StickyNodeID(ctx context.Context) uint32 {
	id, _ := ctx.Value(fakeNodeKey{}).(uint32)
	return id
}

type fakeSend struct {
	node uint32
	hash []byte
}

type fakeTxAPI struct {
	ton.APIClientWrapped
	client *fakeLiteClient
	// wait for transaction ends when confirmed returns true
	confirmed func(sends []fakeSend) bool

	mx        sync.Mutex
	sends     []fakeSend
	deadlines []time.Duration
}

func (a *fakeTxAPI) Client() ton.LiteClient                   { return a.client }
func (a *fakeTxAPI) WaitForBlock(uint32) ton.APIClientWrapped { return a }
func (a *fakeTxAPI) CurrentMasterchainInfo(context.Context) (*ton.BlockIDExt, error) {
	return &ton.BlockIDExt{SeqNo: 1}, nil
}

func (a *fakeTxAPI) GetAccount(context.Context, *ton.BlockIDExt, *address.Address) (*tlb.Account, error) {
	return &tlb.Account{IsActive: true, State: &tlb.AccountState{AccountStorage: tlb.AccountStorage{Status: tlb.AccountStatusActive}}}, nil
}

func (a *fakeTxAPI) SendExternalMessage(ctx context.Context, ext *tlb.ExternalMessage) error {
	a.mx.Lock()
	defer a.mx.Unlock()
	a.sends = append(a.sends, fakeSend{node: a.client.StickyNodeID(ctx), hash: ext.NormalizedHash()})
	return nil
}

func (a *fakeTxAPI) SendExternalMessageWaitTransaction(ctx context.Context, ext *tlb.ExternalMessage) (*tlb.Transaction, *ton.BlockIDExt, []byte, error) {
	if dl, ok := ctx.Deadline(); ok {
		a.mx.Lock()
		a.deadlines = append(a.deadlines, time.Until(dl))
		a.mx.Unlock()
	}

	for {
		a.mx.Lock()
		ok := a.confirmed(a.sends)
		a.mx.Unlock()
		if ok {
			return &tlb.Transaction{Hash: ext.NormalizedHash()}, nil, nil, nil
		}

		select {
		case <-ctx.Done():
			return nil, nil, nil, ton.ErrTxWasNotConfirmed
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func newFakeTxQueue(t *testing.T, ctx context.Context, api *fakeTxAPI) *TxQueue {
	t.Helper()

	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	w, err := wallet.FromPrivateKey(api, key, wallet.V3R2)
	if err != nil {
		t.Fatal(err)
	}
	w.GetSpec().(*wallet.SpecV3).SetSeqnoFetcher(func(context.Context, uint32) (uint32, error) {
		return 7, nil
	})
	return NewTxQueue(ctx, w, api)
}

func testProofMessage() *wallet.Message {
	return wallet.SimpleMessage(address.MustParseAddr("EQD6Dk8CIJL6wUeV2y2kapbugZEoi2LWR4wTYm-B9wpdBffF"), tlb.MustFromTON("0.05"), nil)
}

func TestTxQueue_ResendsSameMessageToNextNodes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	api := &fakeTxAPI{
		client:    &fakeLiteClient{nodes: 5},
		confirmed: func(sends []fakeSend) bool { return len(sends) >= 3 },
	}

	hash, err := newFakeTxQueue(t, ctx, api).SendWait(ctx, testProofMessage())
	if err != nil {
		t.Fatal(err)
	}

	api.mx.Lock()
	sends := append([]fakeSend(nil), api.sends...)
	api.mx.Unlock()
	for i, s := range sends {
		// node 1 gets the first send from SendExternalMessageWaitTransaction, resend starts from the next one
		if s.node != uint32(i+2) {
			t.Fatalf("send %d went to node %d, want %d", i, s.node, i+2)
		}
		if !bytes.Equal(s.hash, hash) {
			t.Fatal("every send should carry the same message")
		}
	}

	time.Sleep(2 * txResendInterval)
	api.mx.Lock()
	defer api.mx.Unlock()
	if len(api.sends) != len(sends) {
		t.Fatalf("message was resent after confirmation: %d sends", len(api.sends))
	}
}

func TestTxQueue_SendBudgetStartsWhenRequestIsTaken(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := make(chan struct{})
	api := &fakeTxAPI{
		client: &fakeLiteClient{nodes: 3},
		confirmed: func([]fakeSend) bool {
			select {
			case <-release:
				return true
			default:
				return false
			}
		},
	}
	q := newFakeTxQueue(t, ctx, api)

	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := q.SendWait(ctx, testProofMessage())
			errs <- err
		}()
	}

	// the first message holds the queue, the second one waits
	time.Sleep(2 * time.Second)
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}

	api.mx.Lock()
	defer api.mx.Unlock()
	if len(api.deadlines) != 2 {
		t.Fatalf("expected 2 waits, got %d", len(api.deadlines))
	}
	if left := api.deadlines[1]; left < txSendTimeout-time.Second {
		t.Fatalf("second message waited in queue and got only %s of %s", left, txSendTimeout)
	}
}
