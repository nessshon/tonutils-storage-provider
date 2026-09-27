package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/ton/wallet"
	"time"
)

const (
	// equals the wallet message TTL set in NewService, after it the message cannot be included anymore
	txSendTimeout = 120 * time.Second
	// liteserver may accept an external message and never relay it, so it goes to the next node every interval,
	// each node gets it rarely, far below the liteserver per address limit (30 messages per 10 seconds)
	txResendInterval = time.Second
)

// TxQueue serializes wallet transactions so they are sent one-by-one from a single goroutine
// while allowing callers (possibly many goroutines) to wait for their own results.
// It prevents concurrent SendWaitTransaction calls on the same wallet, which may
// cause nonce/seqno races or other issues.
//
// Usage:
//   hash, err := q.SendWait(ctx, wallet.SimpleMessage(addr, amount, payload))
//
// The queue must be created with NewTxQueue and will run an internal worker until the context is cancelled.
// Cancelling the provided context to NewTxQueue will stop the queue and unblock pending callers with ctx errors.
// Individual SendWait contexts are still respected for cancellation/timeouts.

type TxQueue struct {
	w      *wallet.Wallet
	api    ton.APIClientWrapped
	reqCh  chan txRequest
	closed chan struct{}
}

type txRequest struct {
	ctx  context.Context
	msg  *wallet.Message
	resp chan txResponse
	at   time.Time
}

type txResponse struct {
	hash []byte
	err  error
}

// NewTxQueue creates a TxQueue bound to the given wallet and starts a single worker
// that processes requests sequentially until ctx is cancelled.
func NewTxQueue(ctx context.Context, w *wallet.Wallet, api ton.APIClientWrapped) *TxQueue {
	q := &TxQueue{
		w:      w,
		api:    api,
		reqCh:  make(chan txRequest),
		closed: make(chan struct{}),
	}

	go q.loop(ctx)
	return q
}

func (q *TxQueue) loop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			q.shutdown(ctx)
			return
		case req := <-q.reqCh:
			if req.ctx.Err() != nil {
				select {
				case req.resp <- txResponse{err: req.ctx.Err()}:
				case <-ctx.Done():
				}
				continue
			}

			// time in queue is not counted, each message gets the whole ttl to be included
			sendCtx, cancel := context.WithTimeout(req.ctx, txSendTimeout)
			hash, err := q.send(sendCtx, req.msg, time.Since(req.at))
			cancel()

			select {
			case req.resp <- txResponse{hash: hash, err: err}:
			case <-req.ctx.Done():
			case <-ctx.Done():
				q.shutdown(ctx)
				return
			}
		}
	}
}

func (q *TxQueue) send(ctx context.Context, msg *wallet.Message, queued time.Duration) ([]byte, error) {
	ext, err := q.w.BuildExternalMessageForMany(ctx, []*wallet.Message{msg})
	if err != nil {
		return nil, fmt.Errorf("failed to build external message: %w", err)
	}

	startedAt := time.Now()
	var sent, failed int
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		sent, failed = q.resend(ctx, ext, stop)
	}()

	tx, _, _, err := q.api.SendExternalMessageWaitTransaction(ctx, ext)
	close(stop)
	<-done

	l := log.Info()
	if err != nil {
		l = log.Warn().Err(err)
		// signed message to emulate it and see why it was not included
		if c, cErr := tlb.ToCell(ext); cErr == nil {
			l = l.Hex("boc", c.ToBOCWithFlags(false))
		}
	}
	l.Hex("msg_hash", ext.NormalizedHash()).Dur("queued", queued).Dur("took", time.Since(startedAt)).
		Int("sent", sent).Int("failed", failed).Msg("external message send finished")

	if err != nil {
		return nil, err
	}
	return tx.Hash, nil
}

// resend sends the same message to the next liteserver every interval until stopped
func (q *TxQueue) resend(ctx context.Context, ext *tlb.ExternalMessage, stop <-chan struct{}) (sent, failed int) {
	cl := q.api.Client()
	node := cl.StickyContext(ctx)
	for {
		sent++
		if err := q.api.SendExternalMessage(node, ext); err != nil && ctx.Err() == nil {
			failed++
			log.Warn().Err(err).Msg("liteserver did not accept external message")
		}

		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-time.After(txResendInterval):
		}

		next, err := cl.StickyContextNextNode(node)
		if err != nil {
			// all nodes were used, go round again
			next = cl.StickyContext(ctx)
		}
		node = next
	}
}

func (q *TxQueue) shutdown(ctx context.Context) {
	close(q.closed)

	for {
		select {
		case req := <-q.reqCh:
			select {
			case req.resp <- txResponse{err: ctx.Err()}:
			default:
			}
		default:
			return
		}
	}
}

// SendWait enqueues the message and waits for the transaction result.
// If the queue is already stopped, it returns context.Canceled-like error.
func (q *TxQueue) SendWait(ctx context.Context, msg *wallet.Message) ([]byte, error) {
	if msg == nil {
		return nil, errors.New("nil wallet message")
	}
	respCh := make(chan txResponse, 1)
	req := txRequest{ctx: ctx, msg: msg, resp: respCh, at: time.Now()}

	select {
	case q.reqCh <- req:
		// enqueued
	case <-q.closed:
		return nil, context.Canceled
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case r := <-respCh:
		return r.hash, r.err
	case <-q.closed:
		return nil, context.Canceled
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
