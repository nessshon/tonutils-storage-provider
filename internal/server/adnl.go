package server

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-storage-provider/internal/service"
	"github.com/xssnick/tonutils-storage-provider/pkg/transport"
	"strings"
	"time"
)

// minDHTRecordCopies is the number of dht nodes below which our record is
// considered poorly replicated and clients may fail to resolve us.
const minDHTRecordCopies = 3

type Service interface {
	FetchStorageInfo(ctx context.Context, contractAddr *address.Address, byteToProof uint64) (*service.StorageInfo, error)
	GetStorageInfo(bagSize uint64) (available bool, minSpan, maxSpan uint32, spaceAvailable uint64, ratePerMB tlb.Coins)
	RequestStorageADNLProof(ctx context.Context, contractAddr *address.Address) (ed25519.PublicKey, []byte, error)
}

type Server struct {
	key         ed25519.PrivateKey
	providerKey ed25519.PrivateKey
	dht         *dht.Client
	gate        *adnl.Gateway
	svc         Service
	logger      zerolog.Logger

	globalCtx context.Context
	closer    func()
}

func NewServer(dht *dht.Client, gate *adnl.Gateway, key ed25519.PrivateKey, providerKey ed25519.PrivateKey, svc Service, logger zerolog.Logger) *Server {
	s := &Server{
		key:         key,
		providerKey: providerKey,
		dht:         dht,
		gate:        gate,
		svc:         svc,
		logger:      logger,
	}
	s.globalCtx, s.closer = context.WithCancel(context.Background())
	s.gate.SetConnectionHandler(s.bootstrapPeer)

	go func() {
		wait := 1 * time.Second
		// refresh dht records
		for {
			select {
			case <-s.globalCtx.Done():
				logger.Info().Msg("DHT updater stopped")
				return
			case <-time.After(wait):
			}

			logger.Debug().Msg("updating our address record")

			ctx, cancel := context.WithTimeout(s.globalCtx, 300*time.Second)
			err := s.updateDHT(ctx)
			cancel()

			if err != nil {
				logger.Warn().Err(err).Msg("failed to update our DHT address record, will retry...")

				// on err, retry sooner
				wait = 5 * time.Second
				continue
			}
			wait = 1 * time.Minute
		}
	}()

	return s
}

func (s *Server) updateDHT(ctx context.Context) error {
	addr := s.gate.GetAddressList()

	ctxStore, cancel := context.WithTimeout(ctx, 90*time.Second)
	stored, id, err := s.dht.StoreAddress(ctxStore, addr, 30*time.Minute, s.key)
	cancel()
	if stored == 0 {
		if err != nil {
			return fmt.Errorf("failed to store address: %w", err)
		}
		return fmt.Errorf("address record was not accepted by any dht node")
	}

	pID := keys.PublicKeyED25519{Key: s.providerKey.Public().(ed25519.PublicKey)}
	data, err := tl.Serialize(transport.ProviderDHTRecord{
		ADNLAddr: id,
	}, true)
	if err != nil {
		return fmt.Errorf("failed to serialize data for dht: %w", err)
	}

	ctxStore, cancel = context.WithTimeout(ctx, 90*time.Second)
	stored, id, err = s.dht.Store(ctxStore, pID, []byte("storage-provider"), 0, data, dht.UpdateRuleSignature{}, 30*time.Minute, s.providerKey)
	cancel()
	if stored == 0 {
		if err != nil {
			return fmt.Errorf("failed to store storage-provider record in dht: %w", err)
		}
		return fmt.Errorf("storage-provider record was not accepted by any dht node")
	}

	rt := s.dht.RoutingTableStats()
	if stored < minDHTRecordCopies {
		s.logger.Warn().Int("nodes", stored).
			Int("dht_active", rt.ActiveNodes).
			Int("dht_backup", rt.BackupNodes).
			Int("dht_buckets", rt.FilledBuckets).
			Msg("our record is stored on too few dht nodes, we may be hard to discover")
	}

	s.logger.Debug().Int("nodes", stored).
		Int("dht_active", rt.ActiveNodes).
		Int("dht_backup", rt.BackupNodes).
		Int("dht_buckets", rt.FilledBuckets).
		Msg("our address record updated")

	return nil
}

func (s *Server) bootstrapPeer(client adnl.Peer) error {
	rl := rldp.NewClientV2(client)
	rl.SetOnQuery(s.handleRLDPQuery(rl))

	return nil
}

func (s *Server) handleRLDPQuery(peer *rldp.RLDP) func(transfer []byte, query *rldp.Query) error {
	return func(transfer []byte, query *rldp.Query) error {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		switch q := query.Data.(type) {
		case transport.StorageRatesRequest:
			ok, minSpan, maxSpan, av, rate := s.svc.GetStorageInfo(q.Size)

			err := peer.SendAnswer(ctx, query.MaxAnswerSize, query.Timeout, query.ID, transfer, &transport.StorageRatesResponse{
				Available:        ok,
				RatePerMBDay:     rate.Nano().Bytes(),
				MinBounty:        tlb.MustFromTON("0.05").Nano().Bytes(),
				SpaceAvailableMB: av,
				MinSpan:          minSpan,
				MaxSpan:          maxSpan,
			})
			if err != nil {
				return err
			}
		case transport.StorageRequest:
			addr := address.NewAddress(0, 0, q.ContractAddress)

			var resp transport.StorageResponse
			info, err := s.svc.FetchStorageInfo(ctx, addr, q.ByteToProof)
			if err != nil {
				reason := err.Error()
				switch {
				case errors.Is(err, service.ErrLowBalance):
				case errors.Is(err, service.ErrLowBounty):
				case errors.Is(err, service.ErrNotDeployed):
				case errors.Is(err, service.ErrTooLowRate):
				case errors.Is(err, service.ErrTooShortSpan):
				case errors.Is(err, service.ErrTooLongSpan):
				case errors.Is(err, service.ErrNoSpace):
				case errors.Is(err, service.ErrTooBigBag):
				case errors.Is(err, service.ErrProviderRemoved):
				case isTemporaryProviderError(err):
					reason = "temporary provider error"
					log.Debug().Err(err).Str("addr", addr.String()).Msg("temporary provider error")
				default:
					reason = "internal provider error"
					log.Warn().Err(err).Str("addr", addr.String()).Msg("internal provider error")
				}

				resp = transport.StorageResponse{
					Status: "error",
					Reason: reason,
				}
			} else {
				resp = transport.StorageResponse{
					Status:     info.Status,
					Downloaded: info.Downloaded,
					Proof:      info.Proof,
				}
			}

			if err = peer.SendAnswer(ctx, query.MaxAnswerSize, query.Timeout, query.ID, transfer, &resp); err != nil {
				return err
			}
		case transport.StorageADNLProofRequest:
			addr := address.NewAddress(0, 0, q.ContractAddress)

			key, sign, err := s.svc.RequestStorageADNLProof(ctx, addr)
			if err != nil {
				log.Warn().Err(err).Str("addr", addr.String()).Msg("request storage adnl proof err")
				return err
			}

			err = peer.SendAnswer(ctx, query.MaxAnswerSize, query.Timeout, query.ID, transfer, &transport.StorageADNLProofResponse{
				StorageKey: key,
				Signature:  sign,
			})
			if err != nil {
				return err
			}
		default:
			log.Debug().Type("type", q).Msg("received unknown request type")
		}

		return nil
	}
}

func isTemporaryProviderError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}

	errText := strings.ToLower(err.Error())
	return strings.Contains(errText, "failed to get master block") ||
		strings.Contains(errText, "deadline exceeded") ||
		strings.Contains(errText, "get masterchain info")
}
