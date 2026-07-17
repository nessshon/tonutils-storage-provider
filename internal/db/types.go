package db

import "errors"

type StoredBagStatus int
type StoredBag struct {
	BagID        []byte              `json:"b"`
	Size         uint64              `json:"s"`
	ContractAddr string              `json:"a"`
	Status       StoredBagStatus     `json:"t"`
	ContractInfo *ContractInfo       `json:"i"`
	StopReason   StoredBagStopReason `json:"r,omitempty"`
}

type StoredBagStopReason string

type ContractInfo struct {
	MaxSpan uint32 `json:"ms"`
	PerMB   string `json:"p"`
}

type CronContract struct {
	ContractAddr string `json:"a"`
	NextQuery    int64  `json:"t"`
	Reward       string `json:"r"`
	Version      int    `json:"v"`
}

var ErrNotFound = errors.New("not found")

const (
	StoredBagStatusAdded StoredBagStatus = iota
	StoredBagStatusActive
	StoredBagStatusStopped
)

const (
	StoredBagStopReasonUnknown             StoredBagStopReason = ""
	StoredBagStopReasonInvalidContract     StoredBagStopReason = "invalid_contract"
	StoredBagStopReasonUnsupportedContract StoredBagStopReason = "unsupported_contract"
	StoredBagStopReasonNotDeployed         StoredBagStopReason = "not_deployed"
	StoredBagStopReasonBagTooBig           StoredBagStopReason = "bag_too_big"
	StoredBagStopReasonDownloadStalled     StoredBagStopReason = "download_stalled"
	StoredBagStopReasonProviderRemoved     StoredBagStopReason = "provider_removed"
	StoredBagStopReasonShortSpan           StoredBagStopReason = "short_span"
	StoredBagStopReasonLongSpan            StoredBagStopReason = "long_span"
	StoredBagStopReasonLowRate             StoredBagStopReason = "low_rate"
	StoredBagStopReasonLowBounty           StoredBagStopReason = "low_bounty"
	StoredBagStopReasonLowBalance          StoredBagStopReason = "low_balance"
	StoredBagStopReasonNoSpace             StoredBagStopReason = "no_space"
	StoredBagStopReasonTemporaryError      StoredBagStopReason = "temporary_error"
)
