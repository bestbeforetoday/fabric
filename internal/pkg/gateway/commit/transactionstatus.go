/*
Copyright 2021 IBM All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package commit

import (
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/pkg/errors"
)

// TransactionStatus is sent to the returned channel when received from the block iterator. The returned channel will
// be closed after the status is sent, or if an error occurs that prevents the transaction status from being obtained.
// The receiver should always check the the closed status of the channel on reads.
func TransactionStatus(done <-chan struct{}, blockIterator blockledger.Iterator, transactionID string) <-chan peer.TxValidationCode {
	out := make(chan peer.TxValidationCode, 1)
	go func() {
		defer close(out)

		for {
			select {
			case <-done:
				return
			default:
			}

			block, status := blockIterator.Next()
			if status != common.Status_SUCCESS {
				// TODO: log error -- ledger is broken
				return
			}

			if validationCode, found := findTransactionStatus(block, transactionID); found {
				out <- validationCode
				return
			}
		}
	}()

	return out
}

func findTransactionStatus(block *common.Block, transactionID string) (peer.TxValidationCode, bool) {
	envelopes := block.Data.Data
	for i, envelopeBytes := range envelopes {
		validationCode := validationCode(block, i)
		if isSpoofedTransaction(validationCode) {
			continue
		}

		channelHeader, err := unmarshalChannelHeader(envelopeBytes)
		if err != nil {
			// TODO: log error, block number and envelope index
			continue
		}

		if channelHeader.TxId == transactionID {
			return validationCode, true
		}
	}

	return peer.TxValidationCode(0), false
}

func validationCode(block *common.Block, envelopeIndex int) peer.TxValidationCode {
	validationCodes := block.Metadata.Metadata[int(common.BlockMetadataIndex_TRANSACTIONS_FILTER)]
	return peer.TxValidationCode(validationCodes[envelopeIndex])
}

func isSpoofedTransaction(validationCode peer.TxValidationCode) bool {
	return validationCode == peer.TxValidationCode_BAD_PROPOSAL_TXID
}

func unmarshalChannelHeader(envelopeBytes []byte) (*common.ChannelHeader, error) {
	envelope := &common.Envelope{}
	if err := proto.Unmarshal(envelopeBytes, envelope); err != nil {
		return nil, err
	}

	payload := &common.Payload{}
	if err := proto.Unmarshal(envelope.Payload, payload); err != nil {
		return nil, err
	}

	if payload.Header == nil {
		return nil, errors.New("missing payload header")
	}

	channelHeader := &common.ChannelHeader{}
	if err := proto.Unmarshal(payload.Header.ChannelHeader, channelHeader); err != nil {
		return nil, err
	}

	return channelHeader, nil
}
