/*
Copyright 2021 IBM All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package commit

import (
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/pkg/errors"
)

func transactionValidationCodes(block *common.Block) (map[string]peer.TxValidationCode, error) {
	results := make(map[string]peer.TxValidationCode)

	envelopes := block.Data.Data
	for i, envelopeBytes := range envelopes {
		channelHeader, err := unmarshalChannelHeader(envelopeBytes)
		if err != nil {
			return nil, errors.WithMessagef(err, "failed to unmarshal channel header from envelope at index %v in block number %v",
				i, block.Header.Number)
		}

		validationCode := validationCode(block, i)
		if validationCode == peer.TxValidationCode_BAD_PROPOSAL_TXID {
			continue
		}

		if _, exists := results[channelHeader.TxId]; !exists {
			results[channelHeader.TxId] = validationCode
		}
	}

	return results, nil
}

func validationCode(block *common.Block, envelopeIndex int) peer.TxValidationCode {
	validationCodes := block.Metadata.Metadata[int(common.BlockMetadataIndex_TRANSACTIONS_FILTER)]
	return peer.TxValidationCode(validationCodes[envelopeIndex])
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
