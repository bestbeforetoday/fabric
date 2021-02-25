/*
Copyright 2021 IBM All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package commit_test

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/deliver/mock"
	"github.com/hyperledger/fabric/internal/pkg/gateway/commit"
	"github.com/hyperledger/fabric/internal/pkg/gateway/commit/mocks"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestNotifier(t *testing.T) {
	type transactionInfo struct {
		ID     string
		Status peer.TxValidationCode
	}

	newPayload := func(t *testing.T, transactionID string) *common.Payload {
		channelHeader := &common.ChannelHeader{
			Type: int32(common.HeaderType_ENDORSER_TRANSACTION),
			TxId: transactionID,
		}
		channelHeaderBytes, err := proto.Marshal(channelHeader)
		require.NoError(t, err)

		return &common.Payload{
			Header: &common.Header{
				ChannelHeader: channelHeaderBytes,
			},
		}
	}

	newTransactionEnvelope := func(t *testing.T, transaction *transactionInfo) *common.Envelope {
		payload := newPayload(t, transaction.ID)
		payloadBytes, err := proto.Marshal(payload)
		require.NoError(t, err)

		return &common.Envelope{
			Payload: payloadBytes,
		}
	}

	newBlock := func(t *testing.T, transactions ...*transactionInfo) *common.Block {
		var validationCodes []byte
		var envelopes [][]byte

		for _, transaction := range transactions {
			validationCodes = append(validationCodes, byte(transaction.Status))

			envelope := newTransactionEnvelope(t, transaction)
			envelopeBytes, err := proto.Marshal(envelope)
			require.NoError(t, err)
			envelopes = append(envelopes, envelopeBytes)
		}

		metadata := make([][]byte, len(common.BlockMetadataIndex_name))
		metadata[int(common.BlockMetadataIndex_TRANSACTIONS_FILTER)] = validationCodes

		return &common.Block{
			Header: &common.BlockHeader{
				Number: 1,
			},
			Metadata: &common.BlockMetadata{
				Metadata: metadata,
			},
			Data: &common.BlockData{
				Data: envelopes,
			},
		}
	}

	newMockBlockReader := func(blocks ...*common.Block) *mocks.BlockReader {
		blockReader := &mocks.BlockReader{}

		iterator := &mock.BlockIterator{}
		blockReader.IteratorReturns(iterator, nil)

		for i, block := range blocks {
			iterator.NextReturnsOnCall(i, block, common.Status_SUCCESS)
		}

		return blockReader
	}

	newTestNotifier := func(blocks ...*common.Block) *commit.Notifier {
		blockReader := newMockBlockReader(blocks...)
		return commit.NewNotifier(blockReader)
	}

	t.Run("Notify returns error if block iterator cannot be obtained", func(t *testing.T) {
		expected := "MY_ERROR_MESSAGE"
		blockReader := &mocks.BlockReader{}
		blockReader.IteratorReturns(nil, errors.New(expected))
		notifier := commit.NewNotifier(blockReader)

		_, err := notifier.Notify("channel", "transactionID")
		require.ErrorContains(t, err, expected)
	})

	t.Run("Closes notification channel if unable to read from block iterator", func(t *testing.T) {
		blockReader := &mocks.BlockReader{}
		blockIterator := &mock.BlockIterator{}
		blockReader.IteratorReturns(blockIterator, nil)
		blockIterator.NextReturns(nil, common.Status_INTERNAL_SERVER_ERROR)
		notifier := commit.NewNotifier(blockReader)

		commitChannel, err := notifier.Notify("channel", "transactionID")
		require.NoError(t, err)

		_, ok := <-commitChannel

		require.False(t, ok, "Commit channel not closed")
	})

	t.Run("Notifies valid transaction status", func(t *testing.T) {
		transaction := &transactionInfo{
			ID:     "TRANSACTION_ID",
			Status: peer.TxValidationCode_VALID,
		}
		block := newBlock(t, transaction)
		notifier := newTestNotifier(block)

		commitChannel, err := notifier.Notify("channel", transaction.ID)
		require.NoError(t, err)

		result, ok := <-commitChannel

		require.True(t, ok, "Commit channel was closed")
		require.Equal(t, transaction.Status, result)
	})

	t.Run("Notifies invalid transaction status", func(t *testing.T) {
		transaction := &transactionInfo{
			ID:     "TRANSACTION_ID",
			Status: peer.TxValidationCode_MVCC_READ_CONFLICT,
		}
		block := newBlock(t, transaction)
		notifier := newTestNotifier(block)

		commitChannel, _ := notifier.Notify("channel", transaction.ID)

		result, ok := <-commitChannel

		require.True(t, ok, "Commit channel was closed")
		require.Equal(t, transaction.Status, result)
	})

	t.Run("Closes notification channel after delivering transaction status", func(t *testing.T) {
		transaction := &transactionInfo{
			ID:     "TRANSACTION_ID",
			Status: peer.TxValidationCode_MVCC_READ_CONFLICT, // Don't use VALID since it matches default channel value
		}
		block := newBlock(t, transaction)
		notifier := newTestNotifier(block)

		commitChannel, err := notifier.Notify("channel", transaction.ID)
		require.NoError(t, err)

		<-commitChannel
		result, ok := <-commitChannel

		require.False(t, ok, "Commit channel not closed")
		require.Equal(t, peer.TxValidationCode(0), result)
	})

	t.Run("Ignores envelopes for other transactions", func(t *testing.T) {
		dummyTransaction := &transactionInfo{
			ID:     "DUMMY",
			Status: peer.TxValidationCode_MVCC_READ_CONFLICT,
		}
		transaction := &transactionInfo{
			ID:     "TRANSACTION_ID",
			Status: peer.TxValidationCode_VALID,
		}
		block := newBlock(t, dummyTransaction, transaction)
		notifier := newTestNotifier(block)

		commitChannel, err := notifier.Notify("channel", transaction.ID)
		require.NoError(t, err)

		result, ok := <-commitChannel

		require.True(t, ok, "Commit channel was closed")
		require.Equal(t, transaction.Status, result)
	})

	t.Run("Ignores blocks not containing specified transaction", func(t *testing.T) {
		dummyTransaction := &transactionInfo{
			ID:     "DUMMY",
			Status: peer.TxValidationCode_MVCC_READ_CONFLICT,
		}
		dummyBlock := newBlock(t, dummyTransaction)

		transaction := &transactionInfo{
			ID:     "TRANSACTION_ID",
			Status: peer.TxValidationCode_VALID,
		}
		block := newBlock(t, transaction)

		notifier := newTestNotifier(dummyBlock, block)

		commitChannel, err := notifier.Notify("channel", transaction.ID)
		require.NoError(t, err)

		result, ok := <-commitChannel

		require.True(t, ok, "Commit channel was closed")
		require.Equal(t, transaction.Status, result)
	})

	t.Run("Closes notification channel on missing payload header", func(t *testing.T) {
		transaction := &transactionInfo{
			ID:     "TRANSACTION_ID",
			Status: peer.TxValidationCode_VALID,
		}
		block := newBlock(t, transaction)

		payload := newPayload(t, transaction.ID)
		payload.Header = nil
		payloadBytes, err := proto.Marshal(payload)
		require.NoError(t, err)

		envelope := &common.Envelope{
			Payload: payloadBytes,
		}
		envelopeBytes, err := proto.Marshal(envelope)
		require.NoError(t, err)

		block.Data.Data[0] = envelopeBytes

		// Dummy block required to prevent channel close if first block does not cause close correctly
		dummyTransaction := &transactionInfo{
			ID:     transaction.ID,
			Status: peer.TxValidationCode_VALID,
		}
		dummyBlock := newBlock(t, dummyTransaction)

		notifier := newTestNotifier(block, dummyBlock)

		commitChannel, err := notifier.Notify("channel", transaction.ID)
		require.NoError(t, err)

		_, ok := <-commitChannel

		require.False(t, ok, "Commit channel not closed")
	})

	t.Run("Ignores transactions with TxValidationCode_BAD_PROPOSAL_TXID status since may have faked the ID we want", func(t *testing.T) {
		transaction := &transactionInfo{
			ID:     "TRANSACTION_ID",
			Status: peer.TxValidationCode_VALID,
		}
		badTransaction := &transactionInfo{
			ID:     transaction.ID,
			Status: peer.TxValidationCode_BAD_PROPOSAL_TXID,
		}
		block := newBlock(t, badTransaction, transaction, badTransaction)
		notifier := newTestNotifier(block)

		commitChannel, err := notifier.Notify("channel", transaction.ID)
		require.NoError(t, err)

		result, ok := <-commitChannel

		require.True(t, ok, "Commit channel was closed")
		require.Equal(t, transaction.Status, result)
	})

	t.Run("Notifies status of first transaction with matching ID in block", func(t *testing.T) {
		transaction1 := &transactionInfo{
			ID:     "TRANSACTION_ID",
			Status: peer.TxValidationCode_VALID,
		}
		transaction2 := &transactionInfo{
			ID:     transaction1.ID,
			Status: peer.TxValidationCode_MVCC_READ_CONFLICT,
		}
		block := newBlock(t, transaction1, transaction2)
		notifier := newTestNotifier(block)

		commitChannel, err := notifier.Notify("channel", transaction1.ID)
		require.NoError(t, err)

		result, ok := <-commitChannel

		require.True(t, ok, "Commit channel was closed")
		require.Equal(t, transaction1.Status, result)
	})
}
