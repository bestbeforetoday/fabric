/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package commit

import (
	"context"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/internal/pkg/gateway/commit/mocks"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

//go:generate mockery --name=QueryProvider --inpackage --filename=queryprovider_mock_test.go
//go:generate counterfeiter -o mocks/queryprovider.go --fake-name QueryProvider . queryProvider
type queryProvider interface { // Mimic QueryProvider to avoid circular import with generated mock
	QueryProvider
}

func TestFinder(t *testing.T) {
	sendUntilDone := func(commitSend chan<- *ledger.CommitNotification, msg *ledger.CommitNotification) chan struct{} {
		done := make(chan struct{})

		go func() {
			for ; ; time.Sleep(10 * time.Millisecond) {
				select {
				case commitSend <- msg:
				case <-done:
					return
				}
			}
		}()

		return done
	}

	t.Run("BROKEN - passes channel name to query provider", func(t *testing.T) {
		provider := mocks.QueryProvider{}
		status := &Status{
			Code:        peer.TxValidationCode_MVCC_READ_CONFLICT,
			BlockNumber: 101,
		}
		provider.TransactionStatusReturns(status, nil)
		finder := &Finder{
			Query:    provider,
			Notifier: NewNotifier(newNotificationSupplier(nil)),
		}

		finder.TransactionStatus(context.Background(), "CHANNEL", "TX_ID")

		require.Equal(t, 1, provider.TransactionStatusCallCount(), "unexpected call count")
		actual, _ := provider.TransactionStatusArgsForCall(0)
		require.Equal(t, "CHANNEL", actual)
	})

	t.Run("passes channel name to query provider", func(t *testing.T) {
		provider := &MockQueryProvider{}
		status := &Status{
			Code:        peer.TxValidationCode_MVCC_READ_CONFLICT,
			BlockNumber: 101,
		}
		provider.On("TransactionStatus", "CHANNEL", mock.Anything).
			Return(status, nil).
			Once()
		finder := &Finder{
			Query:    provider,
			Notifier: NewNotifier(newNotificationSupplier(nil)),
		}

		finder.TransactionStatus(context.Background(), "CHANNEL", "TX_ID")

		provider.AssertExpectations(t)
	})

	t.Run("passes transaction ID to query provider", func(t *testing.T) {
		provider := &MockQueryProvider{}
		status := &Status{
			Code:        peer.TxValidationCode_MVCC_READ_CONFLICT,
			BlockNumber: 101,
		}
		provider.On("TransactionStatus", mock.Anything, "TX_ID").
			Return(status, nil).
			Once()
		finder := &Finder{
			Query:    provider,
			Notifier: NewNotifier(newNotificationSupplier(nil)),
		}

		finder.TransactionStatus(context.Background(), "CHANNEL", "TX_ID")

		provider.AssertExpectations(t)
	})

	t.Run("returns previously committed transaction status", func(t *testing.T) {
		provider := &MockQueryProvider{}
		status := &Status{
			Code:        peer.TxValidationCode_MVCC_READ_CONFLICT,
			BlockNumber: 101,
		}
		provider.On("TransactionStatus", mock.Anything, mock.Anything).
			Return(status, nil)
		finder := &Finder{
			Query:    provider,
			Notifier: NewNotifier(newNotificationSupplier(nil)),
		}

		actual, err := finder.TransactionStatus(context.Background(), "CHANNEL", "TX_ID")
		require.NoError(t, err)

		require.Equal(t, status, actual)
	})

	t.Run("returns notified transaction status when no previous commit", func(t *testing.T) {
		provider := &MockQueryProvider{}
		provider.On("TransactionStatus", mock.Anything, mock.Anything).
			Return(nil, errors.New("NOT_FOUND"))
		commitSend := make(chan *ledger.CommitNotification)
		finder := &Finder{
			Query:    provider,
			Notifier: newTestNotifier(commitSend),
		}

		msg := &ledger.CommitNotification{
			BlockNumber: 101,
			TxsInfo: []*ledger.CommitNotificationTxInfo{
				{
					TxID:           "TX_ID",
					ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
				},
			},
		}
		defer close(sendUntilDone(commitSend, msg))

		actual, err := finder.TransactionStatus(context.Background(), "CHANNEL", "TX_ID")
		require.NoError(t, err)

		expected := &Status{
			Code:        peer.TxValidationCode_MVCC_READ_CONFLICT,
			BlockNumber: 101,
		}
		require.Equal(t, expected, actual)
	})

	t.Run("returns error from notifier", func(t *testing.T) {
		provider := &MockQueryProvider{}
		provider.On("TransactionStatus", mock.Anything, mock.Anything).
			Return(nil, errors.New("NOT_FOUND"))
		supplier := &MockNotificationSupplier{}
		supplier.On("CommitNotifications", mock.Anything, mock.Anything).
			Return(nil, errors.New("MY_ERROR"))
		finder := &Finder{
			Query:    provider,
			Notifier: NewNotifier(supplier),
		}

		_, err := finder.TransactionStatus(context.Background(), "CHANNEL", "TX_ID")
		require.ErrorContains(t, err, "MY_ERROR")
	})

	t.Run("passes channel name to supplier", func(t *testing.T) {
		provider := &MockQueryProvider{}
		provider.On("TransactionStatus", mock.Anything, mock.Anything).
			Return(nil, errors.New("NOT_FOUND"))
		supplier := &MockNotificationSupplier{}
		supplier.On("CommitNotifications", mock.Anything, "CHANNEL").
			Return(nil, errors.New("MY_ERROR")).
			Once()
		finder := &Finder{
			Query:    provider,
			Notifier: NewNotifier(supplier),
		}

		finder.TransactionStatus(context.Background(), "CHANNEL", "TX_ID")

		supplier.AssertExpectations(t)
	})

	t.Run("returns context error when context cancelled", func(t *testing.T) {
		provider := &MockQueryProvider{}
		provider.On("TransactionStatus", mock.Anything, mock.Anything).
			Return(nil, errors.New("NOT_FOUND"))
		finder := &Finder{
			Query:    provider,
			Notifier: newTestNotifier(nil),
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := finder.TransactionStatus(ctx, "CHANNEL", "TX_ID")

		require.Equal(t, context.Canceled, err)
	})

	t.Run("returns error when notification supplier fails", func(t *testing.T) {
		provider := &MockQueryProvider{}
		provider.On("TransactionStatus", mock.Anything, mock.Anything).
			Return(nil, errors.New("NOT_FOUND"))
		commitSend := make(chan *ledger.CommitNotification)
		close(commitSend)
		finder := &Finder{
			Query:    provider,
			Notifier: newTestNotifier(commitSend),
		}

		_, err := finder.TransactionStatus(context.Background(), "CHANNEL", "TX_ID")

		require.ErrorContains(t, err, "unexpected close of commit notification channel")
	})
}
