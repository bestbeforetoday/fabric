/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package commit

import (
	"testing"

	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

//go:generate mockery --name=NotificationSupplier --inpackage --filename=notificationsupplier_mock_test.go

func newNotificationSupplier(commitSends ...<-chan *ledger.CommitNotification) *MockNotificationSupplier {
	supplier := &MockNotificationSupplier{}
	for _, commitSend := range commitSends {
		supplier.On("CommitNotifications", mock.Anything, mock.Anything).
			Return(commitSend, nil).
			Once()
	}
	return supplier
}

func newTestNotifier(commitSends ...<-chan *ledger.CommitNotification) *Notifier {
	supplier := newNotificationSupplier(commitSends...)
	return NewNotifier(supplier)
}

func TestNotifier(t *testing.T) {
	t.Run("Notify", func(t *testing.T) {
		t.Run("returns error from notification supplier", func(t *testing.T) {
			supplier := &MockNotificationSupplier{}
			supplier.On("CommitNotifications", mock.Anything, mock.Anything).
				Return(nil, errors.New("MY_ERROR"))
			notifier := NewNotifier(supplier)
			defer notifier.close()

			_, err := notifier.notify(nil, "CHANNEL_NAME", "TX_ID")

			require.ErrorContains(t, err, "MY_ERROR")
		})

		t.Run("delivers notification for matching transaction", func(t *testing.T) {
			commitSend := make(chan *ledger.CommitNotification, 1)
			notifier := newTestNotifier(commitSend)
			defer notifier.close()

			commitReceive, err := notifier.notify(nil, "CHANNEL_NAME", "TX_ID")
			require.NoError(t, err)

			commitSend <- &ledger.CommitNotification{
				BlockNumber: 1,
				TxsInfo: []*ledger.CommitNotificationTxInfo{
					{
						TxID:           "TX_ID",
						ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
					},
				},
			}
			actual := <-commitReceive

			expected := notification{
				BlockNumber:    1,
				TransactionID:  "TX_ID",
				ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
			}
			require.EqualValues(t, expected, actual)
		})

		t.Run("ignores non-matching transaction in same block", func(t *testing.T) {
			commitSend := make(chan *ledger.CommitNotification, 1)
			notifier := newTestNotifier(commitSend)
			defer notifier.close()

			commitReceive, err := notifier.notify(nil, "CHANNEL_NAME", "TX_ID")
			require.NoError(t, err)

			commitSend <- &ledger.CommitNotification{
				BlockNumber: 1,
				TxsInfo: []*ledger.CommitNotificationTxInfo{
					{
						TxID:           "WRONG_TX_ID",
						ValidationCode: peer.TxValidationCode_VALID,
					},
					{
						TxID:           "TX_ID",
						ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
					},
				},
			}
			actual := <-commitReceive

			expected := notification{
				BlockNumber:    1,
				TransactionID:  "TX_ID",
				ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
			}
			require.EqualValues(t, expected, actual)
		})

		t.Run("ignores blocks without matching transaction", func(t *testing.T) {
			commitSend := make(chan *ledger.CommitNotification, 2)
			notifier := newTestNotifier(commitSend)
			defer notifier.close()

			commitReceive, err := notifier.notify(nil, "CHANNEL_NAME", "TX_ID")
			require.NoError(t, err)

			commitSend <- &ledger.CommitNotification{
				BlockNumber: 1,
				TxsInfo: []*ledger.CommitNotificationTxInfo{
					{
						TxID:           "WRONG_TX_ID",
						ValidationCode: peer.TxValidationCode_VALID,
					},
				},
			}
			commitSend <- &ledger.CommitNotification{
				BlockNumber: 2,
				TxsInfo: []*ledger.CommitNotificationTxInfo{
					{
						TxID:           "TX_ID",
						ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
					},
				},
			}
			actual := <-commitReceive

			expected := notification{
				BlockNumber:    2,
				TransactionID:  "TX_ID",
				ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
			}
			require.EqualValues(t, expected, actual)
		})

		t.Run("processes blocks in order", func(t *testing.T) {
			commitSend := make(chan *ledger.CommitNotification, 2)
			notifier := newTestNotifier(commitSend)
			defer notifier.close()

			commitReceive, err := notifier.notify(nil, "CHANNEL_NAME", "TX_ID")
			require.NoError(t, err)

			commitSend <- &ledger.CommitNotification{
				BlockNumber: 1,
				TxsInfo: []*ledger.CommitNotificationTxInfo{
					{
						TxID:           "TX_ID",
						ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
					},
				},
			}
			commitSend <- &ledger.CommitNotification{
				BlockNumber: 2,
				TxsInfo: []*ledger.CommitNotificationTxInfo{
					{
						TxID:           "TX_ID",
						ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
					},
				},
			}
			actual := <-commitReceive

			expected := notification{
				BlockNumber:    1,
				TransactionID:  "TX_ID",
				ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
			}
			require.EqualValues(t, expected, actual)
		})

		t.Run("closes channel after notification", func(t *testing.T) {
			commitSend := make(chan *ledger.CommitNotification, 2)
			notifier := newTestNotifier(commitSend)
			defer notifier.close()

			commitReceive, err := notifier.notify(nil, "CHANNEL_NAME", "TX_ID")
			require.NoError(t, err)

			commitSend <- &ledger.CommitNotification{
				BlockNumber: 1,
				TxsInfo: []*ledger.CommitNotificationTxInfo{
					{
						TxID:           "TX_ID",
						ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
					},
				},
			}
			commitSend <- &ledger.CommitNotification{
				BlockNumber: 2,
				TxsInfo: []*ledger.CommitNotificationTxInfo{
					{
						TxID:           "TX_ID",
						ValidationCode: peer.TxValidationCode_VALID,
					},
				},
			}
			<-commitReceive
			_, ok := <-commitReceive

			require.False(t, ok, "Expected notification channel to be closed but receive was successful")
		})

		t.Run("stops notification when done channel closed", func(t *testing.T) {
			commitSend := make(chan *ledger.CommitNotification, 1)
			notifier := newTestNotifier(commitSend)
			defer notifier.close()

			done := make(chan struct{})
			commitReceive, err := notifier.notify(done, "CHANNEL_NAME", "TX_ID")
			require.NoError(t, err)

			close(done)
			commitSend <- &ledger.CommitNotification{
				BlockNumber: 1,
				TxsInfo: []*ledger.CommitNotificationTxInfo{
					{
						TxID:           "TX_ID",
						ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
					},
				},
			}
			_, ok := <-commitReceive

			require.False(t, ok, "Expected notification channel to be closed but receive was successful")
		})

		t.Run("multiple listeners receive notifications", func(t *testing.T) {
			commitSend := make(chan *ledger.CommitNotification, 1)
			notifier := newTestNotifier(commitSend)
			defer notifier.close()

			commitReceive1, err := notifier.notify(nil, "CHANNEL_NAME", "TX_ID")
			require.NoError(t, err)

			commitReceive2, err := notifier.notify(nil, "CHANNEL_NAME", "TX_ID")
			require.NoError(t, err)

			commitSend <- &ledger.CommitNotification{
				BlockNumber: 1,
				TxsInfo: []*ledger.CommitNotificationTxInfo{
					{
						TxID:           "TX_ID",
						ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
					},
				},
			}
			actual1 := <-commitReceive1
			actual2 := <-commitReceive2

			expected := notification{
				BlockNumber:    1,
				TransactionID:  "TX_ID",
				ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
			}
			require.EqualValues(t, expected, actual1)
			require.EqualValues(t, expected, actual2)
		})

		t.Run("multiple listeners can stop listening independently", func(t *testing.T) {
			commitSend := make(chan *ledger.CommitNotification, 1)
			notifier := newTestNotifier(commitSend)
			defer notifier.close()

			done := make(chan struct{})
			commitReceive1, err := notifier.notify(done, "CHANNEL_NAME", "TX_ID")
			require.NoError(t, err)

			commitReceive2, err := notifier.notify(nil, "CHANNEL_NAME", "TX_ID")
			require.NoError(t, err)

			close(done)
			commitSend <- &ledger.CommitNotification{
				BlockNumber: 1,
				TxsInfo: []*ledger.CommitNotificationTxInfo{
					{
						TxID:           "TX_ID",
						ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
					},
				},
			}
			_, ok1 := <-commitReceive1
			_, ok2 := <-commitReceive2

			require.False(t, ok1, "Expected notification channel to be closed but receive was successful")
			require.True(t, ok2, "Expected notification channel to deliver a result but was closed")
		})

		t.Run("passes open done channel to notification supplier", func(t *testing.T) {
			supplier := &MockNotificationSupplier{}
			supplier.On("CommitNotifications", mock.Anything, mock.Anything).
				Return(nil, nil).
				Once()

			notifier := NewNotifier(supplier)
			defer notifier.close()

			_, err := notifier.notify(nil, "CHANNEL_NAME", "TX_ID")
			require.NoError(t, err)

			supplier.AssertExpectations(t)

			done := supplier.Calls[0].Arguments.Get(0).(<-chan struct{})
			select {
			case <-done:
				require.FailNow(t, "Expected done channel to be open but was closed")
			default:
			}
		})

		t.Run("passes channel name to notification supplier", func(t *testing.T) {
			supplier := &MockNotificationSupplier{}
			supplier.On("CommitNotifications", mock.Anything, "CHANNEL_NAME").
				Return(nil, nil).
				Once()
			notifier := NewNotifier(supplier)
			defer notifier.close()

			_, err := notifier.notify(nil, "CHANNEL_NAME", "TX_ID")
			require.NoError(t, err)

			supplier.AssertExpectations(t)
		})

		t.Run("stops notification if supplier stops", func(t *testing.T) {
			commitSend := make(chan *ledger.CommitNotification, 1)
			notifier := newTestNotifier(commitSend)
			defer notifier.close()

			commitReceive, err := notifier.notify(nil, "CHANNEL_NAME", "TX_ID")
			require.NoError(t, err)

			close(commitSend)
			_, ok := <-commitReceive

			require.False(t, ok, "Expected notification channel to be closed but receive was successful")
		})

		t.Run("can attach new listener after supplier stops", func(t *testing.T) {
			commitSend1 := make(chan *ledger.CommitNotification, 1)
			commitSend2 := make(chan *ledger.CommitNotification, 1)

			notifier := newTestNotifier(commitSend1, commitSend2)
			defer notifier.close()

			commitReceive1, err := notifier.notify(nil, "CHANNEL_NAME", "TX_ID")
			require.NoError(t, err)

			close(commitSend1)
			_, ok := <-commitReceive1
			require.False(t, ok, "Expected notification channel to be closed but receive was successful")

			commitReceive2, err := notifier.notify(nil, "CHANNEL_NAME", "TX_ID")
			require.NoError(t, err)

			commitSend2 <- &ledger.CommitNotification{
				BlockNumber: 1,
				TxsInfo: []*ledger.CommitNotificationTxInfo{
					{
						TxID:           "TX_ID",
						ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
					},
				},
			}

			actual, ok := <-commitReceive2
			require.True(t, ok, "Expected notification channel to deliver a result but was closed")

			expected := notification{
				BlockNumber:    1,
				TransactionID:  "TX_ID",
				ValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
			}
			require.EqualValues(t, expected, actual)
		})
	})

	t.Run("Close", func(t *testing.T) {
		t.Run("stops all listeners", func(t *testing.T) {
			commitSend := make(chan *ledger.CommitNotification)
			notifier := newTestNotifier(commitSend)

			commitReceive, err := notifier.notify(nil, "CHANNEL_NAME", "TX_ID")
			require.NoError(t, err)
			notifier.close()

			_, ok := <-commitReceive

			require.False(t, ok, "Expected notification channel to be closed but receive was successful")
		})

		t.Run("idempotent", func(t *testing.T) {
			commitSend := make(chan *ledger.CommitNotification)
			notifier := newTestNotifier(commitSend)
			notifier.close()

			require.NotPanics(t, func() {
				notifier.close()
			})
		})

		t.Run("stops notification supplier", func(t *testing.T) {
			supplier := &MockNotificationSupplier{}
			supplier.On("CommitNotifications", mock.Anything, mock.Anything).
				Return(nil, nil).
				Once()
			notifier := NewNotifier(supplier)

			_, err := notifier.notify(nil, "CHANNEL_NAME", "TX_ID")
			require.NoError(t, err)
			notifier.close()

			supplier.AssertExpectations(t)

			done := supplier.Calls[0].Arguments.Get(0).(<-chan struct{})
			_, ok := <-done
			require.False(t, ok, "Expected notification supplier done channel to be closed but receive was successful")
		})
	})
}
