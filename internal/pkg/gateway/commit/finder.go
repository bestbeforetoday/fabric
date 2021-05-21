/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package commit provides an implementation for finding transaction commit status that is specific to the Gateway
// embedded within a peer.
package commit

import (
	"context"

	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/pkg/errors"
)

type Status struct {
	Code        peer.TxValidationCode
	BlockNumber uint64
}

// QueryProvider provides status of previously committed transactions on a given channel. An error is returned if the
// transaction is not present in the ledger.
type QueryProvider interface {
	TransactionStatus(channelName string, transactionID string) (*Status, error)
}

// Finder is used to obtain transaction status.
type Finder struct {
	Query    QueryProvider
	Notifier *Notifier
}

// TransactionStatus provides status of a specified transaction on a given channel. If the transaction has already
// committed, the status is returned immediately; otherwise this call blocks waiting for the transaction to be
// committed or the context to be cancelled.
func (finder *Finder) TransactionStatus(ctx context.Context, channelName string, transactionID string) (*Status, error) {
	// Set up notifier first to ensure no commit missed after completing query
	notifyDone := make(chan struct{})
	defer close(notifyDone)
	commitReceive, err := finder.Notifier.notify(notifyDone, channelName, transactionID)
	if err != nil {
		return nil, err
	}

	if status, err := finder.Query.TransactionStatus(channelName, transactionID); err == nil {
		return status, nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case commit, ok := <-commitReceive:
		if !ok {
			return nil, errors.New("unexpected close of commit notification channel")
		}
		status := &Status{
			Code:        commit.ValidationCode,
			BlockNumber: commit.BlockNumber,
		}
		return status, nil
	}
}
