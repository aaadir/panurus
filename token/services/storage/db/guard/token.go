/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package guard

import (
	"context"

	tokendriver "github.com/LFDT-Panurus/panurus/token/driver"
	driver "github.com/LFDT-Panurus/panurus/token/services/storage/db/driver"
	"github.com/LFDT-Panurus/panurus/token/token"
)

// WrapToken wraps a token store with the guard policy.
func WrapToken(s driver.TokenStore, p Policy) driver.TokenStore {
	if s == nil {
		return s
	}

	return &guardedTokenStore{TokenStore: s, policy: p}
}

type guardedTokenStore struct {
	driver.TokenStore
	policy Policy
}

// --- streaming reads: cap rows ---

func (g *guardedTokenStore) UnspentTokensIterator(ctx context.Context) (tokendriver.UnspentTokensIterator, error) {
	it, err := g.TokenStore.UnspentTokensIterator(ctx)
	if err != nil {
		return nil, err
	}

	return LimitIterator(it, g.policy.MaxPageSize, "UnspentTokensIterator"), nil
}

func (g *guardedTokenStore) UnspentLedgerTokensIteratorBy(ctx context.Context) (tokendriver.LedgerTokensIterator, error) {
	it, err := g.TokenStore.UnspentLedgerTokensIteratorBy(ctx)
	if err != nil {
		return nil, err
	}

	return LimitIterator(it, g.policy.MaxPageSize, "UnspentLedgerTokensIteratorBy"), nil
}

func (g *guardedTokenStore) UnspentTokensIteratorBy(ctx context.Context, walletID string, tokenType token.Type) (tokendriver.UnspentTokensIterator, error) {
	it, err := g.TokenStore.UnspentTokensIteratorBy(ctx, walletID, tokenType)
	if err != nil {
		return nil, err
	}

	return LimitIterator(it, g.policy.MaxPageSize, "UnspentTokensIteratorBy"), nil
}

func (g *guardedTokenStore) SpendableTokensIteratorBy(ctx context.Context, walletID string, typ token.Type) (tokendriver.SpendableTokensIterator, error) {
	it, err := g.TokenStore.SpendableTokensIteratorBy(ctx, walletID, typ)
	if err != nil {
		return nil, err
	}

	return LimitIterator(it, g.policy.MaxPageSize, "SpendableTokensIteratorBy"), nil
}

func (g *guardedTokenStore) UnsupportedTokensIteratorBy(ctx context.Context, walletID string, tokenType token.Type) (tokendriver.UnsupportedTokensIterator, error) {
	it, err := g.TokenStore.UnsupportedTokensIteratorBy(ctx, walletID, tokenType)
	if err != nil {
		return nil, err
	}

	return LimitIterator(it, g.policy.MaxPageSize, "UnsupportedTokensIteratorBy"), nil
}

// --- writes: cap payload size ---

func (g *guardedTokenStore) StorePublicParams(ctx context.Context, raw []byte) error {
	if err := CheckPayload("StorePublicParams", len(raw), g.policy.MaxPayloadSize); err != nil {
		return err
	}

	return g.TokenStore.StorePublicParams(ctx, raw)
}

func (g *guardedTokenStore) StoreCertifications(ctx context.Context, certifications map[*token.ID][]byte) error {
	size := 0
	for _, v := range certifications {
		size += len(v)
	}
	if err := CheckPayload("StoreCertifications", size, g.policy.MaxPayloadSize); err != nil {
		return err
	}

	return g.TokenStore.StoreCertifications(ctx, certifications)
}

func (g *guardedTokenStore) NewTokenDBTransaction() (driver.TokenStoreTransaction, error) {
	w, err := g.TokenStore.NewTokenDBTransaction()
	if err != nil {
		return nil, err
	}

	return &guardedTokenStoreTx{TokenStoreTransaction: w, policy: g.policy}, nil
}

func (g *guardedTokenStore) ContinueTokenDBTransaction(tx driver.Transaction) (driver.TokenStoreTransaction, error) {
	w, err := g.TokenStore.ContinueTokenDBTransaction(tx)
	if err != nil {
		return nil, err
	}

	return &guardedTokenStoreTx{TokenStoreTransaction: w, policy: g.policy}, nil
}

type guardedTokenStoreTx struct {
	driver.TokenStoreTransaction
	policy Policy
}

func (w *guardedTokenStoreTx) StoreToken(ctx context.Context, tr driver.TokenRecord, owners []string) error {
	size := len(tr.TxID) + len(tr.IssuerRaw) + len(tr.OwnerRaw) + len(tr.OwnerIdentity) +
		len(tr.OwnerType) + len(tr.OwnerWalletID) + len(tr.Ledger) + len(tr.LedgerFormat) +
		len(tr.LedgerMetadata) + len(tr.Quantity) + len(tr.Type)
	for _, o := range owners {
		size += len(o)
	}
	if err := CheckPayload("StoreToken", size, w.policy.MaxPayloadSize); err != nil {
		return err
	}

	return w.TokenStoreTransaction.StoreToken(ctx, tr, owners)
}
