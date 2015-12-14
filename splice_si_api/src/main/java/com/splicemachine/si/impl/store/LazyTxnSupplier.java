package com.splicemachine.si.impl.store;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.txn.LazyTxnView;

import java.io.IOException;

/**
 * Transaction Store which constructs lazy Txn elements when needed
 * to reduce the performance cost of doing remote lookups.
 *
 * @author Scott Fines
 * Date: 6/19/14
 */
public class LazyTxnSupplier implements TxnSupplier {
		private final TxnSupplier delegate;

		public LazyTxnSupplier(TxnSupplier delegate) {
				this.delegate = delegate;
		}

		@Override
		public TxnView getTransaction(long txnId) throws IOException {
				if(txnId<0) return Txn.ROOT_TRANSACTION;
				/*
				 * When the delegate contains the transaction in it's local cache,
				 * it should be very inexpensive to perform a direct lookup. Therefore,
				 * we can save an extra object creation when it's contained in the cache
				 * by just delegating in that case.
				 *
				 * When it's not there, we would like to defer the lookup of the transaction,
				 * in case it's not needed (e.g. in case all values are present in the
				 * child or whatever, so defaults are never needed).
				 */
        TxnView cached = delegate.getTransactionFromCache(txnId);
        if(cached!=null) return cached;

				return new LazyTxnView(txnId,delegate);
		}

		@Override
		public TxnView getTransaction(long txnId, boolean getDestinationTables) throws IOException {
				return getTransaction(txnId);
		}

		@Override
		public boolean transactionCached(long txnId) {
				return delegate.transactionCached(txnId);
		}

		@Override
		public void cache(TxnView toCache) {
        delegate.cache(toCache);
		}

    @Override
    public TxnView getTransactionFromCache(long txnId) {
        return delegate.getTransactionFromCache(txnId);
    }
}
