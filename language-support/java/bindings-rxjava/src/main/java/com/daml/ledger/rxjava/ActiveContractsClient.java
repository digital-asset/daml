// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.GetActiveContractsResponse;
import com.daml.ledger.javaapi.data.TransactionFilter;
import io.reactivex.Flowable;


/**
 * An RxJava version of {@link com.digitalasset.ledger.api.v1.ActiveContractsServiceGrpc}
 */
public interface ActiveContractsClient {

    Flowable<GetActiveContractsResponse> getActiveContracts(TransactionFilter filter, boolean verbose);
}
