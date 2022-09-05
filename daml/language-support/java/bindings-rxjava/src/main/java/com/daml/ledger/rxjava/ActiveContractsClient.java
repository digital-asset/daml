// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.GetActiveContractsResponse;
import com.daml.ledger.javaapi.data.TransactionFilter;
import io.reactivex.Flowable;

/** An RxJava version of {@link com.daml.ledger.api.v1.ActiveContractsServiceGrpc} */
public interface ActiveContractsClient {

  Flowable<GetActiveContractsResponse> getActiveContracts(
      TransactionFilter filter, boolean verbose);

  Flowable<GetActiveContractsResponse> getActiveContracts(
      TransactionFilter filter, boolean verbose, String accessToken);
}
