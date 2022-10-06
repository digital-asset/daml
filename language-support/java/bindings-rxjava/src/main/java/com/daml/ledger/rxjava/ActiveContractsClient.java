// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.ActiveContracts;
import com.daml.ledger.javaapi.data.GetActiveContractsResponse;
import com.daml.ledger.javaapi.data.TransactionFilter;
import io.reactivex.Flowable;
import java.util.Set;

/** An RxJava version of {@link com.daml.ledger.api.v1.ActiveContractsServiceGrpc} */
public interface ActiveContractsClient {

  Flowable<GetActiveContractsResponse> getActiveContracts(
      TransactionFilter filter, boolean verbose);

  Flowable<GetActiveContractsResponse> getActiveContracts(
      TransactionFilter filter, boolean verbose, String accessToken);

  <Ct> Flowable<ActiveContracts<Ct>> getActiveContracts(
      ContractUtil<Ct> contractUtil, Set<String> parties, boolean verbose);

  <Ct> Flowable<ActiveContracts<Ct>> getActiveContracts(
      ContractUtil<Ct> contractUtil, Set<String> parties, boolean verbose, String accessToken);
}
