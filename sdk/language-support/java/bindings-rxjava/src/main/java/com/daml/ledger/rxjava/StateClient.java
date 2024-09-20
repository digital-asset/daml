// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.*;
import io.reactivex.Flowable;
import io.reactivex.Single;
import java.util.Optional;
import java.util.Set;

/** An RxJava version of {@link com.daml.ledger.api.v1.ActiveContractsServiceGrpc} */
public interface StateClient {

  Flowable<GetActiveContractsResponse> getActiveContracts(
      TransactionFilter filter, boolean verbose);

  Flowable<GetActiveContractsResponse> getActiveContracts(
      TransactionFilter filter, boolean verbose, String accessToken);

  /**
   * Get active Contracts
   *
   * @param contractFilter Utilities for specified type of contract. It can be instantiated with
   *     <code>ContractTypeCompanion</code>
   * @param parties Set of parties to be included in the transaction filter.
   * @param verbose If enabled, values served over the API will contain more information than
   *     strictly necessary to interpret the data.
   * @return Flowable of active contracts of type <code>Ct</code>
   */
  <Ct> Flowable<ActiveContracts<Ct>> getActiveContracts(
      ContractFilter<Ct> contractFilter, Set<String> parties, boolean verbose);

  /**
   * Get active Contracts
   *
   * @param contractFilter Utilities for specified type of contract. It can be instantiated with
   *     <code>ContractTypeCompanion</code>
   * @param parties Set of parties to be included in the transaction filter.
   * @param verbose If enabled, values served over the API will contain more information than
   *     strictly necessary to interpret the data.
   * @param accessToken Access token for authentication.
   * @return Active contracts of type <code>Ct</code>
   */
  <Ct> Flowable<ActiveContracts<Ct>> getActiveContracts(
      ContractFilter<Ct> contractFilter, Set<String> parties, boolean verbose, String accessToken);

  Single<Optional<Long>> getLedgerEnd();

  Single<Optional<Long>> getLedgerEnd(String accessToken);
}
