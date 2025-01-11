// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.*;
import io.reactivex.Flowable;
import io.reactivex.Single;
import java.util.Set;

/** An RxJava version of {@link com.daml.ledger.api.v2.StateServiceGrpc} */
public interface StateClient {

  Flowable<GetActiveContractsResponse> getActiveContracts(
      TransactionFilter filter, boolean verbose, Long activeAtOffset);

  Flowable<GetActiveContractsResponse> getActiveContracts(
      TransactionFilter filter, boolean verbose, Long activeAtOffset, String accessToken);

  /**
   * Get active Contracts
   *
   * @param contractFilter Utilities for specified type of contract. It can be instantiated with
   *     <code>ContractTypeCompanion</code>
   * @param parties Set of parties to be included in the transaction filter.
   * @param verbose If enabled, values served over the API will contain more information than
   *     strictly necessary to interpret the data.
   * @param activeAtOffset The offset at which the snapshot of the active contracts will be
   *     computed.
   * @return Flowable of active contracts of type <code>Ct</code>
   */
  <Ct> Flowable<ActiveContracts<Ct>> getActiveContracts(
      ContractFilter<Ct> contractFilter, Set<String> parties, boolean verbose, Long activeAtOffset);

  /**
   * Get active Contracts
   *
   * @param contractFilter Utilities for specified type of contract. It can be instantiated with
   *     <code>ContractTypeCompanion</code>
   * @param parties Set of parties to be included in the transaction filter.
   * @param verbose If enabled, values served over the API will contain more information than
   *     strictly necessary to interpret the data.
   * @param activeAtOffset The offset at which the snapshot of the active contracts will be
   *     computed.
   * @param accessToken Access token for authentication.
   * @return Active contracts of type <code>Ct</code>
   */
  <Ct> Flowable<ActiveContracts<Ct>> getActiveContracts(
      ContractFilter<Ct> contractFilter,
      Set<String> parties,
      boolean verbose,
      Long activeAtOffset,
      String accessToken);

  Single<Long> getLedgerEnd();

  Single<Long> getLedgerEnd(String accessToken);
}
