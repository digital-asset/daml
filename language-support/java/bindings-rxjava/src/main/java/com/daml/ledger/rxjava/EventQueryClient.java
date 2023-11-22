// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.GetEventsByContractIdResponse;
import com.daml.ledger.javaapi.data.GetEventsByContractKeyResponse;
import com.daml.ledger.javaapi.data.Identifier;
import com.daml.ledger.javaapi.data.Value;
import io.reactivex.Single;
import java.util.Set;

/** An RxJava version of {@link com.daml.ledger.api.v1.PackageServiceGrpc} */
public interface EventQueryClient {
  Single<GetEventsByContractIdResponse> getEventsByContractId(
      String contractId, Set<String> requestingParties);

  Single<GetEventsByContractIdResponse> getEventsByContractId(
      String contractId, Set<String> requestingParties, String accessToken);

  Single<GetEventsByContractKeyResponse> getEventsByContractKey(
      Value contractKey,
      Identifier templateId,
      Set<String> requestingParties,
      String continuationToken);

  Single<GetEventsByContractKeyResponse> getEventsByContractKey(
      Value contractKey,
      Identifier templateId,
      Set<String> requestingParties,
      String continuationToken,
      String accessToken);
}
