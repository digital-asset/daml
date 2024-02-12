// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.GetEventsByContractIdResponse;
import io.reactivex.Single;
import java.util.Set;

/** An RxJava version of {@link com.daml.ledger.api.v1.PackageServiceGrpc} */
public interface EventQueryClient {
  Single<GetEventsByContractIdResponse> getEventsByContractId(
      String contractId, Set<String> requestingParties);

  Single<GetEventsByContractIdResponse> getEventsByContractId(
      String contractId, Set<String> requestingParties, String accessToken);
}
