// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.ledger.api.v2.EventQueryServiceGrpc;
import com.daml.ledger.api.v2.EventQueryServiceOuterClass.GetEventsByContractIdRequest;
import com.daml.ledger.javaapi.data.GetEventsByContractIdResponse;
import com.daml.ledger.rxjava.EventQueryClient;
import com.daml.ledger.rxjava.grpc.helpers.StubHelper;
import io.grpc.Channel;
import io.reactivex.Single;
import java.util.Optional;
import java.util.Set;

public class EventQueryClientImpl implements EventQueryClient {

  private final EventQueryServiceGrpc.EventQueryServiceFutureStub serviceStub;

  public EventQueryClientImpl(Channel channel, Optional<String> accessToken) {
    serviceStub =
        StubHelper.authenticating(EventQueryServiceGrpc.newFutureStub(channel), accessToken);
  }

  private Single<GetEventsByContractIdResponse> getEventsByContractId(
      String contractId, Set<String> requestingParties, Optional<String> accessToken) {
    GetEventsByContractIdRequest request =
        GetEventsByContractIdRequest.newBuilder()
            .setContractId(contractId)
            .addAllRequestingParties(requestingParties)
            .build();
    return Single.fromFuture(
            StubHelper.authenticating(this.serviceStub, accessToken).getEventsByContractId(request))
        .map(GetEventsByContractIdResponse::fromProto);
  }

  @Override
  public Single<GetEventsByContractIdResponse> getEventsByContractId(
      String contractId, Set<String> requestingParties) {
    return getEventsByContractId(contractId, requestingParties, Optional.empty());
  }

  @Override
  public Single<GetEventsByContractIdResponse> getEventsByContractId(
      String contractId, Set<String> requestingParties, String accessToken) {
    return getEventsByContractId(contractId, requestingParties, Optional.of(accessToken));
  }
}
