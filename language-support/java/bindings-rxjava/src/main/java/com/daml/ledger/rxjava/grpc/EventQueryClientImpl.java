// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.ledger.api.v1.EventQueryServiceGrpc;
import com.daml.ledger.api.v1.EventQueryServiceOuterClass;
import com.daml.ledger.javaapi.data.GetEventsByContractIdResponse;
import com.daml.ledger.javaapi.data.GetEventsByContractKeyResponse;
import com.daml.ledger.javaapi.data.Identifier;
import com.daml.ledger.javaapi.data.Value;
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
    EventQueryServiceOuterClass.GetEventsByContractIdRequest request =
        EventQueryServiceOuterClass.GetEventsByContractIdRequest.newBuilder()
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

  private Single<GetEventsByContractKeyResponse> getEventsByContractKey(
      Value contractKey,
      Identifier templateId,
      Set<String> requestingParties,
      String continuationToken,
      Optional<String> accessToken) {
    EventQueryServiceOuterClass.GetEventsByContractKeyRequest request =
        EventQueryServiceOuterClass.GetEventsByContractKeyRequest.newBuilder()
            .setContractKey(contractKey.toProto())
            .setTemplateId(templateId.toProto())
            .setContinuationToken(continuationToken)
            .addAllRequestingParties(requestingParties)
            .build();

    return Single.fromFuture(
            StubHelper.authenticating(this.serviceStub, accessToken)
                .getEventsByContractKey(request))
        .map(GetEventsByContractKeyResponse::fromProto);
  }

  @Override
  public Single<GetEventsByContractKeyResponse> getEventsByContractKey(
      Value contractKey,
      Identifier templateId,
      Set<String> requestingParties,
      String continuationToken) {
    return getEventsByContractKey(
        contractKey, templateId, requestingParties, continuationToken, Optional.empty());
  }

  @Override
  public Single<GetEventsByContractKeyResponse> getEventsByContractKey(
      Value contractKey,
      Identifier templateId,
      Set<String> requestingParties,
      String continuationToken,
      String accessToken) {
    return getEventsByContractKey(
        contractKey, templateId, requestingParties, continuationToken, Optional.of(accessToken));
  }
}
