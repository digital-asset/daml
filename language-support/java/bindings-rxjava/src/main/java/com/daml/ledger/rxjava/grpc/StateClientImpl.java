// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.grpc.adapter.ExecutionSequencerFactory;
import com.daml.ledger.api.v2.StateServiceGrpc;
import com.daml.ledger.api.v2.StateServiceOuterClass;
import com.daml.ledger.javaapi.data.*;
import com.daml.ledger.rxjava.StateClient;
import com.daml.ledger.rxjava.grpc.helpers.StubHelper;
import com.daml.ledger.rxjava.util.ClientPublisherFlowable;
import io.grpc.Channel;
import io.reactivex.Flowable;
import io.reactivex.Single;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;

public class StateClientImpl implements StateClient {

  private final StateServiceGrpc.StateServiceStub serviceStub;
  private final StateServiceGrpc.StateServiceFutureStub serviceFutureStub;
  private ExecutionSequencerFactory sequencerFactory;

  public StateClientImpl(
      Channel channel, ExecutionSequencerFactory sequencerFactory, Optional<String> accessToken) {
    this.sequencerFactory = sequencerFactory;
    this.serviceStub = StubHelper.authenticating(StateServiceGrpc.newStub(channel), accessToken);
    this.serviceFutureStub =
        StubHelper.authenticating(StateServiceGrpc.newFutureStub(channel), accessToken);
  }

  private Flowable<GetActiveContractsResponse> getActiveContracts(
      @NonNull TransactionFilter filter, boolean verbose, @NonNull Optional<String> accessToken) {
    StateServiceOuterClass.GetActiveContractsRequest request =
        new GetActiveContractsRequest(filter, verbose, "").toProto();
    return ClientPublisherFlowable.create(
            request,
            StubHelper.authenticating(this.serviceStub, accessToken)::getActiveContracts,
            sequencerFactory)
        .map(GetActiveContractsResponse::fromProto);
  }

  @Override
  public Flowable<GetActiveContractsResponse> getActiveContracts(
      @NonNull TransactionFilter filter, boolean verbose) {
    return getActiveContracts(filter, verbose, Optional.empty());
  }

  @Override
  public Flowable<GetActiveContractsResponse> getActiveContracts(
      @NonNull TransactionFilter filter, boolean verbose, @NonNull String accessToken) {
    return getActiveContracts(filter, verbose, Optional.of(accessToken));
  }

  private <Ct> Flowable<ActiveContracts<Ct>> getActiveContracts(
      ContractFilter<Ct> contractFilter,
      Set<String> parties,
      boolean verbose,
      Optional<String> accessToken) {
    TransactionFilter filter = contractFilter.transactionFilter(parties);

    Flowable<GetActiveContractsResponse> responses =
        getActiveContracts(filter, verbose, accessToken);
    return responses.map(
        response -> {
          List<Ct> activeContracts =
              response.getContractEntry().stream()
                  .map(ce -> contractFilter.toContract(ce.getCreatedEvent()))
                  .collect(Collectors.toList());
          return new ActiveContracts<>(
              response.getOffset(), activeContracts, response.getWorkflowId());
        });
  }

  @Override
  public <Ct> Flowable<ActiveContracts<Ct>> getActiveContracts(
      ContractFilter<Ct> contractFilter, Set<String> parties, boolean verbose) {
    return getActiveContracts(contractFilter, parties, verbose, Optional.empty());
  }

  @Override
  public <Ct> Flowable<ActiveContracts<Ct>> getActiveContracts(
      ContractFilter<Ct> contractFilter, Set<String> parties, boolean verbose, String accessToken) {
    return getActiveContracts(contractFilter, parties, verbose, Optional.of(accessToken));
  }

  private Single<ParticipantOffset> getLedgerEnd(Optional<String> accessToken) {
    StateServiceOuterClass.GetLedgerEndRequest request =
        StateServiceOuterClass.GetLedgerEndRequest.newBuilder().build();
    return Single.fromFuture(
            StubHelper.authenticating(this.serviceFutureStub, accessToken).getLedgerEnd(request))
        .map(GetLedgerEndResponse::fromProto)
        .map(GetLedgerEndResponse::getOffset);
  }

  @Override
  public Single<ParticipantOffset> getLedgerEnd() {
    return getLedgerEnd(Optional.empty());
  }

  @Override
  public Single<ParticipantOffset> getLedgerEnd(String accessToken) {
    return getLedgerEnd(Optional.of(accessToken));
  }
}
