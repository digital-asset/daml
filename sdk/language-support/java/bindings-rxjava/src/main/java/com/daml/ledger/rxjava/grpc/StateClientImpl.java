// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
      @NonNull TransactionFilter filter,
      boolean verbose,
      Long activeAtOffset,
      @NonNull Optional<String> accessToken) {
    StateServiceOuterClass.GetActiveContractsRequest request =
        new GetActiveContractsRequest(filter, verbose, activeAtOffset).toProto();
    return ClientPublisherFlowable.create(
            request,
            StubHelper.authenticating(this.serviceStub, accessToken)::getActiveContracts,
            sequencerFactory)
        .map(GetActiveContractsResponse::fromProto);
  }

  @Override
  public Flowable<GetActiveContractsResponse> getActiveContracts(
      @NonNull TransactionFilter filter, boolean verbose, Long activeAtOffset) {
    return getActiveContracts(filter, verbose, activeAtOffset, Optional.empty());
  }

  @Override
  public Flowable<GetActiveContractsResponse> getActiveContracts(
      @NonNull TransactionFilter filter,
      boolean verbose,
      Long activeAtOffset,
      @NonNull String accessToken) {
    return getActiveContracts(filter, verbose, activeAtOffset, Optional.of(accessToken));
  }

  @Override
  public Flowable<GetActiveContractsResponse> getActiveContracts(
      @NonNull EventFormat eventFormat, Long activeAtOffset) {
    return getActiveContracts(eventFormat, activeAtOffset, Optional.empty());
  }

  @Override
  public Flowable<GetActiveContractsResponse> getActiveContracts(
      @NonNull EventFormat eventFormat, Long activeAtOffset, @NonNull String accessToken) {
    return getActiveContracts(eventFormat, activeAtOffset, Optional.of(accessToken));
  }

  private Flowable<GetActiveContractsResponse> getActiveContracts(
      @NonNull EventFormat eventFormat,
      Long activeAtOffset,
      @NonNull Optional<String> accessToken) {
    StateServiceOuterClass.GetActiveContractsRequest request =
        new GetActiveContractsRequest(eventFormat, activeAtOffset).toProto();
    return ClientPublisherFlowable.create(
            request,
            StubHelper.authenticating(this.serviceStub, accessToken)::getActiveContracts,
            sequencerFactory)
        .map(GetActiveContractsResponse::fromProto);
  }

  private <Ct> Flowable<ActiveContracts<Ct>> getActiveContracts(
      ContractFilter<Ct> contractFilter,
      Set<String> parties,
      boolean verbose,
      Long activeAtOffset,
      Optional<String> accessToken) {
    TransactionFilter filter = contractFilter.transactionFilter(Optional.of(parties));

    Flowable<GetActiveContractsResponse> responses =
        getActiveContracts(filter, verbose, activeAtOffset, accessToken);
    return responses.map(
        response -> {
          List<Ct> activeContracts =
              response.getContractEntry().stream()
                  .map(ce -> contractFilter.toContract(ce.getCreatedEvent()))
                  .collect(Collectors.toList());
          return new ActiveContracts<>(activeContracts, response.getWorkflowId());
        });
  }

  @Override
  public <Ct> Flowable<ActiveContracts<Ct>> getActiveContracts(
      ContractFilter<Ct> contractFilter,
      Set<String> parties,
      boolean verbose,
      Long activeAtOffset) {
    return getActiveContracts(contractFilter, parties, verbose, activeAtOffset, Optional.empty());
  }

  @Override
  public <Ct> Flowable<ActiveContracts<Ct>> getActiveContracts(
      ContractFilter<Ct> contractFilter,
      Set<String> parties,
      boolean verbose,
      Long activeAtOffset,
      String accessToken) {
    return getActiveContracts(
        contractFilter, parties, verbose, activeAtOffset, Optional.of(accessToken));
  }

  private Single<Long> getLedgerEnd(Optional<String> accessToken) {
    StateServiceOuterClass.GetLedgerEndRequest request =
        StateServiceOuterClass.GetLedgerEndRequest.newBuilder().build();
    return Single.fromFuture(
            StubHelper.authenticating(this.serviceFutureStub, accessToken).getLedgerEnd(request))
        .map(GetLedgerEndResponse::fromProto)
        .map(GetLedgerEndResponse::getOffset);
  }

  @Override
  public Single<Long> getLedgerEnd() {
    return getLedgerEnd(Optional.empty());
  }

  @Override
  public Single<Long> getLedgerEnd(String accessToken) {
    return getLedgerEnd(Optional.of(accessToken));
  }
}
