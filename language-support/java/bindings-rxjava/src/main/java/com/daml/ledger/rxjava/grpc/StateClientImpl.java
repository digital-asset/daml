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

  private Flowable<GetActiveContractsResponseV2> getActiveContracts(
      @NonNull TransactionFilterV2 filter, boolean verbose, @NonNull Optional<String> accessToken) {
    StateServiceOuterClass.GetActiveContractsRequest request =
        new GetActiveContractsRequestV2(filter, verbose, "").toProto();
    return ClientPublisherFlowable.create(
            request,
            StubHelper.authenticating(this.serviceStub, accessToken)::getActiveContracts,
            sequencerFactory)
        .map(GetActiveContractsResponseV2::fromProto);
  }

  @Override
  public Flowable<GetActiveContractsResponseV2> getActiveContracts(
      @NonNull TransactionFilterV2 filter, boolean verbose) {
    return getActiveContracts(filter, verbose, Optional.empty());
  }

  @Override
  public Flowable<GetActiveContractsResponseV2> getActiveContracts(
      @NonNull TransactionFilterV2 filter, boolean verbose, @NonNull String accessToken) {
    return getActiveContracts(filter, verbose, Optional.of(accessToken));
  }

  private <Ct> Flowable<ActiveContracts<Ct>> getActiveContracts(
      ContractFilter<Ct> contractFilter,
      Set<String> parties,
      boolean verbose,
      Optional<String> accessToken) {
    TransactionFilterV2 filter = contractFilter.transactionFilterV2(parties);

    Flowable<GetActiveContractsResponseV2> responses =
        getActiveContracts(filter, verbose, accessToken);
    return responses.map(
        response -> {
          List<Ct> activeContracts =
              List.of(contractFilter.toContract(response.getContractEntry().getCreatedEvent()));
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

  private Single<ParticipantOffsetV2> getLedgerEnd(Optional<String> accessToken) {
    StateServiceOuterClass.GetLedgerEndRequest request =
        StateServiceOuterClass.GetLedgerEndRequest.newBuilder().build();
    return Single.fromFuture(
            StubHelper.authenticating(this.serviceFutureStub, accessToken).getLedgerEnd(request))
        .map(GetLedgerEndResponseV2::fromProto)
        .map(GetLedgerEndResponseV2::getOffset);
  }

  @Override
  public Single<ParticipantOffsetV2> getLedgerEnd() {
    return getLedgerEnd(Optional.empty());
  }

  @Override
  public Single<ParticipantOffsetV2> getLedgerEnd(String accessToken) {
    return getLedgerEnd(Optional.of(accessToken));
  }
}
