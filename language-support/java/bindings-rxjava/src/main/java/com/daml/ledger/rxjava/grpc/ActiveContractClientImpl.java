// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.grpc.adapter.ExecutionSequencerFactory;
import com.daml.ledger.api.v1.ActiveContractsServiceGrpc;
import com.daml.ledger.api.v1.ActiveContractsServiceOuterClass;
import com.daml.ledger.javaapi.data.ActiveContracts;
import com.daml.ledger.javaapi.data.ContractFilter;
import com.daml.ledger.javaapi.data.GetActiveContractsRequest;
import com.daml.ledger.javaapi.data.GetActiveContractsResponse;
import com.daml.ledger.javaapi.data.TransactionFilter;
import com.daml.ledger.rxjava.ActiveContractsClient;
import com.daml.ledger.rxjava.grpc.helpers.StubHelper;
import com.daml.ledger.rxjava.util.ClientPublisherFlowable;
import io.grpc.Channel;
import io.reactivex.Flowable;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;

public class ActiveContractClientImpl implements ActiveContractsClient {

  private final String ledgerId;
  private final ActiveContractsServiceGrpc.ActiveContractsServiceStub serviceStub;
  private ExecutionSequencerFactory sequencerFactory;

  public ActiveContractClientImpl(
      String ledgerId,
      Channel channel,
      ExecutionSequencerFactory sequencerFactory,
      Optional<String> accessToken) {
    this.ledgerId = ledgerId;
    this.sequencerFactory = sequencerFactory;
    this.serviceStub =
        StubHelper.authenticating(ActiveContractsServiceGrpc.newStub(channel), accessToken);
  }

  private Flowable<GetActiveContractsResponse> getActiveContracts(
      @NonNull TransactionFilter filter, boolean verbose, @NonNull Optional<String> accessToken) {
    ActiveContractsServiceOuterClass.GetActiveContractsRequest request =
        new GetActiveContractsRequest(ledgerId, filter, verbose).toProto();
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
              response.getCreatedEvents().stream()
                  .map(contractFilter::toContract)
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
}
