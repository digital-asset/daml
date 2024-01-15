// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.grpc.adapter.ExecutionSequencerFactory;
import com.daml.ledger.api.v1.LedgerConfigurationServiceGrpc;
import com.daml.ledger.api.v1.LedgerConfigurationServiceOuterClass;
import com.daml.ledger.rxjava.LedgerConfigurationClient;
import com.daml.ledger.rxjava.grpc.helpers.StubHelper;
import com.daml.ledger.rxjava.util.ClientPublisherFlowable;
import io.grpc.Channel;
import io.reactivex.Flowable;
import java.util.Optional;

public class LedgerConfigurationClientImpl implements LedgerConfigurationClient {

  private final String ledgerId;
  private final LedgerConfigurationServiceGrpc.LedgerConfigurationServiceStub serviceStub;
  private final ExecutionSequencerFactory sequencerFactory;

  public LedgerConfigurationClientImpl(
      String ledgerId,
      Channel channel,
      ExecutionSequencerFactory sequencerFactory,
      Optional<String> accessToken) {
    this.ledgerId = ledgerId;
    this.sequencerFactory = sequencerFactory;
    this.serviceStub =
        StubHelper.authenticating(LedgerConfigurationServiceGrpc.newStub(channel), accessToken);
  }

  private Flowable<LedgerConfigurationServiceOuterClass.LedgerConfiguration> getLedgerConfiguration(
      Optional<String> accessToken) {
    LedgerConfigurationServiceOuterClass.GetLedgerConfigurationRequest request =
        LedgerConfigurationServiceOuterClass.GetLedgerConfigurationRequest.newBuilder()
            .setLedgerId(ledgerId)
            .build();
    return ClientPublisherFlowable.create(
            request,
            StubHelper.authenticating(this.serviceStub, accessToken)::getLedgerConfiguration,
            sequencerFactory)
        .map(
            LedgerConfigurationServiceOuterClass.GetLedgerConfigurationResponse
                ::getLedgerConfiguration);
  }

  @Override
  public Flowable<LedgerConfigurationServiceOuterClass.LedgerConfiguration>
      getLedgerConfiguration() {
    return getLedgerConfiguration(Optional.empty());
  }

  @Override
  public Flowable<LedgerConfigurationServiceOuterClass.LedgerConfiguration> getLedgerConfiguration(
      String accessToken) {
    return getLedgerConfiguration(Optional.of(accessToken));
  }
}
