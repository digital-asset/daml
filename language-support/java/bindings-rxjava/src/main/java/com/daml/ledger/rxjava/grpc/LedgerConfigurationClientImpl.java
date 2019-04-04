// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.ledger.rxjava.LedgerConfigurationClient;
import com.daml.ledger.rxjava.util.ClientPublisherFlowable;
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory;
import com.digitalasset.ledger.api.v1.LedgerConfigurationServiceGrpc;
import com.digitalasset.ledger.api.v1.LedgerConfigurationServiceOuterClass;
import io.grpc.Channel;
import io.reactivex.Flowable;

public class LedgerConfigurationClientImpl implements LedgerConfigurationClient {

    private final String ledgerId;
    private final LedgerConfigurationServiceGrpc.LedgerConfigurationServiceStub serviceStub;
    private final ExecutionSequencerFactory sequencerFactory;

    public LedgerConfigurationClientImpl(String ledgerId, Channel channel, ExecutionSequencerFactory sequencerFactory) {
        this.ledgerId = ledgerId;
        this.sequencerFactory = sequencerFactory;
        serviceStub = LedgerConfigurationServiceGrpc.newStub(channel);
    }

    @Override
    public Flowable<LedgerConfigurationServiceOuterClass.LedgerConfiguration> getLedgerConfiguration() {
        LedgerConfigurationServiceOuterClass.GetLedgerConfigurationRequest request = LedgerConfigurationServiceOuterClass.GetLedgerConfigurationRequest.newBuilder().setLedgerId(ledgerId).build();
        return ClientPublisherFlowable
                .create(request, serviceStub::getLedgerConfiguration, sequencerFactory)
                .map(LedgerConfigurationServiceOuterClass.GetLedgerConfigurationResponse::getLedgerConfiguration);
    }
}
