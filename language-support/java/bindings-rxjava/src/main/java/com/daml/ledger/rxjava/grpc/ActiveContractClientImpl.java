// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.ledger.javaapi.data.GetActiveContractsRequest;
import com.daml.ledger.javaapi.data.GetActiveContractsResponse;
import com.daml.ledger.javaapi.data.TransactionFilter;
import com.daml.ledger.rxjava.ActiveContractsClient;
import com.daml.ledger.rxjava.util.ClientPublisherFlowable;
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory;
import com.digitalasset.ledger.api.v1.ActiveContractsServiceGrpc;
import com.digitalasset.ledger.api.v1.ActiveContractsServiceOuterClass;
import io.grpc.Channel;
import io.reactivex.Flowable;

public class ActiveContractClientImpl implements ActiveContractsClient {

    private final String ledgerId;
    private final ActiveContractsServiceGrpc.ActiveContractsServiceStub serviceStub;
    private ExecutionSequencerFactory sequencerFactory;

    public ActiveContractClientImpl(String ledgerId, Channel channel, ExecutionSequencerFactory sequencerFactory) {
        this.ledgerId = ledgerId;
        this.sequencerFactory = sequencerFactory;
        serviceStub = ActiveContractsServiceGrpc.newStub(channel);
    }

    @Override
    public Flowable<GetActiveContractsResponse> getActiveContracts(TransactionFilter filter, boolean verbose) {
        ActiveContractsServiceOuterClass.GetActiveContractsRequest request = new GetActiveContractsRequest(ledgerId, filter, verbose).toProto();
        return ClientPublisherFlowable
                .create(request, serviceStub::getActiveContracts, sequencerFactory)
                .map(GetActiveContractsResponse::fromProto);
    }
}
