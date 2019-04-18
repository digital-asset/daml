// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.util;

import com.digitalasset.grpc.adapter.ExecutionSequencerFactory;
import com.digitalasset.grpc.adapter.client.rs.ClientPublisher;
import io.grpc.stub.StreamObserver;
import io.reactivex.Flowable;

import java.util.function.BiConsumer;

public class ClientPublisherFlowable {
    public static <Req, Resp> Flowable<Resp> create(Req request,
                                                    BiConsumer<Req, StreamObserver<Resp>> clientStub,
                                                    ExecutionSequencerFactory executionSequencerFactory) {
        return Flowable.fromPublisher(new ClientPublisher<>(request, clientStub, executionSequencerFactory));
    }
}
