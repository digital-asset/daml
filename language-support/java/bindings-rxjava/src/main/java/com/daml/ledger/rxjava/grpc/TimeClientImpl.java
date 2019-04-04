// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.ledger.rxjava.TimeClient;
import com.daml.ledger.rxjava.grpc.helpers.TimestampComparator;
import com.daml.ledger.rxjava.util.ClientPublisherFlowable;
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory;
import com.digitalasset.ledger.api.v1.testing.TimeServiceGrpc;
import com.digitalasset.ledger.api.v1.testing.TimeServiceOuterClass;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import io.grpc.Channel;
import io.reactivex.Flowable;
import io.reactivex.Single;

import java.time.Instant;

public final class TimeClientImpl implements TimeClient {

    private final String ledgerId;
    private final TimeServiceGrpc.TimeServiceFutureStub serviceFutureStub;
    private final TimeServiceGrpc.TimeServiceStub serviceStub;
    private final ExecutionSequencerFactory sequencerFactory;

    public TimeClientImpl(String ledgerId, Channel channel, ExecutionSequencerFactory sequencerFactory) {
        this.ledgerId = ledgerId;
        this.sequencerFactory = sequencerFactory;
        this.serviceFutureStub = TimeServiceGrpc.newFutureStub(channel);
        this.serviceStub = TimeServiceGrpc.newStub(channel);
    }

    @Override
    public Single<Empty> setTime(Instant currentTime, Instant newTime) {
        if (currentTime.compareTo(newTime) >= 0) {
            throw new SetTimeException(currentTime, newTime);
        }
        TimeServiceOuterClass.SetTimeRequest request = TimeServiceOuterClass.SetTimeRequest.newBuilder()
                .setLedgerId(this.ledgerId)
                .setCurrentTime(Timestamp.newBuilder().setSeconds(currentTime.getEpochSecond()).setNanos(currentTime.getNano()).build())
                .setNewTime(Timestamp.newBuilder().setSeconds(newTime.getEpochSecond()).setNanos(newTime.getNano())).build();
        return Single.fromFuture(this.serviceFutureStub.setTime(request));
    }

    @Override
    public Flowable<Instant> getTime() {
        TimeServiceOuterClass.GetTimeRequest request = TimeServiceOuterClass.GetTimeRequest.newBuilder()
                .setLedgerId(this.ledgerId)
                .build();
        return ClientPublisherFlowable
                .create(request, this.serviceStub::getTime, sequencerFactory)
                .map(r -> Instant.ofEpochSecond(r.getCurrentTime().getSeconds(), r.getCurrentTime().getNanos()));
    }

    private class SetTimeException extends RuntimeException {
        public SetTimeException(Instant currentTime, Instant newTime) {
            super(String.format("Cannot set a new time smaller or equal to the current one. That new time tried is %s but the current one is %s", newTime, currentTime));
        }
    }
}
