// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.grpc.adapter.ExecutionSequencerFactory;
import com.daml.ledger.api.v1.testing.TimeServiceGrpc;
import com.daml.ledger.api.v1.testing.TimeServiceOuterClass;
import com.daml.ledger.rxjava.TimeClient;
import com.daml.ledger.rxjava.grpc.helpers.StubHelper;
import com.daml.ledger.rxjava.util.ClientPublisherFlowable;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import io.grpc.Channel;
import io.reactivex.Flowable;
import io.reactivex.Single;
import java.time.Instant;
import java.util.Optional;

public final class TimeClientImpl implements TimeClient {

  private final String ledgerId;
  private final TimeServiceGrpc.TimeServiceFutureStub serviceFutureStub;
  private final TimeServiceGrpc.TimeServiceStub serviceStub;
  private final ExecutionSequencerFactory sequencerFactory;

  public TimeClientImpl(
      String ledgerId,
      Channel channel,
      ExecutionSequencerFactory sequencerFactory,
      Optional<String> accessToken) {
    this.ledgerId = ledgerId;
    this.sequencerFactory = sequencerFactory;
    this.serviceFutureStub =
        StubHelper.authenticating(TimeServiceGrpc.newFutureStub(channel), accessToken);
    this.serviceStub = StubHelper.authenticating(TimeServiceGrpc.newStub(channel), accessToken);
  }

  private Single<Empty> setTime(
      Instant currentTime, Instant newTime, Optional<String> accessToken) {
    if (currentTime.compareTo(newTime) >= 0) {
      throw new SetTimeException(currentTime, newTime);
    }
    TimeServiceOuterClass.SetTimeRequest request =
        TimeServiceOuterClass.SetTimeRequest.newBuilder()
            .setLedgerId(this.ledgerId)
            .setCurrentTime(
                Timestamp.newBuilder()
                    .setSeconds(currentTime.getEpochSecond())
                    .setNanos(currentTime.getNano())
                    .build())
            .setNewTime(
                Timestamp.newBuilder()
                    .setSeconds(newTime.getEpochSecond())
                    .setNanos(newTime.getNano()))
            .build();
    return Single.fromFuture(
        StubHelper.authenticating(this.serviceFutureStub, accessToken).setTime(request));
  }

  @Override
  public Single<Empty> setTime(Instant currentTime, Instant newTime) {
    return setTime(currentTime, newTime, Optional.empty());
  }

  @Override
  public Single<Empty> setTime(Instant currentTime, Instant newTime, String accessToken) {
    return setTime(currentTime, newTime, Optional.of(accessToken));
  }

  private Flowable<Instant> getTime(Optional<String> accessToken) {
    TimeServiceOuterClass.GetTimeRequest request =
        TimeServiceOuterClass.GetTimeRequest.newBuilder().setLedgerId(this.ledgerId).build();
    return ClientPublisherFlowable.create(
            request,
            StubHelper.authenticating(this.serviceStub, accessToken)::getTime,
            sequencerFactory)
        .map(
            r ->
                Instant.ofEpochSecond(
                    r.getCurrentTime().getSeconds(), r.getCurrentTime().getNanos()));
  }

  @Override
  public Flowable<Instant> getTime() {
    return getTime(Optional.empty());
  }

  @Override
  public Flowable<Instant> getTime(String accessToken) {
    return getTime(Optional.of(accessToken));
  }

  private class SetTimeException extends RuntimeException {
    public SetTimeException(Instant currentTime, Instant newTime) {
      super(
          String.format(
              "Cannot set a new time smaller or equal to the current one. That new time tried is"
                  + " %s but the current one is %s",
              newTime, currentTime));
    }
  }
}
