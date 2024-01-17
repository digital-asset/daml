// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.daml.ledger.api.v1.CommandSubmissionServiceGrpc;
import com.daml.ledger.api.v1.CommandSubmissionServiceOuterClass;
import com.daml.ledger.javaapi.data.CommandsSubmission;
import com.daml.ledger.javaapi.data.SubmitRequest;
import com.daml.ledger.rxjava.CommandSubmissionClient;
import com.daml.ledger.rxjava.grpc.helpers.StubHelper;
import com.google.protobuf.Empty;
import io.grpc.Channel;
import io.reactivex.Single;
import java.time.Duration;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

public class CommandSubmissionClientImpl implements CommandSubmissionClient {

  private final String ledgerId;
  private final CommandSubmissionServiceGrpc.CommandSubmissionServiceFutureStub serviceStub;
  private final Optional<Duration> timeout;

  public CommandSubmissionClientImpl(
      @NonNull String ledgerId,
      @NonNull Channel channel,
      Optional<String> accessToken,
      Optional<Duration> timeout) {
    this.ledgerId = ledgerId;
    this.timeout = timeout;
    this.serviceStub =
        StubHelper.authenticating(CommandSubmissionServiceGrpc.newFutureStub(channel), accessToken);
  }

  @Override
  public Single<Empty> submit(CommandsSubmission submission) {
    CommandSubmissionServiceOuterClass.SubmitRequest request =
        SubmitRequest.toProto(ledgerId, submission);
    CommandSubmissionServiceGrpc.CommandSubmissionServiceFutureStub stubWithTimeout =
        this.timeout
            .map(t -> this.serviceStub.withDeadlineAfter(t.toMillis(), MILLISECONDS))
            .orElse(this.serviceStub);
    return Single.fromFuture(
        StubHelper.authenticating(stubWithTimeout, submission.getAccessToken()).submit(request));
  }
}
