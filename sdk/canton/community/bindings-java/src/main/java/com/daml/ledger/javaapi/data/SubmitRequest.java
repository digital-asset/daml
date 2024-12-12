// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.CommandSubmissionServiceOuterClass;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class SubmitRequest {

  public static CommandSubmissionServiceOuterClass.SubmitRequest toProto(
      @NonNull String ledgerId, @NonNull CommandsSubmission submission) {
    return CommandSubmissionServiceOuterClass.SubmitRequest.newBuilder()
        .setCommands(SubmitCommandsRequest.toProto(ledgerId, submission))
        .build();
  }

  public static CommandSubmissionServiceOuterClass.SubmitRequest toProto(
      @NonNull String ledgerId,
      @NonNull String submissionId,
      @NonNull CommandsSubmission submission) {
    return CommandSubmissionServiceOuterClass.SubmitRequest.newBuilder()
        .setCommands(SubmitCommandsRequest.toProto(ledgerId, submissionId, submission))
        .build();
  }
}
