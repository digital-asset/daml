// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandSubmissionServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class SubmitRequest {

  public static CommandSubmissionServiceOuterClass.SubmitRequest toProto(
      @NonNull CommandsSubmission submission) {
    return CommandSubmissionServiceOuterClass.SubmitRequest.newBuilder()
        .setCommands(submission.toProto())
        .build();
  }

  public static CommandsSubmission fromProto(
      CommandSubmissionServiceOuterClass.@NonNull SubmitRequest request) {
    return CommandsSubmission.fromProto(request.getCommands());
  }
}
