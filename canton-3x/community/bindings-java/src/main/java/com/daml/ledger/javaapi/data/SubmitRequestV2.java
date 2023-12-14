// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandSubmissionServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class SubmitRequestV2 {

  public static CommandSubmissionServiceOuterClass.SubmitRequest toProto(
      @NonNull CommandsSubmissionV2 submission) {
    return CommandSubmissionServiceOuterClass.SubmitRequest.newBuilder()
        .setCommands(submission.toProto())
        .build();
  }

  public static CommandsSubmissionV2 fromProto(
      CommandSubmissionServiceOuterClass.@NonNull SubmitRequest request) {
    return CommandsSubmissionV2.fromProto(request.getCommands());
  }
}
