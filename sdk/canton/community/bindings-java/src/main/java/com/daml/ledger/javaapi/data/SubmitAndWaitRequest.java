// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class SubmitAndWaitRequest {

  public static CommandServiceOuterClass.SubmitAndWaitRequest toProto(
      @NonNull CommandsSubmission submission) {
    return CommandServiceOuterClass.SubmitAndWaitRequest.newBuilder()
        .setCommands(submission.toProto())
        .build();
  }

  public static CommandsSubmission fromProto(
      CommandServiceOuterClass.@NonNull SubmitAndWaitRequest request) {
    return CommandsSubmission.fromProto(request.getCommands());
  }
}
