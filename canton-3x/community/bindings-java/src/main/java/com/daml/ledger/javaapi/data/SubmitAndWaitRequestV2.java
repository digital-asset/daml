// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class SubmitAndWaitRequestV2 {

  public static CommandServiceOuterClass.SubmitAndWaitRequest toProto(
      @NonNull CommandsSubmissionV2 submission) {
    return CommandServiceOuterClass.SubmitAndWaitRequest.newBuilder()
        .setCommands(submission.toProto())
        .build();
  }

  public static CommandsSubmissionV2 fromProto(
      CommandServiceOuterClass.@NonNull SubmitAndWaitRequest request) {
    return CommandsSubmissionV2.fromProto(request.getCommands());
  }
}
