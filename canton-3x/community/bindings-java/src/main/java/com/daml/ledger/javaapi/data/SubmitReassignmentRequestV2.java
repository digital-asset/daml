// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandSubmissionServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class SubmitReassignmentRequestV2 {

  public static CommandSubmissionServiceOuterClass.SubmitReassignmentRequest toProto(
      @NonNull ReassignmentCommandV2 command) {
    return CommandSubmissionServiceOuterClass.SubmitReassignmentRequest.newBuilder()
        .setReassignmentCommand(command.toProto())
        .build();
  }

  public static ReassignmentCommandV2 fromProto(
      CommandSubmissionServiceOuterClass.@NonNull SubmitReassignmentRequest request) {
    return ReassignmentCommandV2.fromProto(request.getReassignmentCommand());
  }
}
