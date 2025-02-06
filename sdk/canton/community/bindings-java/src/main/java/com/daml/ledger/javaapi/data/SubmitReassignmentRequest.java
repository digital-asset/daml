// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandSubmissionServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class SubmitReassignmentRequest {

  public static CommandSubmissionServiceOuterClass.SubmitReassignmentRequest toProto(
      @NonNull ReassignmentCommand command) {
    return CommandSubmissionServiceOuterClass.SubmitReassignmentRequest.newBuilder()
        .setReassignmentCommand(command.toProto())
        .build();
  }

  public static ReassignmentCommand fromProto(
      CommandSubmissionServiceOuterClass.@NonNull SubmitReassignmentRequest request) {
    return ReassignmentCommand.fromProto(request.getReassignmentCommand());
  }
}
