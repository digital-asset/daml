// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandSubmissionServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class SubmitReassignmentRequest {

  public static CommandSubmissionServiceOuterClass.SubmitReassignmentRequest toProto(
      @NonNull ReassignmentCommands command) {
    return CommandSubmissionServiceOuterClass.SubmitReassignmentRequest.newBuilder()
        .setReassignmentCommands(command.toProto())
        .build();
  }

  public static ReassignmentCommands fromProto(
      CommandSubmissionServiceOuterClass.@NonNull SubmitReassignmentRequest request) {
    return ReassignmentCommands.fromProto(request.getReassignmentCommands());
  }
}
