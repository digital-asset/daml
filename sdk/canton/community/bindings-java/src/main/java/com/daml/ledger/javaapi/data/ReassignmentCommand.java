// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.ReassignmentCommandOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

import static java.util.Optional.empty;

public abstract class ReassignmentCommand {
  public static ReassignmentCommand fromProtoCommand(
      ReassignmentCommandOuterClass.ReassignmentCommand command) {
    switch (command.getCommandCase()) {
      case ASSIGN_COMMAND:
        return AssignCommand.fromProto(command.getAssignCommand());
      case UNASSIGN_COMMAND:
        return UnassignCommand.fromProto(command.getUnassignCommand());
      case COMMAND_NOT_SET:
      default:
        throw new ProtoReassignmentCommandUnknown(command);
    }
  }

  public ReassignmentCommandOuterClass.ReassignmentCommand toProtoCommand() {
    ReassignmentCommandOuterClass.ReassignmentCommand.Builder builder =
        ReassignmentCommandOuterClass.ReassignmentCommand.newBuilder();
    if (this instanceof UnassignCommand) {
      builder.setUnassignCommand(((UnassignCommand) this).toProto());
    } else if (this instanceof AssignCommand) {
      builder.setAssignCommand(((AssignCommand) this).toProto());
    } else {
      throw new ReassignmentCommandUnknown(this);
    }
    return builder.build();
  }
}

class ReassignmentCommandUnknown extends RuntimeException {
  public ReassignmentCommandUnknown(ReassignmentCommand command) {
    super("Reassignment command unknown " + command.toString());
  }
}

class ProtoReassignmentCommandUnknown extends RuntimeException {
  public ProtoReassignmentCommandUnknown(
      ReassignmentCommandOuterClass.ReassignmentCommand command) {
    super("Reassignment command unknown " + command.toString());
  }
}
