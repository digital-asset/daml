// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.ReassignmentCommandOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class AssignCommand extends ReassignmentCommand {

  private final @NonNull String reassignmentId;

  private final @NonNull String source;

  private final @NonNull String target;

  public AssignCommand(
      @NonNull String reassignmentId, @NonNull String source, @NonNull String target) {
    this.reassignmentId = reassignmentId;
    this.source = source;
    this.target = target;
  }

  @NonNull
  public String getReassignmentId() {
    return reassignmentId;
  }

  @NonNull
  public String getSource() {
    return source;
  }

  @NonNull
  public String getTarget() {
    return target;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AssignCommand that = (AssignCommand) o;
    return Objects.equals(reassignmentId, that.reassignmentId)
        && Objects.equals(source, that.source)
        && Objects.equals(target, that.target);
  }

  @Override
  public int hashCode() {
    return Objects.hash(reassignmentId, source, target);
  }

  @Override
  public String toString() {
    return "AssignCommand{"
        + "reassignmentId='"
        + reassignmentId
        + '\''
        + ", source="
        + source
        + ", target="
        + target
        + '}';
  }

  public ReassignmentCommandOuterClass.AssignCommand toProto() {
    return ReassignmentCommandOuterClass.AssignCommand.newBuilder()
        .setReassignmentId(this.reassignmentId)
        .setSource(this.source)
        .setTarget(this.target)
        .build();
  }

  public static AssignCommand fromProto(ReassignmentCommandOuterClass.AssignCommand assignCommand) {
    return new AssignCommand(
        assignCommand.getReassignmentId(), assignCommand.getSource(), assignCommand.getTarget());
  }
}
