// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.ReassignmentCommandOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class AssignCommand {

  private final @NonNull String unassignId;

  private final @NonNull String source;

  private final @NonNull String target;

  public AssignCommand(@NonNull String unassignId, @NonNull String source, @NonNull String target) {
    this.unassignId = unassignId;
    this.source = source;
    this.target = target;
  }

  @NonNull
  public String getUnassignId() {
    return unassignId;
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
    return Objects.equals(unassignId, that.unassignId)
        && Objects.equals(source, that.source)
        && Objects.equals(target, that.target);
  }

  @Override
  public int hashCode() {
    return Objects.hash(unassignId, source, target);
  }

  @Override
  public String toString() {
    return "AssignCommand{"
        + "unassignId='"
        + unassignId
        + '\''
        + ", source="
        + source
        + ", target="
        + target
        + '}';
  }

  public ReassignmentCommandOuterClass.AssignCommand toProto() {
    return ReassignmentCommandOuterClass.AssignCommand.newBuilder()
        .setUnassignId(this.unassignId)
        .setSource(this.source)
        .setTarget(this.target)
        .build();
  }

  public static AssignCommand fromProto(ReassignmentCommandOuterClass.AssignCommand assignCommand) {
    return new AssignCommand(
        assignCommand.getUnassignId(), assignCommand.getSource(), assignCommand.getTarget());
  }
}
