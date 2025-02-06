// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.ReassignmentCommandOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class UnassignCommand {

  private final @NonNull String contractId;

  private final @NonNull String source;

  private final @NonNull String target;

  public UnassignCommand(
      @NonNull String contractId, @NonNull String source, @NonNull String target) {
    this.contractId = contractId;
    this.source = source;
    this.target = target;
  }

  @NonNull
  public String getContractId() {
    return contractId;
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
    UnassignCommand that = (UnassignCommand) o;
    return Objects.equals(contractId, that.contractId)
        && Objects.equals(source, that.source)
        && Objects.equals(target, that.target);
  }

  @Override
  public int hashCode() {
    return Objects.hash(contractId, source, target);
  }

  @Override
  public String toString() {
    return "UnassignCommand{"
        + "contractId='"
        + contractId
        + '\''
        + ", source="
        + source
        + ", target="
        + target
        + '}';
  }

  public ReassignmentCommandOuterClass.UnassignCommand toProto() {
    return ReassignmentCommandOuterClass.UnassignCommand.newBuilder()
        .setContractId(this.contractId)
        .setSource(this.source)
        .setTarget(this.target)
        .build();
  }

  public static UnassignCommand fromProto(
      ReassignmentCommandOuterClass.UnassignCommand unassignCommand) {
    return new UnassignCommand(
        unassignCommand.getContractId(), unassignCommand.getSource(), unassignCommand.getTarget());
  }
}
