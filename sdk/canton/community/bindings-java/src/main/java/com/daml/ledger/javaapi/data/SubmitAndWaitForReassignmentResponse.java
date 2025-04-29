// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class SubmitAndWaitForReassignmentResponse {

  @NonNull private final Reassignment reassignment;

  private SubmitAndWaitForReassignmentResponse(@NonNull Reassignment reassignment) {
    this.reassignment = reassignment;
  }

  @NonNull
  public Reassignment getReassignment() {
    return reassignment;
  }

  @NonNull
  public Long getCompletionOffset() {
    return reassignment.getOffset();
  }

  public static SubmitAndWaitForReassignmentResponse fromProto(
      CommandServiceOuterClass.SubmitAndWaitForReassignmentResponse response) {
    return new SubmitAndWaitForReassignmentResponse(
        Reassignment.fromProto(response.getReassignment()));
  }

  public CommandServiceOuterClass.SubmitAndWaitForReassignmentResponse toProto() {
    return CommandServiceOuterClass.SubmitAndWaitForReassignmentResponse.newBuilder()
        .setReassignment(reassignment.toProto())
        .build();
  }

  @Override
  public String toString() {

    return "SubmitAndWaitForReassignmentResponse{" + "reassignment=" + reassignment + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SubmitAndWaitForReassignmentResponse that = (SubmitAndWaitForReassignmentResponse) o;
    return Objects.equals(reassignment, that.reassignment);
  }

  @Override
  public int hashCode() {
    return Objects.hash(reassignment);
  }
}
