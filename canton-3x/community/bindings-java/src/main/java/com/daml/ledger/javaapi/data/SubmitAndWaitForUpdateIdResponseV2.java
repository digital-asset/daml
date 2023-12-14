// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class SubmitAndWaitForUpdateIdResponseV2 {

  @NonNull private final String updateId;

  @NonNull private final String completionOffset;

  private SubmitAndWaitForUpdateIdResponseV2(
      @NonNull String updateId, @NonNull String completionOffset) {
    this.updateId = updateId;
    this.completionOffset = completionOffset;
  }

  @NonNull
  public String getUpdateId() {
    return updateId;
  }

  @NonNull
  public String getCompletionOffset() {
    return completionOffset;
  }

  public static SubmitAndWaitForUpdateIdResponseV2 fromProto(
      CommandServiceOuterClass.SubmitAndWaitForUpdateIdResponse response) {
    return new SubmitAndWaitForUpdateIdResponseV2(
        response.getUpdateId(), response.getCompletionOffset());
  }

  public CommandServiceOuterClass.SubmitAndWaitForUpdateIdResponse toProto() {
    return CommandServiceOuterClass.SubmitAndWaitForUpdateIdResponse.newBuilder()
        .setUpdateId(updateId)
        .setCompletionOffset(completionOffset)
        .build();
  }

  @Override
  public String toString() {

    return "SubmitAndWaitForUpdateIdResponse{"
        + "updateId='"
        + updateId
        + '\''
        + ", completionOffset='"
        + completionOffset
        + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SubmitAndWaitForUpdateIdResponseV2 that = (SubmitAndWaitForUpdateIdResponseV2) o;
    return Objects.equals(updateId, that.updateId)
        && Objects.equals(completionOffset, that.completionOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(updateId, completionOffset);
  }
}
