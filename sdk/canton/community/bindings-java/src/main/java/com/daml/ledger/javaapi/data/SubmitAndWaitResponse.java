// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class SubmitAndWaitResponse {

  @NonNull private final String updateId;

  @NonNull private final Long completionOffset;

  private SubmitAndWaitResponse(@NonNull String updateId, @NonNull Long completionOffset) {
    this.updateId = updateId;
    this.completionOffset = completionOffset;
  }

  @NonNull
  public String getUpdateId() {
    return updateId;
  }

  @NonNull
  public Long getCompletionOffset() {
    return completionOffset;
  }

  public static SubmitAndWaitResponse fromProto(
      CommandServiceOuterClass.SubmitAndWaitResponse response) {
    return new SubmitAndWaitResponse(response.getUpdateId(), response.getCompletionOffset());
  }

  public CommandServiceOuterClass.SubmitAndWaitResponse toProto() {
    return CommandServiceOuterClass.SubmitAndWaitResponse.newBuilder()
        .setUpdateId(updateId)
        .setCompletionOffset(completionOffset)
        .build();
  }

  @Override
  public String toString() {

    return "SubmitAndWaitResponse{"
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
    SubmitAndWaitResponse that = (SubmitAndWaitResponse) o;
    return Objects.equals(updateId, that.updateId)
        && Objects.equals(completionOffset, that.completionOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(updateId, completionOffset);
  }
}
