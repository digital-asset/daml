// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandCompletionServiceOuterClass;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class CompletionStreamResponse {

  @NonNull private final Checkpoint checkpoint;

  @NonNull private final Completion completion;

  @NonNull private final String domainId;

  public CompletionStreamResponse(
      @NonNull Checkpoint checkpoint, @NonNull Completion completion, @NonNull String domainId) {
    this.checkpoint = checkpoint;
    this.completion = completion;
    this.domainId = domainId;
  }

  @NonNull
  public Checkpoint getCheckpoint() {
    return checkpoint;
  }

  @NonNull
  public Completion getCompletion() {
    return completion;
  }

  @NonNull
  public String getDomainId() {
    return domainId;
  }

  public static CompletionStreamResponse fromProto(
      CommandCompletionServiceOuterClass.CompletionStreamResponse response) {
    return new CompletionStreamResponse(
        Checkpoint.fromProto(response.getCheckpoint()),
        Completion.fromProto(response.getCompletion()),
        response.getDomainId());
  }

  public CommandCompletionServiceOuterClass.CompletionStreamResponse toProto() {
    return CommandCompletionServiceOuterClass.CompletionStreamResponse.newBuilder()
        .setCheckpoint(checkpoint.toProto())
        .setCompletion(completion.toProto())
        .setDomainId(domainId)
        .build();
  }

  @Override
  public String toString() {
    return "CompletionStreamResponse{"
        + "checkpoint="
        + checkpoint
        + ", completion="
        + completion
        + ", domainId='"
        + domainId
        + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CompletionStreamResponse that = (CompletionStreamResponse) o;
    return Objects.equals(checkpoint, that.checkpoint)
        && Objects.equals(completion, that.completion)
        && Objects.equals(domainId, that.domainId);
  }

  @Override
  public int hashCode() {

    return Objects.hash(checkpoint, completion, domainId);
  }
}
