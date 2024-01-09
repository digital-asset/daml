// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandCompletionServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class CompletionStreamResponseV2 {

  @NonNull private final Checkpoint checkpoint;

  @NonNull private final CompletionV2 completion;

  @NonNull private final String domainId;

  public CompletionStreamResponseV2(
      @NonNull Checkpoint checkpoint, @NonNull CompletionV2 completion, @NonNull String domainId) {
    this.checkpoint = checkpoint;
    this.completion = completion;
    this.domainId = domainId;
  }

  @NonNull
  public Checkpoint getCheckpoint() {
    return checkpoint;
  }

  @NonNull
  public CompletionV2 getCompletion() {
    return completion;
  }

  @NonNull
  public String getDomainId() {
    return domainId;
  }

  public static CompletionStreamResponseV2 fromProto(
      CommandCompletionServiceOuterClass.CompletionStreamResponse response) {
    return new CompletionStreamResponseV2(
        Checkpoint.fromProto(response.getCheckpoint()),
        CompletionV2.fromProto(response.getCompletion()),
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
    CompletionStreamResponseV2 that = (CompletionStreamResponseV2) o;
    return Objects.equals(checkpoint, that.checkpoint)
        && Objects.equals(completion, that.completion)
        && Objects.equals(domainId, that.domainId);
  }

  @Override
  public int hashCode() {

    return Objects.hash(checkpoint, completion, domainId);
  }
}
