// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandCompletionServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

public final class CompletionStreamResponse {

  @NonNull private final Optional<Completion> completion;

  @NonNull private final Optional<OffsetCheckpoint> offsetCheckpoint;

  public CompletionStreamResponse(
      @NonNull Optional<Completion> completion,
      @NonNull Optional<OffsetCheckpoint> offsetCheckpoint) {
    this.completion = completion;
    this.offsetCheckpoint = offsetCheckpoint;
  }

  public CompletionStreamResponse(@NonNull Completion completion) {
    this(Optional.of(completion), Optional.empty());
  }

  public CompletionStreamResponse(@NonNull OffsetCheckpoint offsetCheckpoint) {
    this(Optional.empty(), Optional.of(offsetCheckpoint));
  }

  @NonNull
  public Optional<Completion> getCompletion() {
    return completion;
  }

  @NonNull
  public Optional<OffsetCheckpoint> getOffsetCheckpoint() {
    return offsetCheckpoint;
  }

  public static CompletionStreamResponse fromProto(
      CommandCompletionServiceOuterClass.CompletionStreamResponse response) {
    return new CompletionStreamResponse(
        response.hasCompletion()
            ? Optional.of(Completion.fromProto(response.getCompletion()))
            : Optional.empty(),
        response.hasOffsetCheckpoint()
            ? Optional.of(OffsetCheckpoint.fromProto(response.getOffsetCheckpoint()))
            : Optional.empty());
  }

  public CommandCompletionServiceOuterClass.CompletionStreamResponse toProto() {
    var builder = CommandCompletionServiceOuterClass.CompletionStreamResponse.newBuilder();
    completion.ifPresent(c -> builder.setCompletion(c.toProto()));
    offsetCheckpoint.ifPresent(c -> builder.setOffsetCheckpoint(c.toProto()));
    return builder.build();
  }

  @Override
  public String toString() {
    return "CompletionStreamResponse{"
        + "completion="
        + completion
        + ", offsetCheckpoint="
        + offsetCheckpoint
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CompletionStreamResponse that = (CompletionStreamResponse) o;
    return Objects.equals(completion, that.completion)
        && Objects.equals(offsetCheckpoint, that.offsetCheckpoint);
  }

  @Override
  public int hashCode() {

    return Objects.hash(completion, offsetCheckpoint);
  }
}
