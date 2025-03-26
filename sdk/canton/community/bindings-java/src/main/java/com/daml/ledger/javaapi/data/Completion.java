// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CompletionOuterClass;
import com.daml.ledger.api.v2.TraceContextOuterClass;
import com.google.rpc.Status;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class Completion {

  private final @NonNull String commandId;

  private final @NonNull Status status;

  private final @NonNull String updateId;

  private final @NonNull String userId;

  private final @NonNull List<@NonNull String> actAs;

  private final @NonNull String submissionId;

  private final @NonNull Optional<Long> deduplicationOffset;

  private final @NonNull Optional<Duration> deduplicationDuration;

  // model trace-context with its own class
  private final TraceContextOuterClass.@NonNull TraceContext traceContext;

  private final @NonNull Long offset;

  private final @NonNull SynchronizerTime synchronizerTime;

  private Completion(
      @NonNull String commandId,
      @NonNull Status status,
      @NonNull String updateId,
      @NonNull String userId,
      @NonNull List<@NonNull String> actAs,
      @NonNull String submissionId,
      @NonNull Optional<Long> deduplicationOffset,
      @NonNull Optional<Duration> deduplicationDuration,
      TraceContextOuterClass.@NonNull TraceContext traceContext,
      @NonNull Long offset,
      @NonNull SynchronizerTime synchronizerTime) {
    this.commandId = commandId;
    this.status = status;
    this.updateId = updateId;
    this.userId = userId;
    this.actAs = List.copyOf(actAs);
    this.submissionId = submissionId;
    this.deduplicationOffset = deduplicationOffset;
    this.deduplicationDuration = deduplicationDuration;
    this.traceContext = traceContext;
    this.offset = offset;
    this.synchronizerTime = synchronizerTime;
  }

  public Completion(
      @NonNull String commandId,
      @NonNull Status status,
      @NonNull String updateId,
      @NonNull String userId,
      @NonNull List<@NonNull String> actAs,
      @NonNull String submissionId,
      @NonNull Long deduplicationOffset,
      TraceContextOuterClass.TraceContext traceContext,
      @NonNull Long offset,
      @NonNull SynchronizerTime synchronizerTime) {
    this(
        commandId,
        status,
        updateId,
        userId,
        actAs,
        submissionId,
        Optional.of(deduplicationOffset),
        Optional.empty(),
        traceContext,
        offset,
        synchronizerTime);
  }

  public Completion(
      @NonNull String commandId,
      @NonNull Status status,
      @NonNull String updateId,
      @NonNull String userId,
      @NonNull List<@NonNull String> actAs,
      @NonNull String submissionId,
      @NonNull Duration deduplicationDuration,
      TraceContextOuterClass.TraceContext traceContext,
      @NonNull Long offset,
      @NonNull SynchronizerTime synchronizerTime) {
    this(
        commandId,
        status,
        updateId,
        userId,
        actAs,
        submissionId,
        Optional.empty(),
        Optional.of(deduplicationDuration),
        traceContext,
        offset,
        synchronizerTime);
  }

  @NonNull
  public String getCommandId() {
    return commandId;
  }

  @NonNull
  public Status getStatus() {
    return status;
  }

  @NonNull
  public String getUpdateId() {
    return updateId;
  }

  @NonNull
  public String getUserId() {
    return userId;
  }

  @NonNull
  public List<@NonNull String> getActAs() {
    return actAs;
  }

  @NonNull
  public String getSubmissionId() {
    return submissionId;
  }

  @NonNull
  public Optional<Long> getDeduplicationOffset() {
    return deduplicationOffset;
  }

  @NonNull
  public Optional<Duration> getDeduplicationDuration() {
    return deduplicationDuration;
  }

  public TraceContextOuterClass.@NonNull TraceContext getTraceContext() {
    return traceContext;
  }

  @NonNull
  public Long getOffset() {
    return offset;
  }

  @NonNull
  public SynchronizerTime getSynchronizerTime() {
    return synchronizerTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Completion that = (Completion) o;
    return Objects.equals(commandId, that.commandId)
        && Objects.equals(status, that.status)
        && Objects.equals(updateId, that.updateId)
        && Objects.equals(userId, that.userId)
        && Objects.equals(actAs, that.actAs)
        && Objects.equals(submissionId, that.submissionId)
        && Objects.equals(deduplicationOffset, that.deduplicationOffset)
        && Objects.equals(deduplicationDuration, that.deduplicationDuration)
        && Objects.equals(traceContext, that.traceContext)
        && Objects.equals(offset, that.offset)
        && Objects.equals(synchronizerTime, that.synchronizerTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        commandId,
        status,
        updateId,
        userId,
        actAs,
        submissionId,
        deduplicationOffset,
        deduplicationDuration,
        traceContext,
        offset,
        synchronizerTime);
  }

  @Override
  public String toString() {
    return "Completion{"
        + "commandId='"
        + commandId
        + '\''
        + ", status="
        + status
        + ", updateId='"
        + updateId
        + '\''
        + ", userId="
        + userId
        + ", actAs="
        + actAs
        + ", submissionId="
        + submissionId
        + ", deduplicationOffset="
        + deduplicationOffset
        + ", deduplicationDuration="
        + deduplicationDuration
        + ", traceContext="
        + traceContext
        + ", offset='"
        + offset
        + ", synchronizerTime="
        + synchronizerTime
        + '\''
        + '}';
  }

  public CompletionOuterClass.Completion toProto() {
    CompletionOuterClass.Completion.Builder builder =
        CompletionOuterClass.Completion.newBuilder()
            .setCommandId(commandId)
            .setStatus(status)
            .setUpdateId(updateId)
            .setUserId(userId)
            .addAllActAs(actAs)
            .setSubmissionId(submissionId)
            .setTraceContext(traceContext)
            .setOffset(offset)
            .setSynchronizerTime(synchronizerTime.toProto());
    deduplicationOffset.ifPresent(builder::setDeduplicationOffset);
    deduplicationDuration.ifPresent(
        duration -> builder.setDeduplicationDuration(Utils.durationToProto(duration)));
    return builder.build();
  }

  public static Completion fromProto(CompletionOuterClass.Completion completion) {
    return new Completion(
        completion.getCommandId(),
        completion.getStatus(),
        completion.getUpdateId(),
        completion.getUserId(),
        completion.getActAsList(),
        completion.getSubmissionId(),
        completion.hasDeduplicationOffset()
            ? Optional.of(completion.getDeduplicationOffset())
            : Optional.empty(),
        completion.hasDeduplicationDuration()
            ? Optional.of(Utils.durationFromProto(completion.getDeduplicationDuration()))
            : Optional.empty(),
        completion.getTraceContext(),
        completion.getOffset(),
        SynchronizerTime.fromProto(completion.getSynchronizerTime()));
  }
}
