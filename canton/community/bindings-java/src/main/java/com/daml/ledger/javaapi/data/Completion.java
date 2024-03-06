// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

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

  private final @NonNull String applicationId;

  private final @NonNull List<@NonNull String> actAs;

  private final @NonNull String submissionId;

  private final @NonNull Optional<String> deduplicationOffset;

  private final @NonNull Optional<Duration> deduplicationDuration;

  // model trace-context with its own class
  private final TraceContextOuterClass.@NonNull TraceContext traceContext;

  private Completion(
      @NonNull String commandId,
      @NonNull Status status,
      @NonNull String updateId,
      @NonNull String applicationId,
      @NonNull List<@NonNull String> actAs,
      @NonNull String submissionId,
      @NonNull Optional<String> deduplicationOffset,
      @NonNull Optional<Duration> deduplicationDuration,
      TraceContextOuterClass.@NonNull TraceContext traceContext) {
    this.commandId = commandId;
    this.status = status;
    this.updateId = updateId;
    this.applicationId = applicationId;
    this.actAs = List.copyOf(actAs);
    this.submissionId = submissionId;
    this.deduplicationOffset = deduplicationOffset;
    this.deduplicationDuration = deduplicationDuration;
    this.traceContext = traceContext;
  }

  public Completion(
      @NonNull String commandId,
      @NonNull Status status,
      @NonNull String updateId,
      @NonNull String applicationId,
      @NonNull List<@NonNull String> actAs,
      @NonNull String submissionId,
      @NonNull String deduplicationOffset,
      TraceContextOuterClass.TraceContext traceContext) {
    this(
        commandId,
        status,
        updateId,
        applicationId,
        actAs,
        submissionId,
        Optional.of(deduplicationOffset),
        Optional.empty(),
        traceContext);
  }

  public Completion(
      @NonNull String commandId,
      @NonNull Status status,
      @NonNull String updateId,
      @NonNull String applicationId,
      @NonNull List<@NonNull String> actAs,
      @NonNull String submissionId,
      @NonNull Duration deduplicationDuration,
      TraceContextOuterClass.TraceContext traceContext) {
    this(
        commandId,
        status,
        updateId,
        applicationId,
        actAs,
        submissionId,
        Optional.empty(),
        Optional.of(deduplicationDuration),
        traceContext);
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
  public String getApplicationId() {
    return applicationId;
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
  public Optional<String> getDeduplicationOffset() {
    return deduplicationOffset;
  }

  @NonNull
  public Optional<Duration> getDeduplicationDuration() {
    return deduplicationDuration;
  }

  public TraceContextOuterClass.@NonNull TraceContext getTraceContext() {
    return traceContext;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Completion that = (Completion) o;
    return Objects.equals(commandId, that.commandId)
        && Objects.equals(status, that.status)
        && Objects.equals(updateId, that.updateId)
        && Objects.equals(applicationId, that.applicationId)
        && Objects.equals(actAs, that.actAs)
        && Objects.equals(submissionId, that.submissionId)
        && Objects.equals(deduplicationOffset, that.deduplicationOffset)
        && Objects.equals(deduplicationDuration, that.deduplicationDuration)
        && Objects.equals(traceContext, that.traceContext);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        commandId,
        status,
        updateId,
        applicationId,
        actAs,
        submissionId,
        deduplicationOffset,
        deduplicationDuration,
        traceContext);
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
        + ", applicationId="
        + applicationId
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
        + '}';
  }

  public CompletionOuterClass.Completion toProto() {
    CompletionOuterClass.Completion.Builder builder =
        CompletionOuterClass.Completion.newBuilder()
            .setCommandId(commandId)
            .setStatus(status)
            .setUpdateId(updateId)
            .setApplicationId(applicationId)
            .addAllActAs(actAs)
            .setSubmissionId(submissionId)
            .setTraceContext(traceContext);
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
        completion.getApplicationId(),
        completion.getActAsList(),
        completion.getSubmissionId(),
        completion.hasDeduplicationOffset()
            ? Optional.of(completion.getDeduplicationOffset())
            : Optional.empty(),
        completion.hasDeduplicationDuration()
            ? Optional.of(Utils.durationFromProto(completion.getDeduplicationDuration()))
            : Optional.empty(),
        completion.getTraceContext());
  }
}
