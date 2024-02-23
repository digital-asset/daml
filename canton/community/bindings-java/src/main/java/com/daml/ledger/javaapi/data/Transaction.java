// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.TraceContextOuterClass;
import com.daml.ledger.api.v2.TransactionOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class Transaction {

  @NonNull private final String updateId;

  @NonNull private final String commandId;

  @NonNull private final String workflowId;

  @NonNull private final Instant effectiveAt;

  @NonNull private final List<@NonNull Event> events;

  @NonNull private final String offset;

  @NonNull private final String domainId;

  private final TraceContextOuterClass.@NonNull TraceContext traceContext;

  public Transaction(
      @NonNull String updateId,
      @NonNull String commandId,
      @NonNull String workflowId,
      @NonNull Instant effectiveAt,
      @NonNull List<@NonNull Event> events,
      @NonNull String offset,
      @NonNull String domainId,
      TraceContextOuterClass.@NonNull TraceContext traceContext) {
    this.updateId = updateId;
    this.commandId = commandId;
    this.workflowId = workflowId;
    this.effectiveAt = effectiveAt;
    this.events = events;
    this.offset = offset;
    this.domainId = domainId;
    this.traceContext = traceContext;
  }

  @NonNull
  public String getUpdateId() {
    return updateId;
  }

  @NonNull
  public String getCommandId() {
    return commandId;
  }

  @NonNull
  public String getWorkflowId() {
    return workflowId;
  }

  @NonNull
  public Instant getEffectiveAt() {
    return effectiveAt;
  }

  @NonNull
  public List<Event> getEvents() {
    return events;
  }

  @NonNull
  public String getOffset() {
    return offset;
  }

  @NonNull
  public String getDomainId() {
    return domainId;
  }

  public TraceContextOuterClass.@NonNull TraceContext getTraceContext() {
    return traceContext;
  }

  public static Transaction fromProto(TransactionOuterClass.Transaction transaction) {
    Instant effectiveAt =
        Instant.ofEpochSecond(
            transaction.getEffectiveAt().getSeconds(), transaction.getEffectiveAt().getNanos());
    List<Event> events =
        transaction.getEventsList().stream()
            .map(Event::fromProtoEvent)
            .collect(Collectors.toList());
    return new Transaction(
        transaction.getUpdateId(),
        transaction.getCommandId(),
        transaction.getWorkflowId(),
        effectiveAt,
        events,
        transaction.getOffset(),
        transaction.getDomainId(),
        transaction.getTraceContext());
  }

  public TransactionOuterClass.Transaction toProto() {
    return TransactionOuterClass.Transaction.newBuilder()
        .setUpdateId(updateId)
        .setCommandId(commandId)
        .setWorkflowId(workflowId)
        .setEffectiveAt(
            com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(effectiveAt.getEpochSecond())
                .setNanos(effectiveAt.getNano())
                .build())
        .addAllEvents(events.stream().map(Event::toProtoEvent).collect(Collectors.toList()))
        .setOffset(offset)
        .setDomainId(domainId)
        .setTraceContext(traceContext)
        .build();
  }

  @Override
  public String toString() {
    return "Transaction{"
        + "updateId='"
        + updateId
        + '\''
        + ", commandId='"
        + commandId
        + '\''
        + ", workflowId='"
        + workflowId
        + '\''
        + ", effectiveAt="
        + effectiveAt
        + ", events="
        + events
        + ", offset='"
        + offset
        + '\''
        + ", domainId='"
        + domainId
        + '\''
        + ", traceContext="
        + traceContext
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Transaction that = (Transaction) o;
    return Objects.equals(updateId, that.updateId)
        && Objects.equals(commandId, that.commandId)
        && Objects.equals(workflowId, that.workflowId)
        && Objects.equals(effectiveAt, that.effectiveAt)
        && Objects.equals(events, that.events)
        && Objects.equals(offset, that.offset)
        && Objects.equals(domainId, that.domainId)
        && Objects.equals(traceContext, that.traceContext);
  }

  @Override
  public int hashCode() {

    return Objects.hash(
        updateId, commandId, workflowId, effectiveAt, events, offset, domainId, traceContext);
  }
}
