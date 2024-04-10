// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TraceContextOuterClass;
import com.daml.ledger.api.v2.TransactionOuterClass;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class TransactionTree {

  @NonNull private final String updateId;

  @NonNull private final String commandId;

  @NonNull private final String workflowId;

  @NonNull private final Instant effectiveAt;

  @NonNull private final String offset;

  @NonNull private final Map<String, TreeEvent> eventsById;

  @NonNull private final List<String> rootEventIds;

  @NonNull private final String domainId;

  private final TraceContextOuterClass.@NonNull TraceContext traceContext;

  @NonNull private final Instant recordTime;

  public TransactionTree(
      @NonNull String updateId,
      @NonNull String commandId,
      @NonNull String workflowId,
      @NonNull Instant effectiveAt,
      @NonNull String offset,
      @NonNull Map<@NonNull String, @NonNull TreeEvent> eventsById,
      List<String> rootEventIds,
      @NonNull String domainId,
      TraceContextOuterClass.@NonNull TraceContext traceContext,
      @NonNull Instant recordTime) {
    this.updateId = updateId;
    this.commandId = commandId;
    this.workflowId = workflowId;
    this.effectiveAt = effectiveAt;
    this.offset = offset;
    this.eventsById = eventsById;
    this.rootEventIds = rootEventIds;
    this.domainId = domainId;
    this.traceContext = traceContext;
    this.recordTime = recordTime;
  }

  public static TransactionTree fromProto(TransactionOuterClass.TransactionTree tree) {
    Instant effectiveAt =
        Instant.ofEpochSecond(tree.getEffectiveAt().getSeconds(), tree.getEffectiveAt().getNanos());
    Map<String, TreeEvent> eventsById =
        tree.getEventsByIdMap().values().stream()
            .collect(
                Collectors.toMap(
                    e -> {
                      if (e.hasCreated()) return e.getCreated().getEventId();
                      else if (e.hasExercised()) return e.getExercised().getEventId();
                      else
                        throw new IllegalArgumentException(
                            "Event is neither created not exercied: " + e);
                    },
                    TreeEvent::fromProtoTreeEvent));
    List<String> rootEventIds = tree.getRootEventIdsList();
    return new TransactionTree(
        tree.getUpdateId(),
        tree.getCommandId(),
        tree.getWorkflowId(),
        effectiveAt,
        tree.getOffset(),
        eventsById,
        rootEventIds,
        tree.getDomainId(),
        tree.getTraceContext(),
        Utils.instantFromProto(tree.getRecordTime()));
  }

  public TransactionOuterClass.TransactionTree toProto() {
    return TransactionOuterClass.TransactionTree.newBuilder()
        .setUpdateId(updateId)
        .setCommandId(commandId)
        .setWorkflowId(workflowId)
        .setEffectiveAt(
            com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(effectiveAt.getEpochSecond())
                .setNanos(effectiveAt.getNano())
                .build())
        .setOffset(offset)
        .putAllEventsById(
            eventsById.values().stream()
                .collect(Collectors.toMap(TreeEvent::getEventId, TreeEvent::toProtoTreeEvent)))
        .addAllRootEventIds(rootEventIds)
        .setDomainId(domainId)
        .setTraceContext(traceContext)
        .setRecordTime(Utils.instantToProto(recordTime))
        .build();
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
  public String getOffset() {
    return offset;
  }

  @NonNull
  public Map<String, TreeEvent> getEventsById() {
    return eventsById;
  }

  @NonNull
  public List<String> getRootEventIds() {
    return rootEventIds;
  }

  @NonNull
  public String getDomainId() {
    return domainId;
  }

  public TraceContextOuterClass.@NonNull TraceContext getTraceContext() {
    return traceContext;
  }

  @NonNull
  public Instant getRecordTime() {
    return recordTime;
  }

  @Override
  public String toString() {
    return "TransactionTree{"
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
        + ", offset='"
        + offset
        + '\''
        + ", eventsById="
        + eventsById
        + ", rootEventIds="
        + rootEventIds
        + ", domainId='"
        + domainId
        + '\''
        + ", traceContext="
        + traceContext
        + ", recordTime="
        + recordTime
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TransactionTree that = (TransactionTree) o;
    return Objects.equals(updateId, that.updateId)
        && Objects.equals(commandId, that.commandId)
        && Objects.equals(workflowId, that.workflowId)
        && Objects.equals(effectiveAt, that.effectiveAt)
        && Objects.equals(eventsById, that.eventsById)
        && Objects.equals(rootEventIds, that.rootEventIds)
        && Objects.equals(offset, that.offset)
        && Objects.equals(domainId, that.domainId)
        && Objects.equals(traceContext, that.traceContext)
        && Objects.equals(recordTime, that.recordTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        updateId,
        commandId,
        workflowId,
        effectiveAt,
        offset,
        eventsById,
        rootEventIds,
        domainId,
        traceContext,
        recordTime);
  }
}
