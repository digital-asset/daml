// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TraceContextOuterClass;
import com.daml.ledger.api.v2.TopologyTransactionOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Instant;
// import java.util.List;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
// import java.util.stream.Collectors;

public final class TopologyTransaction {

  @NonNull private final String updateId;
  @NonNull private final Long offset;
  @NonNull private final String synchronizerId;
  @NonNull private final Instant recordTime;
  @NonNull private final List<@NonNull TopologyEvent> events;
  private final TraceContextOuterClass.@NonNull TraceContext traceContext;

  public TopologyTransaction(
      @NonNull String updateId,
      @NonNull Long offset,
      @NonNull String synchronizerId,
      @NonNull Instant recordTime,
      @NonNull List<@NonNull TopologyEvent> events,
      TraceContextOuterClass.@NonNull TraceContext traceContext) {
    this.updateId = updateId;
    this.offset = offset;
    this.synchronizerId = synchronizerId;
    this.recordTime = recordTime;
    this.events = events;
    this.traceContext = traceContext;
  }

  @NonNull
  public String getUpdateId() {
    return updateId;
  }

  @NonNull
  public List<TopologyEvent> getEvents() {
    return events;
  }

  @NonNull
  public Long getOffset() {
    return offset;
  }

  @NonNull
  public String getSynchronizerId() {
    return synchronizerId;
  }

  public TraceContextOuterClass.@NonNull TraceContext getTraceContext() {
    return traceContext;
  }

  @NonNull
  public Instant getRecordTime() {
    return recordTime;
  }

  public static TopologyTransaction fromProto(
      TopologyTransactionOuterClass.TopologyTransaction topologyTransaction) {
    List<TopologyEvent> events =
        topologyTransaction.getEventsList().stream()
            .map(TopologyEvent::fromProtoEvent)
            .collect(Collectors.toList());
    return new TopologyTransaction(
        topologyTransaction.getUpdateId(),
        topologyTransaction.getOffset(),
        topologyTransaction.getSynchronizerId(),
        Utils.instantFromProto(topologyTransaction.getRecordTime()),
        events,
        topologyTransaction.getTraceContext());
  }

  public TopologyTransactionOuterClass.TopologyTransaction toProto() {
    return TopologyTransactionOuterClass.TopologyTransaction.newBuilder()
        .setUpdateId(updateId)
        .addAllEvents(events.stream().map(TopologyEvent::toProtoEvent).collect(Collectors.toList()))
        .setOffset(offset)
        .setSynchronizerId(synchronizerId)
        .setTraceContext(traceContext)
        .setRecordTime(Utils.instantToProto(recordTime))
        .build();
  }

  @Override
  public String toString() {
    return "Transaction{"
        + "updateId='"
        + updateId
        + ", events="
        + events
        + ", offset='"
        + offset
        + '\''
        + ", synchronizerId='"
        + synchronizerId
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
    TopologyTransaction that = (TopologyTransaction) o;
    return Objects.equals(updateId, that.updateId)
        && Objects.equals(events, that.events)
        && Objects.equals(offset, that.offset)
        && Objects.equals(synchronizerId, that.synchronizerId)
        && Objects.equals(traceContext, that.traceContext)
        && Objects.equals(recordTime, that.recordTime);
  }

  @Override
  public int hashCode() {

    return Objects.hash(updateId, events, offset, synchronizerId, traceContext, recordTime);
  }
}
