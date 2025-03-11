// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TraceContextOuterClass;
import com.daml.ledger.api.v2.TransactionOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Instant;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public final class Transaction {

  @NonNull private final String updateId;

  @NonNull private final String commandId;

  @NonNull private final String workflowId;

  @NonNull private final Instant effectiveAt;

  @NonNull private final List<@NonNull Event> events;

  @NonNull private final Long offset;

  @NonNull private final String synchronizerId;

  private final TraceContextOuterClass.@NonNull TraceContext traceContext;

  @NonNull private final Instant recordTime;

  public Transaction(
      @NonNull String updateId,
      @NonNull String commandId,
      @NonNull String workflowId,
      @NonNull Instant effectiveAt,
      @NonNull List<@NonNull Event> events,
      @NonNull Long offset,
      @NonNull String synchronizerId,
      TraceContextOuterClass.@NonNull TraceContext traceContext,
      @NonNull Instant recordTime) {
    this.updateId = updateId;
    this.commandId = commandId;
    this.workflowId = workflowId;
    this.effectiveAt = effectiveAt;
    this.events = events;
    this.offset = offset;
    this.synchronizerId = synchronizerId;
    this.traceContext = traceContext;
    this.recordTime = recordTime;
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

  /** Returns the events of the transaction by their (node) id. */
  @NonNull
  public Map<Integer, Event> getEventsById() {
    return getEvents().stream().collect(Collectors.toMap(Event::getNodeId, event -> event));
  }

  /**
   * Computes the node ids of the root nodes (i.e. the nodes that do not have any ancestors). A node
   * can be considered a root if there are not any ancestors of it. There is no guarantee that the
   * root node was also a root in the original transaction (i.e. before filtering out events from
   * the original transaction). In the case that the transaction is returned in AcsDelta shape all
   * the events returned will trivially be root nodes.
   *
   * @return the root node ids
   */
  @NonNull
  public List<Integer> getRootNodeIds() {
    Map<Integer, Integer> lastDescendantById =
        getEvents().stream()
            .collect(
                Collectors.toMap(
                    Event::getNodeId,
                    event ->
                        event.toProtoEvent().hasExercised()
                            ? event.toProtoEvent().getExercised().getLastDescendantNodeId()
                            : event.getNodeId()));

    List<Integer> nodeIds = getEvents().stream().map(Event::getNodeId).sorted().toList();

    List<Integer> rootNodes = new ArrayList<>();

    int index = 0;
    while (index < nodeIds.size()) {
      Integer nodeId = nodeIds.get(index);
      Integer lastDescendant = lastDescendantById.get(nodeId);
      if (lastDescendant == null) {
        throw new RuntimeException("Node with id " + nodeId + " not found");
      }

      rootNodes.add(nodeId);
      while (index < nodeIds.size() && nodeIds.get(index) <= lastDescendant) {
        index++;
      }
    }

    return rootNodes;
  }

  /**
   * Computes the children nodes of an exercised event. It finds the candidate nodes that could be
   * children of the event given (i.e. its descendants). Then it repeatedly finds from the
   * candidates the one with the lowest id and adds it to its children and removes the child's
   * descendants from the list with the candidates. A node can be considered a child of another node
   * if there are not any intermediate descendants between its parent and itself. There is no
   * guarantee that the child was a child of its parent in the original transaction (i.e. before
   * filtering out events from the original transaction).
   *
   * @param exercised the exercised event
   * @return the children's node ids
   */
  @NonNull
  public List<@NonNull Integer> getChildNodeIds(ExercisedEvent exercised) {
    Integer nodeId = exercised.getNodeId();
    Integer lastDescendant = exercised.getLastDescendantNodeId();

    List<Event> candidates =
        getEvents().stream()
            .filter(event -> event.getNodeId() > nodeId && event.getNodeId() <= lastDescendant)
            .sorted(Comparator.comparing(Event::getNodeId))
            .toList();

    List<Integer> childNodes = new ArrayList<>();

    int index = 0;
    while (index < candidates.size()) {
      Event node = candidates.get(index);
      // first candidate will always be a child since it is not a descendant of another intermediate
      // node
      Integer childNodeId = node.getNodeId();
      Integer childLastDescendant =
          node.toProtoEvent().hasExercised()
              ? node.toProtoEvent().getExercised().getLastDescendantNodeId()
              : childNodeId;

      // add child to children and skip its descendants
      childNodes.add(childNodeId);
      index++;
      while (index < candidates.size()
          && candidates.get(index).getNodeId() <= childLastDescendant) {
        index++;
      }
    }

    return childNodes;
  }

  /**
   * A generic class that encapsulates a transaction tree along with a list of the wrapped root
   * events of the tree. The wrapped root events are used to construct the tree that is described by
   * the transaction as a tree of WrappedEvents.
   *
   * @param <WrappedEvent> the type of the wrapped events
   */
  public static class WrappedTransactionTree<WrappedEvent> {
    /** The original transaction. */
    private final Transaction transaction;
    /** The list of wrapped root events generated from the transaction. */
    private final List<WrappedEvent> wrappedRootEvents;

    public WrappedTransactionTree(Transaction transaction, List<WrappedEvent> wrappedRootEvents) {
      this.transaction = transaction;
      this.wrappedRootEvents = wrappedRootEvents;
    }

    public Transaction getTransaction() {
      return transaction;
    }

    public List<WrappedEvent> getWrappedRootEvents() {
      return wrappedRootEvents;
    }
  }

  /**
   * Constructs a tree described by the transaction.
   *
   * @param <WrappedEvent> the type of the wrapped events of the constructed tree
   * @param createWrappedEvent the function that constructs a WrappedEvent node of the tree given
   *     the current node and its converted children as a list of WrappedEvents nodes
   * @return the original transaction tree and the list of the wrapped root events
   */
  public <WrappedEvent> WrappedTransactionTree<WrappedEvent> toWrappedTree(
      BiFunction<Event, List<WrappedEvent>, WrappedEvent> createWrappedEvent) {

    List<WrappedEvent> wrappedRootEvents = TransactionUtils.buildTree(this, createWrappedEvent);

    return new WrappedTransactionTree<>(this, wrappedRootEvents);
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
        transaction.getSynchronizerId(),
        transaction.getTraceContext(),
        Utils.instantFromProto(transaction.getRecordTime()));
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
    Transaction that = (Transaction) o;
    return Objects.equals(updateId, that.updateId)
        && Objects.equals(commandId, that.commandId)
        && Objects.equals(workflowId, that.workflowId)
        && Objects.equals(effectiveAt, that.effectiveAt)
        && Objects.equals(events, that.events)
        && Objects.equals(offset, that.offset)
        && Objects.equals(synchronizerId, that.synchronizerId)
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
        events,
        offset,
        synchronizerId,
        traceContext,
        recordTime);
  }
}
