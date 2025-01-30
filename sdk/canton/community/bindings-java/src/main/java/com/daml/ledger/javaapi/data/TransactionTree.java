// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TraceContextOuterClass;
import com.daml.ledger.api.v2.TransactionOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public final class TransactionTree {

  @NonNull private final String updateId;

  @NonNull private final String commandId;

  @NonNull private final String workflowId;

  @NonNull private final Instant effectiveAt;

  @NonNull private final Long offset;

  @NonNull private final Map<Integer, TreeEvent> eventsById;

  @NonNull private final String synchronizerId;

  private final TraceContextOuterClass.@NonNull TraceContext traceContext;

  @NonNull private final Instant recordTime;

  public TransactionTree(
      @NonNull String updateId,
      @NonNull String commandId,
      @NonNull String workflowId,
      @NonNull Instant effectiveAt,
      @NonNull Long offset,
      @NonNull Map<@NonNull Integer, @NonNull TreeEvent> eventsById,
      @NonNull String synchronizerId,
      TraceContextOuterClass.@NonNull TraceContext traceContext,
      @NonNull Instant recordTime) {
    this.updateId = updateId;
    this.commandId = commandId;
    this.workflowId = workflowId;
    this.effectiveAt = effectiveAt;
    this.offset = offset;
    this.eventsById = eventsById;
    this.synchronizerId = synchronizerId;
    this.traceContext = traceContext;
    this.recordTime = recordTime;
  }

  public static TransactionTree fromProto(TransactionOuterClass.TransactionTree tree) {
    Instant effectiveAt =
        Instant.ofEpochSecond(tree.getEffectiveAt().getSeconds(), tree.getEffectiveAt().getNanos());
    Map<Integer, TreeEvent> eventsById =
        tree.getEventsByIdMap().values().stream()
            .collect(
                Collectors.toMap(
                    e -> {
                      if (e.hasCreated()) return e.getCreated().getNodeId();
                      else if (e.hasExercised()) return e.getExercised().getNodeId();
                      else
                        throw new IllegalArgumentException(
                            "Event is neither created nor exercised: " + e);
                    },
                    TreeEvent::fromProtoTreeEvent));
    return new TransactionTree(
        tree.getUpdateId(),
        tree.getCommandId(),
        tree.getWorkflowId(),
        effectiveAt,
        tree.getOffset(),
        eventsById,
        tree.getSynchronizerId(),
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
                .collect(Collectors.toMap(TreeEvent::getNodeId, TreeEvent::toProtoTreeEvent)))
        .setSynchronizerId(synchronizerId)
        .setTraceContext(traceContext)
        .setRecordTime(Utils.instantToProto(recordTime))
        .build();
  }

  /**
   * A generic class that encapsulates a transaction tree along with a list of the wrapped root
   * events of the tree. The wrapped root events are used to construct the tree that is described by
   * the transaction tree as a tree of WrappedEvents.
   *
   * @param <WrappedEvent> the type of the wrapped events
   */
  public static class WrappedTransactionTree<WrappedEvent> {
    /** The original transaction tree. */
    private final TransactionTree transactionTree;
    /** The list of wrapped root events generated from the transaction tree. */
    private final List<WrappedEvent> wrappedRootEvents;

    public WrappedTransactionTree(
        TransactionTree transactionTree, List<WrappedEvent> wrappedRootEvents) {
      this.transactionTree = transactionTree;
      this.wrappedRootEvents = wrappedRootEvents;
    }

    public TransactionTree getTransactionTree() {
      return transactionTree;
    }

    public List<WrappedEvent> getWrappedRootEvents() {
      return wrappedRootEvents;
    }
  }

  /**
   * Constructs a tree described by the transaction tree.
   *
   * @param <WrappedEvent> the type of the wrapped events of the constructed tree
   * @param createWrappedEvent the function that constructs a WrappedEvent node of the tree given
   *     the current node and its converted children as a list of WrappedEvents nodes
   * @return the original transaction tree and the list of the wrapped root events
   */
  public <WrappedEvent> WrappedTransactionTree<WrappedEvent> toWrappedTree(
      BiFunction<TreeEvent, List<WrappedEvent>, WrappedEvent> createWrappedEvent) {

    List<WrappedEvent> wrappedRootEvents = TransactionTreeUtils.buildTree(this, createWrappedEvent);

    return new WrappedTransactionTree<>(this, wrappedRootEvents);
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
  public Long getOffset() {
    return offset;
  }

  @NonNull
  public Map<Integer, TreeEvent> getEventsById() {
    return eventsById;
  }

  /**
   * Computes the node ids of the root nodes (i.e. the nodes that do not have any ancestors). A node
   * can be considered a root if there are not any ancestors of it. There is no guarantee that the
   * root node was also a root in the original transaction tree (i.e. before filtering out events
   * from the original transaction tree).
   *
   * @return the root node ids
   */
  @NonNull
  public List<Integer> getRootNodeIds() {
    Map<Integer, Integer> lastDescendantById =
        getEventsById().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry ->
                        entry.getValue().toProtoTreeEvent().hasExercised()
                            ? entry
                                .getValue()
                                .toProtoTreeEvent()
                                .getExercised()
                                .getLastDescendantNodeId()
                            : entry.getKey()));

    List<Integer> nodeIds = getEventsById().keySet().stream().sorted().toList();

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

  /**
   * Computes the children nodes of an exercised event. It finds the candidate nodes that could be
   * children of the event given (i.e. its descendants). Then it repeatedly finds from the
   * candidates the one with the lowest id and adds it to its children and removes the child's
   * descendants from the list with the candidates. A node can be considered a child of another node
   * if there are not any intermediate descendants between its parent and itself. There is no
   * guarantee that the child was a child of its parent in the original transaction tree (i.e.
   * before filtering out events from the original transaction tree).
   *
   * @param exercised the exercised event
   * @return the children's node ids
   */
  @NonNull
  public List<@NonNull Integer> getChildNodeIds(ExercisedEvent exercised) {
    Integer nodeId = exercised.getNodeId();
    Integer lastDescendant = exercised.getLastDescendantNodeId();

    List<TreeEvent> candidates =
        getEventsById().entrySet().stream()
            .filter(entry -> entry.getKey() > nodeId && entry.getKey() <= lastDescendant)
            .sorted(Map.Entry.comparingByKey())
            .map(Map.Entry::getValue)
            .toList();

    List<Integer> childNodes = new ArrayList<>();

    int index = 0;
    while (index < candidates.size()) {
      TreeEvent node = candidates.get(index);
      // first candidate will always be a child since it is not a descendant of another intermediate
      // node
      Integer childNodeId = node.getNodeId();
      Integer childLastDescendant =
          node.toProtoTreeEvent().hasExercised()
              ? node.toProtoTreeEvent().getExercised().getLastDescendantNodeId()
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
    TransactionTree that = (TransactionTree) o;
    return Objects.equals(updateId, that.updateId)
        && Objects.equals(commandId, that.commandId)
        && Objects.equals(workflowId, that.workflowId)
        && Objects.equals(effectiveAt, that.effectiveAt)
        && Objects.equals(eventsById, that.eventsById)
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
        offset,
        eventsById,
        synchronizerId,
        traceContext,
        recordTime);
  }
}
