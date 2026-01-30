// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TraceContextOuterClass;
import com.daml.ledger.api.v2.TransactionOuterClass;
import com.google.protobuf.ByteString;
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

  @NonNull private final Map<Integer, @NonNull Node> nodesById;

  // The root nodes of a forest of nodes representing the transaction.
  @NonNull private final List<@NonNull Node> rootNodes;

  @NonNull private final List<@NonNull Integer> rootNodeIds;

  @NonNull private final ByteString externalTransactionHash;

  public static class Node {
    private final Integer nodeId;
    private final Integer lastDescendantNodeId;
    private final List<Node> children;

    public Node(Integer nodeId, Integer lastDescendant) {
      this.nodeId = nodeId;
      this.lastDescendantNodeId = lastDescendant;
      this.children = new ArrayList<>();
    }

    public Integer getNodeId() {
      return nodeId;
    }

    public Integer getLastDescendantNodeId() {
      return lastDescendantNodeId;
    }

    public List<Node> getChildren() {
      return Collections.unmodifiableList(children);
    }
  }

  // fills nodes with their children based on the last descendant and returns the root nodes
  private static List<Node> buildNodeForest(LinkedList<Node> nodes) {
    List<Node> rootNodes = new ArrayList<>();

    while (!nodes.isEmpty()) {
      Node root = nodes.pollFirst();
      rootNodes.add(root);
      buildNodeTree(root, nodes);
    }

    return Collections.unmodifiableList(rootNodes);
  }

  private static void buildNodeTree(Node parent, LinkedList<Node> nodes) {
    while (!nodes.isEmpty() && nodes.peekFirst().nodeId <= parent.lastDescendantNodeId) {
      parent.children.add(nodes.peekFirst());
      buildNodeTree(nodes.pollFirst(), nodes);
    }
  }

  public Transaction(
      @NonNull String updateId,
      @NonNull String commandId,
      @NonNull String workflowId,
      @NonNull Instant effectiveAt,
      @NonNull List<@NonNull Event> events,
      @NonNull Long offset,
      @NonNull String synchronizerId,
      TraceContextOuterClass.@NonNull TraceContext traceContext,
      @NonNull Instant recordTime,
      @NonNull ByteString externalTransactionHash) {
    // Check if events are sorted by nodeId
    for (int i = 1; i < events.size(); i++) {
      if (events.get(i - 1).getNodeId() > events.get(i).getNodeId()) {
        throw new IllegalArgumentException("Events are not sorted by nodeId at index " + i);
      }
    }

    this.updateId = updateId;
    this.commandId = commandId;
    this.workflowId = workflowId;
    this.effectiveAt = effectiveAt;
    this.events = events;
    this.offset = offset;
    this.synchronizerId = synchronizerId;
    this.traceContext = traceContext;
    this.recordTime = recordTime;

    LinkedList<Node> nodes =
        this.getEvents().stream()
            .map(
                event ->
                    new Node(
                        event.getNodeId(),
                        event.toProtoEvent().hasExercised()
                            ? event.toProtoEvent().getExercised().getLastDescendantNodeId()
                            : event.getNodeId()))
            .collect(Collectors.toCollection(LinkedList::new));

    this.nodesById =
        nodes.stream().collect(Collectors.toUnmodifiableMap(node -> node.nodeId, node -> node));

    this.rootNodes = buildNodeForest(nodes);

    this.rootNodeIds = rootNodes.stream().map(node -> node.nodeId).toList();
    this.externalTransactionHash = externalTransactionHash;
  }

  @Deprecated
  Transaction(
      @NonNull String updateId,
      @NonNull String commandId,
      @NonNull String workflowId,
      @NonNull Instant effectiveAt,
      @NonNull List<@NonNull Event> events,
      @NonNull Long offset,
      @NonNull String synchronizerId,
      TraceContextOuterClass.@NonNull TraceContext traceContext,
      @NonNull Instant recordTime) {
    this(
        updateId,
        commandId,
        workflowId,
        effectiveAt,
        events,
        offset,
        synchronizerId,
        traceContext,
        recordTime,
        ByteString.EMPTY);
  }

  @NonNull
  public ByteString getExternalTransactionHash() {
    return externalTransactionHash;
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
    return rootNodeIds;
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
  public List<@NonNull Integer> getChildNodeIds(ExercisedEvent exercised) {
    final Integer nodeId = exercised.getNodeId();
    Node parentNode = nodesById.get(nodeId);
    if (parentNode == null) {
      throw new RuntimeException("Node with id " + nodeId + " not found.");
    }
    return parentNode.children.stream().map(child -> child.nodeId).collect(Collectors.toList());
  }

  /**
   * Returns the nodes of the transaction as a forest of nodes. The forest is a list of root nodes
   * that are connected to their children.
   *
   * @return the root nodes of the forest of nodes representing the transaction
   */
  public @NonNull List<@NonNull Node> getRootNodes() {
    return rootNodes;
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

    List<WrappedEvent> wrappedRootEvents =
        this.getRootNodes().stream()
            .map(
                rootNode ->
                    buildWrappedEventTree(rootNode, this.getEventsById(), createWrappedEvent))
            .toList();

    return new WrappedTransactionTree<>(this, wrappedRootEvents);
  }

  private static <WrappedEvent> WrappedEvent buildWrappedEventTree(
      Transaction.Node root,
      Map<Integer, Event> eventsById,
      BiFunction<Event, List<WrappedEvent>, WrappedEvent> createWrappedEvent) {
    final Integer nodeId = root.getNodeId();

    final Event event = eventsById.get(nodeId);
    if (event == null) {
      throw new RuntimeException("Event with id " + nodeId + " not found");
    }

    // build children subtrees
    final List<WrappedEvent> childrenWrapped =
        root.getChildren().stream()
            .map(child -> buildWrappedEventTree(child, eventsById, createWrappedEvent))
            .collect(Collectors.toList());

    return createWrappedEvent.apply(event, childrenWrapped);
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
        Utils.instantFromProto(transaction.getRecordTime()),
        transaction.getExternalTransactionHash());
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
        .setExternalTransactionHash(externalTransactionHash)
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
        + ", externalTransactionHash="
        + externalTransactionHash
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
        && Objects.equals(recordTime, that.recordTime)
        && Objects.equals(externalTransactionHash, that.externalTransactionHash);
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
        recordTime,
        externalTransactionHash);
  }
}
