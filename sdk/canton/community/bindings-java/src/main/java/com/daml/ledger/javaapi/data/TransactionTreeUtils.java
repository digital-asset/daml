// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransactionTreeUtils {

  private TransactionTreeUtils() {}

  /**
   * Constructs a tree described by a transaction tree.
   *
   * @param <WrappedEvent> the type of the wrapped events of the constructed tree
   * @param transactionTree the transaction tree
   * @param createWrappedEvent the function that constructs a WrappedEvent node of the tree given
   *     the current node and its converted children as a list of WrappedEvents nodes
   * @return the list of the wrapped root events. Method will be removed in 3.4.0
   */
  public static <WrappedEvent> List<WrappedEvent> buildTree(
      TransactionTree transactionTree,
      BiFunction<TreeEvent, List<WrappedEvent>, WrappedEvent> createWrappedEvent) {
    List<Node> nodes =
        transactionTree.getEventsById().values().stream()
            .map(
                treeEvent ->
                    new Node(
                        treeEvent.getNodeId(),
                        treeEvent.toProtoTreeEvent().hasExercised()
                            ? treeEvent.toProtoTreeEvent().getExercised().getLastDescendantNodeId()
                            : treeEvent.getNodeId()))
            .toList();

    List<Integer> rootNodeIds = transactionTree.getRootNodeIds();

    // fill the nodes with their children
    buildNodeForest(nodes, rootNodeIds);

    Map<Integer, Node> nodesById =
        nodes.stream().collect(Collectors.toMap(node -> node.nodeId, node -> node));

    Stream<Node> rootNodes =
        rootNodeIds.stream()
            .map(
                rootNodeId -> {
                  Node rootNode = nodesById.get(rootNodeId);
                  if (rootNode == null) {
                    throw new RuntimeException("Node with id " + rootNodeId + " not found");
                  } else return rootNode;
                });

    return rootNodes
        .map(
            rootNode ->
                buildWrappedEventTree(
                    rootNode, nodesById, transactionTree.getEventsById(), createWrappedEvent))
        .toList();
  }

  private static class Node {
    Integer nodeId;
    Integer lastDescendantNodeId;
    List<Node> children;

    Node(Integer nodeId, Integer lastDescendant) {
      this.nodeId = nodeId;
      this.lastDescendantNodeId = lastDescendant;
      this.children = new ArrayList<>();
    }
  }

  private static <WrappedEvent> WrappedEvent buildWrappedEventTree(
      Node root,
      Map<Integer, Node> nodesById,
      Map<Integer, TreeEvent> eventsById,
      BiFunction<TreeEvent, List<WrappedEvent>, WrappedEvent> createWrappedEvent) {
    Integer nodeId = root.nodeId;
    Node node = nodesById.get(nodeId);
    if (node == null) {
      throw new RuntimeException("node with id " + nodeId + " not found");
    }

    TreeEvent treeEvent = eventsById.get(nodeId);
    if (treeEvent == null) {
      throw new RuntimeException("TreeEvent with id " + nodeId + " not found");
    }

    // build children subtrees
    List<WrappedEvent> childrenWrapped =
        node.children.stream()
            .map(child -> buildWrappedEventTree(child, nodesById, eventsById, createWrappedEvent))
            .collect(Collectors.toList());

    return createWrappedEvent.apply(treeEvent, childrenWrapped);
  }

  // fill nodes with their children based on the last descendant
  private static void buildNodeForest(List<Node> nodes, List<Integer> rootNodeIds) {

    List<Node> rootNodes =
        nodes.stream()
            .sorted(Comparator.comparingInt(node -> node.nodeId))
            .filter(node -> rootNodeIds.contains(node.nodeId))
            .toList();

    // ensure that nodes are sorted
    List<Node> nodesSorted =
        nodes.stream().sorted(Comparator.comparingInt(node -> node.nodeId)).toList();

    for (Node root : rootNodes) {
      buildNodeTree(root, nodesSorted);
    }
  }

  private static void buildNodeTree(Node root, List<Node> allNodes) {
    Deque<Node> stack = new ArrayDeque<>();
    stack.push(root);

    for (Node node : allNodes) {
      // Skip nodes that are not descendants of this root
      if (node.nodeId <= root.nodeId || node.nodeId > root.lastDescendantNodeId) continue;

      // Pop nodes from the stack if the current node is not a descendant of the top node
      while (!stack.isEmpty() && node.nodeId > stack.peek().lastDescendantNodeId) {
        stack.pop();
      }

      // Add the current node as a child of the top node in the stack
      if (!stack.isEmpty()) {
        stack.peek().children.add(node);
      }

      // Push the current node onto the stack
      stack.push(node);
    }
  }
}
