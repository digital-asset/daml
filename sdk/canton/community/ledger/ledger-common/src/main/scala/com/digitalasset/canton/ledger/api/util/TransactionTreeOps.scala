// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.util

import com.daml.ledger.api.v2.event.ExercisedEvent
import com.daml.ledger.api.v2.transaction.TransactionTree

import scala.annotation.tailrec

object TransactionTreeOps {

  implicit class TransactionTreeOps(val tree: TransactionTree) extends AnyVal {

    /** Computes the node ids of the root nodes (i.e. the nodes that do not have any ancestors). A node can be
      * considered a root if there are not any ancestors of it. There is no guarantee that the root node was also a root
      * in the original transaction tree (i.e. before filtering out events from the original transaction tree).
      *
      * @return the root node ids
      */
    def rootNodeIds(): List[Int] = {
      val lastDescendantById = tree.eventsById.view
        .mapValues(_.kind.exercised)
        .map { case (nodeId, exercisedO) =>
          (nodeId, exercisedO.fold(nodeId)(_.lastDescendantNodeId))
        }
        .toMap

      val sortedNodeIds = tree.eventsById.keys.toList.sorted

      @tailrec
      def go(remainingNodes: List[Int], roots: List[Int]): List[Int] =
        remainingNodes match {
          case Nil => roots.reverse
          case nodeId :: tail =>
            val lastDescendant = lastDescendantById.getOrElse(
              nodeId,
              throw new RuntimeException(
                s"The last descendant for the node with id $nodeId was unexpectedly not found!"
              ),
            )
            val newRoots = nodeId :: roots
            // skip all nodes within the range of the last descendant
            val remaining = tail.dropWhile(_ <= lastDescendant)
            go(remaining, newRoots)
        }

      go(sortedNodeIds, List.empty)
    }

    /** Computes the children nodes of an exercised event. It finds the candidate nodes that could be
      * children of the event given (i.e. its descendants). Then it repeatedly finds from the candidates the one with
      * the lowest id and adds it to its children and removes the child's descendants from the list with the candidates.
      * A node can be considered a child of another node if there are not any intermediate descendants between its
      * parent and itself. There is no guarantee that the child was a child of its parent in the original transaction
      * tree (i.e. before filtering out events from the original transaction tree).
      *
      * @param exercised the exercised event
      * @return the children's node ids
      */
    def childNodeIds(exercised: ExercisedEvent): List[Int] = {
      val nodeId = exercised.nodeId
      val lastDescendant = exercised.lastDescendantNodeId

      val candidatesMap =
        tree.eventsById.view.filter { case (id, _) => id > nodeId && id <= lastDescendant }

      val candidates = candidatesMap.keys.toList.sorted

      val lastDescendantById = candidatesMap
        .mapValues(_.kind.exercised)
        .map { case (nodeId, exercisedO) =>
          (nodeId, exercisedO.fold(nodeId)(_.lastDescendantNodeId))
        }
        .toMap

      @tailrec
      def go(remainingNodes: List[Int], children: List[Int]): List[Int] =
        remainingNodes match {
          case Nil => children.reverse
          // first candidate will always be a child since it is not a descendant of another intermediate node
          case child :: restCandidates =>
            val lastDescendant = lastDescendantById.getOrElse(
              child,
              throw new RuntimeException(
                s"The node with id $child was unexpectedly not found!"
              ),
            )
            // add child to children and skip its descendants
            val remainingCandidates = restCandidates.dropWhile(_ <= lastDescendant)
            go(remainingCandidates, child :: children)
        }

      go(candidates, List.empty)
    }
  }
}
