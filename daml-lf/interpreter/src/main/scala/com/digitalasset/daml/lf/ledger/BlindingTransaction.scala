// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.ledger

import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Relation.Relation
import com.daml.lf.transaction.BlindingInfo
import com.daml.lf.transaction.Node.{NodeCreate, NodeExercises, NodeFetch, NodeLookupByKey}
import com.daml.lf.transaction.{NodeId, Transaction => Tx}
import com.daml.lf.value.Value.ContractId

object BlindingTransaction {

  def crash(reason: String) =
    throw new IllegalArgumentException(reason)

  private object BlindState {
    val Empty =
      BlindState(Map.empty, Map.empty)
  }

  /** State to use while computing blindingInfo. */
  private final case class BlindState(
      disclosures: Relation[NodeId, Party],
      divulgences: Relation[ContractId, Party],
  ) {

    def discloseNode(
        witnesses: Set[Party],
        nid: NodeId,
    ): BlindState = {
      if (disclosures.contains(nid))
        crash(s"discloseNode: nodeId already processed '$nid'.")
      copy(
        disclosures = disclosures.updated(nid, witnesses)
      )
    }

    def divulgeCoidTo(witnesses: Set[Party], acoid: ContractId): BlindState = {
      copy(
        divulgences = divulgences
          .updated(acoid, witnesses union divulgences.getOrElse(acoid, Set.empty)),
      )
    }

  }

  /** Calculate blinding information for a transaction. */
  def calculateBlindingInfo(
      tx: Tx.Transaction,
  ): BlindingInfo = {

    val initialParentExerciseWitnesses: Set[Party] = Set.empty

    def processNode(
        state0: BlindState,
        parentExerciseWitnesses: Set[Party],
        nodeId: NodeId,
    ): BlindState = {
      val node =
        tx.nodes
          .getOrElse(
            nodeId,
            throw new IllegalArgumentException(
              s"processNode - precondition violated: node $nodeId not present"))
      val witnesses = parentExerciseWitnesses union node.informeesOfNode

      // nodes of every type are disclosed to their witnesses
      val state = state0.discloseNode(witnesses, nodeId)

      node match {
        case _: NodeCreate[ContractId] => state
        case _: NodeLookupByKey[ContractId] => state

        // fetch & exercise nodes cause divulgence

        case fetch: NodeFetch[ContractId] =>
          state
            .divulgeCoidTo(parentExerciseWitnesses -- fetch.stakeholders, fetch.coid)

        case ex: NodeExercises[NodeId, ContractId] =>
          val state1 =
            state.divulgeCoidTo(
              (parentExerciseWitnesses union ex.choiceObservers) -- ex.stakeholders,
              ex.targetCoid)

          ex.children.foldLeft(state1) { (s, childNodeId) =>
            processNode(
              s,
              witnesses,
              childNodeId,
            )
          }
      }
    }

    val finalState =
      tx.roots.foldLeft(BlindState.Empty) { (s, nodeId) =>
        processNode(s, initialParentExerciseWitnesses, nodeId)
      }

    BlindingInfo(
      disclosure = finalState.disclosures,
      divulgence = finalState.divulgences,
    )
  }

}
