// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package ledger

import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Relation
import com.daml.lf.transaction.BlindingInfo
import com.daml.lf.transaction.Node
import com.daml.lf.transaction.{NodeId, VersionedTransaction}
import com.daml.lf.value.Value.ContractId
import com.daml.nameof.NameOf

object BlindingTransaction {

  private object BlindState {
    val Empty = BlindState(Relation.empty, Relation.empty, Relation.empty)
  }

  /** State to use while computing blindingInfo. */
  private final case class BlindState(
      disclosures: Relation[NodeId, Party],
      divulgences: Relation[ContractId, Party],
      contractVisibility: Relation[ContractId, Party], // parties that witness the contract
  ) {

    def discloseNode(
        witnesses: Set[Party],
        nid: NodeId,
    ): BlindState = {
      if (disclosures.contains(nid))
        InternalError.illegalArgumentException(
          NameOf.qualifiedNameOfCurrentFunc,
          s"discloseNode: nodeId already processed '$nid'.",
        )
      // Each node should be visible to someone
      copy(
        disclosures = disclosures.updated(nid, witnesses)
      )
    }

    def divulgeCoidTo(
        divulgees: Set[Party],
        witnesses: Set[Party],
        acoid: ContractId,
    ): BlindState = {
      copy(
        divulgences =
          if (divulgees.nonEmpty)
            divulgences.updated(acoid, divulgees union divulgences.getOrElse(acoid, Set.empty))
          else divulgences,
        contractVisibility = contractVisibility.updated(
          acoid,
          witnesses union contractVisibility.getOrElse(acoid, Set.empty),
        ),
      )
    }

  }

  /** Calculate blinding information for a transaction. */
  def calculateBlindingInfo(tx: VersionedTransaction): BlindingInfo = {
    val (info, _) = calculateBlindingInfoWithContactVisibility(tx)
    info
  }

  def calculateBlindingInfoWithContactVisibility(
      tx: VersionedTransaction
  ): (BlindingInfo, Relation[ContractId, Party]) = {
    val state = calculateBlindState(tx)
    (
      BlindingInfo(
        disclosure = state.disclosures,
        divulgence = state.divulgences,
      ),
      state.contractVisibility,
    )
  }

  private def calculateBlindState(
      tx: VersionedTransaction
  ): BlindState = {

    val initialParentExerciseWitnesses: Set[Party] = Set.empty

    def processNode(
        state0: BlindState,
        parentExerciseWitnesses: Set[Party],
        nodeId: NodeId,
    ): BlindState =
      tx.nodes.get(nodeId) match {
        case Some(action: Node.Action) =>
          val witnesses = parentExerciseWitnesses union action.informeesOfNode

          // actions of every type are disclosed to their witnesses
          val state = state0.discloseNode(witnesses, nodeId)

          action match {

            case _: Node.Create => state

            case lbk: Node.LookupByKey =>
              lbk.result.fold(state) { coid =>
                state.divulgeCoidTo(
                  Set.empty,
                  witnesses,
                  coid,
                )
              }

            // fetch & exercise nodes cause divulgence

            case fetch: Node.Fetch =>
              state.divulgeCoidTo(
                parentExerciseWitnesses -- fetch.stakeholders,
                witnesses,
                fetch.coid,
              )

            case ex: Node.Exercise =>
              val state1 =
                state.divulgeCoidTo(
                  (parentExerciseWitnesses union ex.choiceObservers) -- ex.stakeholders,
                  witnesses,
                  ex.targetCoid,
                )

              ex.children.foldLeft(state1) { (s, childNodeId) =>
                processNode(
                  s,
                  witnesses,
                  childNodeId,
                )
              }
          }

        case Some(rollback: Node.Rollback) =>
          // Rollback nodes are disclosed to the witnesses of the parent exercise.
          val state = state0.discloseNode(parentExerciseWitnesses, nodeId)
          rollback.children.foldLeft(state) { (s, childNodeId) =>
            processNode(
              s,
              parentExerciseWitnesses,
              childNodeId,
            )
          }
        case None =>
          InternalError.illegalArgumentException(
            NameOf.qualifiedNameOfCurrentFunc,
            s"processNode - precondition violated: node $nodeId not present",
          )
      }

    val finalState =
      tx.roots.foldLeft(BlindState.Empty) { (s, nodeId) =>
        processNode(s, initialParentExerciseWitnesses, nodeId)
      }

    finalState

  }

}
