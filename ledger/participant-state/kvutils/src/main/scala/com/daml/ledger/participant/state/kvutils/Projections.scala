// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.lf.data.BackStack
import com.daml.lf.data.Ref.Party
import com.daml.lf.transaction.{BlindingInfo, NodeId, Transaction}

final case class ProjectionRoots(
    party: Party,
    /* List of roots, in proper order. */
    roots: BackStack[NodeId],
)

object Projections {

  /** Compute the roots of per-party projections of a transaction.
    *
    * We use the projection roots in privacy-aware kvutils to establish
    * the ordering for the projected transaction roots. We need this as
    * we do not want to disclose the size or form of the transaction (which
    * we would leak if we'd keep the relative node identifiers). Instead
    * we keep an explicit list of roots for each party.
    */
  def computePerPartyProjectionRoots(
      tx: Transaction.Transaction,
      blindingInfo: BlindingInfo,
  ): List[ProjectionRoots] = {

    val perPartyRoots = tx.foldWithPathState(
      globalState0 = Map.empty[Party, BackStack[NodeId]],
      // On the path through the transaction tree we keep track of which
      // parties this part of the tree has been disclosed to.
      pathState0 = Set.empty[Party],
    ) { case (perPartyRoots, alreadyWitnessed, nodeId, node @ _) =>
      // Add this node as a root for each party that has not yet witnessed
      // the parent of this node (if there was one).
      // Note that we're using blinding info instead of repeating the authorization
      // logic from [[Ledger.enrichTransaction]] here.
      val witnesses = blindingInfo.disclosure(nodeId)
      (
        (witnesses -- alreadyWitnessed).foldLeft(perPartyRoots) { case (ppr, p) =>
          ppr.updated(p, ppr.getOrElse(p, BackStack.empty) :+ nodeId)
        },
        // Remember the new witnesses when continuing further down this path
        witnesses ++ alreadyWitnessed,
      )
    }
    perPartyRoots.toList.map(Function.tupled(ProjectionRoots))
  }
}
