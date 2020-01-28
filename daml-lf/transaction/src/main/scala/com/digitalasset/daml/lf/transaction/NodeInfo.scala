// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction

/** Trait for extracting information from an abstract node.
  * Used for sharing the implementation of common computations
  * over nodes and transactions.
  *
  * External codebases use these utilities on transaction and
  * node implementations that are not the one defined by [[Node]]
  * and hence the need for the indirection.
  */
sealed trait NodeInfo[PartyRep] extends Product with Serializable {

  /** Compute the informees of a node based on the ledger model definition.
    *
    * Refer to https://docs.daml.com/concepts/ledger-model/ledger-privacy.html#projections
    */
  def informeesOfNode: Set[PartyRep]
}

object NodeInfo {

  final case class Create[PartyRep](
      signatories: Set[PartyRep],
      stakeholders: Set[PartyRep],
  ) extends NodeInfo[PartyRep] {
    def informeesOfNode: Set[PartyRep] = stakeholders
  }

  final case class Fetch[PartyRep](
      signatories: Set[PartyRep],
      stakeholders: Set[PartyRep],
      actors: Set[PartyRep],
  ) extends NodeInfo[PartyRep] {
    def informeesOfNode: Set[PartyRep] = signatories | actors
  }

  final case class Exercise[PartyRep](
      consuming: Boolean,
      signatories: Set[PartyRep],
      stakeholders: Set[PartyRep],
      actors: Set[PartyRep],
  ) extends NodeInfo[PartyRep] {
    def informeesOfNode: Set[PartyRep] =
      if (consuming)
        stakeholders | actors
      else
        signatories | actors
  }

  final case class LookupByKey[PartyRep](
      keyMaintainers: Set[PartyRep],
      hasResult: Boolean,
  ) extends NodeInfo[PartyRep] {
    def informeesOfNode: Set[PartyRep] =
      // TODO(JM): In the successful case the informees should be the
      // signatories of the fetch contract. The signatories should be
      // added to the LookupByKey node, or a successful lookup should
      // become a Fetch.
      keyMaintainers
  }
}
