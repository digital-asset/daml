// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction

/** Trait for extracting information from an abstract node.
  * Used for sharing the implementation of common computations
  * over nodes and transactions.
  */
trait NodeInfo[PartyRep] {

  /** The node kind, e.g. create, consuming exercise etc. */
  def kind: NodeKind

  /** The signatories of the contract associated with the node. */
  def signatories: Set[PartyRep]

  /** The stakeholders of the contract associated with the node. */
  def stakeholders: Set[PartyRep]

  /** The actors (e.g. the controlling parties of the choice) of the node. */
  def actors: Set[PartyRep]
}

object NodeInfo {

  /** Compute the informees of a node based on the ledger model definition.
    *
    * Refer to https://docs.daml.com/concepts/ledger-model/ledger-privacy.html#projections
    */
  def informeesOfNode[PartyRep](node: NodeInfo[PartyRep]): Set[PartyRep] =
    node.kind match {
      case NodeKind.Create => node.stakeholders
      case NodeKind.Fetch => node.signatories | node.actors
      case NodeKind.ExerciseConsuming => node.stakeholders | node.actors
      case NodeKind.ExerciseNonConsuming => node.signatories | node.actors
      case NodeKind.LookupByKey => Set.empty
    }
}
