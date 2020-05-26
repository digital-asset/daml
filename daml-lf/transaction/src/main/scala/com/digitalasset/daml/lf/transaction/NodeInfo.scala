// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.transaction

import com.daml.lf.data.Ref.Party

/** Trait for extracting information from an abstract node.
  * Used for sharing the implementation of common computations
  * over nodes and transactions.
  *
  * External codebases use these utilities on transaction and
  * node implementations that are not the one defined by [[Node]]
  * and hence the need for the indirection.
  */
trait NodeInfo {

  /** Compute the informees of a node based on the ledger model definition.
    *
    * Refer to https://docs.daml.com/concepts/ledger-model/ledger-privacy.html#projections
    */
  def informeesOfNode: Set[Party]

  /** Required authorizers (see ledger model); UNSAFE TO USE on fetch nodes of transaction with versions < 5
    *
    * The ledger model defines the fetch node actingParties as the nodes' required authorizers.
    * However, the our transaction data structure did not include the actingParties in versions < 5.
    * The usage of this method must thus be restricted to:
    * 1. settings where no fetch nodes appear (for example, the `validate` method of DAMLe, which uses it on root
    *    nodes, which are guaranteed never to contain a fetch node)
    * 2. DAML ledger implementations that do not store or process any transactions with version < 5
    *
    */
  def requiredAuthorizers: Set[Party]
}

object NodeInfo {

  trait Create extends NodeInfo {
    def signatories: Set[Party]
    def stakeholders: Set[Party]

    final def requiredAuthorizers: Set[Party] = signatories
    final def informeesOfNode: Set[Party] = stakeholders
  }

  trait Fetch extends NodeInfo {
    def signatories: Set[Party]
    def stakeholders: Set[Party]
    def actingParties: Option[Set[Party]]

    final def requiredAuthorizers: Set[Party] =
      actingPartiesOrThrow
    final def informeesOfNode: Set[Party] =
      signatories | actingPartiesOrThrow

    private[this] def actingPartiesOrThrow: Set[Party] =
      actingParties.fold(
        throw new IllegalStateException(
          "'Fetch.actingParties' should always be present but is 'None'"))(identity)
  }

  trait Exercise extends NodeInfo {

    def consuming: Boolean
    def signatories: Set[Party]
    def stakeholders: Set[Party]
    def actingParties: Set[Party]

    final def requiredAuthorizers(): Set[Party] = actingParties

    final def informeesOfNode: Set[Party] =
      if (consuming)
        stakeholders | actingParties
      else
        signatories | actingParties
  }

  trait LookupByKey extends NodeInfo {
    def keyMaintainers: Set[Party]
    def hasResult: Boolean

    final def requiredAuthorizers(): Set[Party] = keyMaintainers
    final def informeesOfNode: Set[Party] =
      // TODO(JM): In the successful case the informees should be the
      // signatories of the fetch contract. The signatories should be
      // added to the LookupByKey node, or a successful lookup should
      // become a Fetch.
      keyMaintainers
  }

}
