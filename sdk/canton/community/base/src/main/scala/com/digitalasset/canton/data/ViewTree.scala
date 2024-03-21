// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{RootHash, ViewHash}
import com.digitalasset.canton.topology.{DomainId, MediatorRef}

/** Common supertype of all view trees that are sent as [[com.digitalasset.canton.protocol.messages.EncryptedViewMessage]]s */
trait ViewTree extends PrettyPrinting {

  /** The informees of the view in the tree */
  def informees: Set[LfPartyId]

  /** Return the hash whose signature is to be included in the [[com.digitalasset.canton.protocol.messages.EncryptedViewMessage]] */
  def toBeSigned: Option[RootHash]

  /** The hash of the view */
  def viewHash: ViewHash

  def viewPosition: ViewPosition

  /** The root hash of the view tree.
    *
    * Two view trees with the same [[rootHash]] must also have the same [[domainId]] and [[mediator]]
    * (except for hash collisions).
    */
  def rootHash: RootHash

  /** The domain to which the [[com.digitalasset.canton.protocol.messages.EncryptedViewMessage]] should be sent to */
  def domainId: DomainId

  /** The mediator that is responsible for coordinating this request */
  def mediator: MediatorRef

  override def pretty: Pretty[this.type]
}

/** Supertype of [[FullTransferOutTree]] and [[FullTransferInTree]]
  */
trait TransferViewTree extends ViewTree {
  def submitterMetadata: TransferSubmitterMetadata

  val viewPosition: ViewPosition =
    ViewPosition.root // Use a dummy value, as there is only one view.
}
