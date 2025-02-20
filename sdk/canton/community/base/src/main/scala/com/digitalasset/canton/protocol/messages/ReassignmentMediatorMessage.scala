// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.{
  ReassignmentCommonData,
  ReassignmentViewTree,
  ViewConfirmationParameters,
  ViewPosition,
}

/** Message sent to the mediator as part of an unassignment or assignment request
  *
  * @param tree
  *   The unassignment|assignment view tree blinded for the mediator
  * @throws java.lang.IllegalArgumentException
  *   if the common data is blinded or the view is not blinded
  */
trait ReassignmentMediatorMessage extends MediatorConfirmationRequest with UnsignedProtocolMessage {
  def tree: ReassignmentViewTree
  def submittingParticipantSignature: Signature

  protected[this] def commonData: ReassignmentCommonData

  override def informeesAndConfirmationParamsByViewPosition
      : Map[ViewPosition, ViewConfirmationParameters] = {
    val confirmingParties =
      commonData.confirmingParties.map(_ -> NonNegativeInt.one).toMap
    val nonConfirmingParties = commonData.stakeholders.nonConfirming.map(_ -> NonNegativeInt.zero)

    val informees = confirmingParties ++ nonConfirmingParties
    val threshold = NonNegativeInt.tryCreate(confirmingParties.size)

    Map(tree.viewPosition -> ViewConfirmationParameters.create(informees, threshold))
  }
}
