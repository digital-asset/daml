// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*

/** Tags transaction views where all the view metadata are visible (such as in the views sent to participants).
  *
  * Note that the subviews and their metadata are not guaranteed to be visible.
  */
final case class ParticipantTransactionView private (view: TransactionView) {
  def unwrap: TransactionView = view
  def viewCommonData: ViewCommonData = view.viewCommonData.tryUnwrap
  def viewParticipantData: ViewParticipantData = view.viewParticipantData.tryUnwrap
}

object ParticipantTransactionView {

  final case class InvalidParticipantTransactionView(message: String)
      extends RuntimeException(message)

  def tryCreate(view: TransactionView): ParticipantTransactionView =
    view.viewCommonData.unwrap
      .leftMap(rh => s"Common data blinded (hash $rh)")
      .toValidatedNec
      .product(
        view.viewParticipantData.unwrap
          .leftMap(rh => s"Participant data blinded (hash $rh)")
          .toValidatedNec
      )
      .map(_ => new ParticipantTransactionView(view))
      .valueOr(err =>
        throw InvalidParticipantTransactionView(
          s"Unable to convert view (hash ${view.viewHash}) to a participant view: $err"
        )
      )

  def create(view: TransactionView): Either[String, ParticipantTransactionView] =
    Either.catchOnly[InvalidParticipantTransactionView](tryCreate(view)).leftMap(_.message)
}
