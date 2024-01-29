// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.data.EitherT
import cats.syntax.foldable.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.protocol.{Recipient, Recipients, RecipientsTree}
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient

import scala.concurrent.{ExecutionContext, Future}

/** Encodes the hierarchy of the witnesses of a view.
  *
  * By convention, the order is: the view's informees are at the head of the list, then the parent's views informees,
  * then the grandparent's, etc.
  */
final case class Witnesses(unwrap: NonEmpty[Seq[Set[Informee]]]) {
  import Witnesses.*

  def prepend(informees: Set[Informee]) = Witnesses(informees +: unwrap)

  /** Derive a recipient tree that mirrors the given hierarchy of witnesses. */
  def toRecipients(
      topology: PartyTopologySnapshotClient
  )(implicit ec: ExecutionContext): EitherT[Future, InvalidWitnesses, Recipients] =
    for {
      recipientsList <- unwrap.forgetNE.foldLeftM(Seq.empty[RecipientsTree]) {
        (children, informees) =>
          val parties = informees.map(_.party).toList
          for {
            informeeParticipants <- EitherT
              .right[InvalidWitnesses](
                topology
                  .activeParticipantsOfParties(parties)
              )
            _ <- {
              val informeesWithNoActiveParticipants =
                informeeParticipants
                  .collect {
                    case (party, participants) if participants.isEmpty => party
                  }
              EitherT.cond[Future](
                informeesWithNoActiveParticipants.isEmpty,
                (),
                InvalidWitnesses(
                  s"Found no active participants for informees: $informeesWithNoActiveParticipants"
                ),
              )
            }
            recipients = informeeParticipants.toList
              .flatMap { case (_, participants) =>
                participants.map(Recipient(_))
              }
              .toSet[Recipient]

            informeeRecipientSet <- EitherT.fromOption[Future](
              NonEmpty.from(recipients),
              InvalidWitnesses(s"Empty set of witnesses given"),
            )
          } yield Seq(
            RecipientsTree(informeeRecipientSet, children)
          )
      }
      // recipientsList is non-empty, because unwrap is.
      recipients = Recipients(NonEmptyUtil.fromUnsafe(recipientsList))
    } yield recipients

  def flatten: Set[Informee] = unwrap.foldLeft(Set.empty[Informee])(_ union _)

}

case object Witnesses {
  final case class InvalidWitnesses(message: String) extends PrettyPrinting {
    override def pretty: Pretty[InvalidWitnesses] = prettyOfClass(unnamedParam(_.message.unquoted))
  }
}
