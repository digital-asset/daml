// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.LedgerString.ordering
import com.daml.lf.value.{Value => Lf}
import scalaz.syntax.tag._
import scalaz.{@@, Tag}

import java.net.URL
import scala.util.Try
import scala.util.control.NonFatal

object domain {

  type Value = Lf

  sealed trait CommandIdTag

  type CommandId = Ref.CommandId @@ CommandIdTag
  val CommandId: Tag.TagOf[CommandIdTag] = Tag.of[CommandIdTag]

  sealed trait TransactionIdTag

  type TransactionId = Ref.TransactionId @@ TransactionIdTag
  val TransactionId: Tag.TagOf[TransactionIdTag] = Tag.of[TransactionIdTag]

  sealed trait ContractIdTag

  type ContractId = Ref.ContractIdString @@ ContractIdTag
  val ContractId: Tag.TagOf[ContractIdTag] = Tag.of[ContractIdTag]

  sealed trait EventIdTag

  type EventId = Ref.LedgerString @@ EventIdTag
  val EventId: Tag.TagOf[EventIdTag] = Tag.of[EventIdTag]
  implicit val eventIdOrdering: Ordering[EventId] =
    Ordering.by[EventId, Ref.LedgerString](_.unwrap)

  sealed trait LedgerIdTag

  type LedgerId = String @@ LedgerIdTag
  val LedgerId: Tag.TagOf[LedgerIdTag] = Tag.of[LedgerIdTag]

  def optionalLedgerId(raw: String): Option[LedgerId] =
    if (raw.isEmpty) None else Some(LedgerId(raw))

  sealed trait ParticipantIdTag

  type ParticipantId = Ref.ParticipantId @@ ParticipantIdTag
  val ParticipantId: Tag.TagOf[ParticipantIdTag] = Tag.of[ParticipantIdTag]

  sealed trait SubmissionIdTag

  type SubmissionId = Ref.SubmissionId @@ SubmissionIdTag
  val SubmissionId: Tag.TagOf[SubmissionIdTag] = Tag.of[SubmissionIdTag]

  final case class ObjectMeta(
      resourceVersionO: Option[Long],
      annotations: Map[String, String],
  )

  object ObjectMeta {
    def empty: ObjectMeta = ObjectMeta(
      resourceVersionO = None,
      annotations = Map.empty,
    )
  }

  final case class User(
      id: Ref.UserId,
      primaryParty: Option[Ref.Party],
      isDeactivated: Boolean = false,
      metadata: ObjectMeta = ObjectMeta.empty,
      identityProviderId: IdentityProviderId = IdentityProviderId.Default,
  )

  case class PartyDetails(
      party: Ref.Party,
      displayName: Option[String],
      isLocal: Boolean,
      metadata: ObjectMeta,
      identityProviderId: IdentityProviderId,
  )

  sealed abstract class UserRight extends Product with Serializable
  object UserRight {
    final case object ParticipantAdmin extends UserRight
    final case object IdentityProviderAdmin extends UserRight
    final case class CanActAs(party: Ref.Party) extends UserRight
    final case class CanReadAs(party: Ref.Party) extends UserRight
  }

  sealed abstract class Feature extends Product with Serializable
  object Feature {
    case object UserManagement extends Feature
  }

  final case class JwksUrl(value: String) extends AnyVal {
    def toURL = new URL(value)
  }
  object JwksUrl {
    def fromString(value: String): Either[String, JwksUrl] =
      Try(new URL(value)).toEither.left
        .map { case NonFatal(e) =>
          e.getMessage
        }
        .map(_ => JwksUrl(value))

    def assertFromString(str: String): JwksUrl = fromString(str) match {
      case Right(value) => value
      case Left(err) => throw new IllegalArgumentException(err)
    }
  }

  sealed trait IdentityProviderId {
    def toRequestString: String

    def toDb: Option[IdentityProviderId.Id]
  }

  object IdentityProviderId {
    final case object Default extends IdentityProviderId {
      override def toRequestString: String = ""
      override def toDb: Option[Id] = None
    }

    final case class Id(value: Ref.LedgerString) extends IdentityProviderId {
      override def toRequestString: String = value

      override def toDb: Option[Id] = Some(this)
    }

    object Id {
      def fromString(id: String): Either[String, IdentityProviderId.Id] = {
        Ref.LedgerString.fromString(id).map(Id.apply)
      }

      def assertFromString(id: String): Id = {
        Id(Ref.LedgerString.assertFromString(id))
      }
    }

    def apply(identityProviderId: String): IdentityProviderId =
      Some(identityProviderId).filter(_.nonEmpty) match {
        case Some(id) => Id(Ref.LedgerString.assertFromString(id))
        case None => Default
      }

    def fromString(identityProviderId: String): Either[String, IdentityProviderId] =
      Some(identityProviderId).filter(_.nonEmpty) match {
        case Some(id) => Ref.LedgerString.fromString(id).map(Id.apply)
        case None => Right(Default)
      }

    def fromDb(identityProviderId: Option[IdentityProviderId.Id]): IdentityProviderId =
      identityProviderId match {
        case None => IdentityProviderId.Default
        case Some(id) => id
      }
  }

  final case class IdentityProviderConfig(
      identityProviderId: IdentityProviderId.Id,
      isDeactivated: Boolean = false,
      jwksUrl: JwksUrl,
      issuer: String,
      audience: Option[String],
  )
}
