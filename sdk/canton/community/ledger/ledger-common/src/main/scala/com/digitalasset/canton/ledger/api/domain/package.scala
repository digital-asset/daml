// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value as Lf
import scalaz.{@@, Tag}

import java.net.URI
import scala.util.Try

package object domain {
  type Value = Lf

  type WorkflowId = Ref.WorkflowId @@ WorkflowIdTag
  val WorkflowId: Tag.TagOf[WorkflowIdTag] = Tag.of[WorkflowIdTag]

  type CommandId = Ref.CommandId @@ CommandIdTag
  val CommandId: Tag.TagOf[CommandIdTag] = Tag.of[CommandIdTag]

  type UpdateId = Ref.TransactionId @@ UpdateIdTag
  val UpdateId: Tag.TagOf[UpdateIdTag] = Tag.of[UpdateIdTag]

  type ParticipantId = Ref.ParticipantId @@ ParticipantIdTag
  val ParticipantId: Tag.TagOf[ParticipantIdTag] = Tag.of[ParticipantIdTag]

  type SubmissionId = Ref.SubmissionId @@ SubmissionIdTag
  val SubmissionId: Tag.TagOf[SubmissionIdTag] = Tag.of[SubmissionIdTag]
}

package domain {
  sealed trait WorkflowIdTag
  sealed trait CommandIdTag
  sealed trait UpdateIdTag
  sealed trait EventIdTag
  sealed trait ParticipantIdTag
  sealed trait SubmissionIdTag

  final case class JwksUrl(value: String) extends AnyVal {
    def toURL = new URI(value).toURL
  }

  object JwksUrl {
    def fromString(value: String): Either[String, JwksUrl] =
      Try(new URI(value).toURL).toEither.left
        .map(_.getMessage)
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
      def fromString(id: String): Either[String, IdentityProviderId.Id] =
        Ref.LedgerString.fromString(id).map(Id.apply)

      def assertFromString(id: String): Id =
        Id(Ref.LedgerString.assertFromString(id))
    }

    def apply(identityProviderId: String): IdentityProviderId =
      Some(identityProviderId).filter(_.nonEmpty) match {
        case Some(id) => Id.assertFromString(id)
        case None => Default
      }

    def fromString(identityProviderId: String): Either[String, IdentityProviderId] =
      Some(identityProviderId).filter(_.nonEmpty) match {
        case Some(id) => Id.fromString(id)
        case None => Right(Default)
      }

    def fromDb(identityProviderId: Option[IdentityProviderId.Id]): IdentityProviderId =
      identityProviderId match {
        case None => IdentityProviderId.Default
        case Some(id) => id
      }

    def fromOptionalLedgerString(identityProviderId: Option[Ref.LedgerString]): IdentityProviderId =
      identityProviderId match {
        case None => IdentityProviderId.Default
        case Some(id) => IdentityProviderId.Id(id)
      }
  }

  final case class IdentityProviderConfig(
      identityProviderId: IdentityProviderId.Id,
      isDeactivated: Boolean = false,
      jwksUrl: JwksUrl,
      issuer: String,
      audience: Option[String],
  )

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
  ) {
    // Note: this should be replaced by pretty printing once the ledger-api server packages move
    //  into their proper place
    override def toString: String =
      s"User(id=$id, primaryParty=$primaryParty, isDeactivated=$isDeactivated, metadata=${metadata.toString
          .take(512)}, identityProviderId=${identityProviderId.toRequestString})"
  }

  final case class PartyDetails(
      party: Ref.Party,
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

    final case object CanReadAsAnyParty extends UserRight
  }

  sealed abstract class Feature extends Product with Serializable

  object Feature {
    case object UserManagement extends Feature
  }
}
