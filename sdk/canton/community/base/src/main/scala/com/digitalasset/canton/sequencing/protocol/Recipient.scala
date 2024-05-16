// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.{
  InvariantViolation,
  StringConversionError,
  ValueConversionError,
}
import com.digitalasset.canton.config.CantonRequireTypes.{String3, String300}
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{Member, PartyId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

sealed trait Recipient extends Product with Serializable with PrettyPrinting {

  /** Determines if this recipient is authorized to send or receive through the sequencer.
    */
  def isAuthorized(snapshot: TopologySnapshot)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[Boolean]

  def toProtoPrimitive: String = toLengthLimitedString.unwrap

  def toLengthLimitedString: String300
}

object Recipient {
  def fromProtoPrimitive(
      recipient: String,
      fieldName: String,
  ): ParsingResult[Recipient] = {
    val dlen = UniqueIdentifier.delimiter.length
    val (typ, rest) = {
      val (code, str) = recipient.splitAt(3)
      (code, str.drop(dlen))
    }
    lazy val codeE = GroupRecipientCode.fromProtoPrimitive(typ, fieldName)

    if (codeE.isLeft)
      Member.fromProtoPrimitive(recipient, fieldName).map(MemberRecipient)
    else
      for {
        _ <- Either.cond(
          recipient.length >= 3 + dlen,
          (),
          ValueConversionError(
            fieldName,
            s"Invalid group recipient `$recipient`, expecting <three-letter-code>::id::info.",
          ),
        )
        _ <- Either.cond(
          recipient.substring(3, 3 + dlen) == UniqueIdentifier.delimiter,
          (),
          ValueConversionError(
            fieldName,
            s"Expected delimiter ${UniqueIdentifier.delimiter} after three letter code of `$recipient`",
          ),
        )
        code <- codeE
        groupRecipient <- code match {
          case ParticipantsOfParty.Code =>
            UniqueIdentifier
              .fromProtoPrimitive(rest, fieldName)
              .map(PartyId(_))
              .map(ParticipantsOfParty(_))
          case SequencersOfDomain.Code =>
            Right(SequencersOfDomain)
          case MediatorGroupRecipient.Code =>
            for {
              groupInt <-
                Either
                  .catchOnly[NumberFormatException](rest.toInt)
                  .leftMap(e =>
                    StringConversionError(
                      s"Cannot parse group number $rest, error ${e.getMessage}"
                    )
                  )
              group <- NonNegativeInt
                .create(groupInt)
                .leftMap(e => InvariantViolation(e.message))
            } yield MediatorGroupRecipient(group)
          case AllMembersOfDomain.Code =>
            Right(AllMembersOfDomain)
        }
      } yield groupRecipient
  }

}

sealed trait GroupRecipientCode {
  def threeLetterId: String3

  def toProtoPrimitive: String = threeLetterId.unwrap
}

object GroupRecipientCode {
  def fromProtoPrimitive_(code: String): Either[String, GroupRecipientCode] =
    String3.create(code).flatMap {
      case ParticipantsOfParty.Code.threeLetterId => Right(ParticipantsOfParty.Code)
      case SequencersOfDomain.Code.threeLetterId => Right(SequencersOfDomain.Code)
      case MediatorGroupRecipient.Code.threeLetterId => Right(MediatorGroupRecipient.Code)
      case AllMembersOfDomain.Code.threeLetterId => Right(AllMembersOfDomain.Code)
      case _ => Left(s"Unknown three letter type $code")
    }

  def fromProtoPrimitive(
      code: String,
      field: String,
  ): ParsingResult[GroupRecipientCode] =
    fromProtoPrimitive_(code).leftMap(ValueConversionError(field, _))
}

sealed trait GroupRecipient extends Recipient {
  def code: GroupRecipientCode
  def suffix: String

  def toLengthLimitedString: String300 =
    String300.tryCreate(
      s"${code.threeLetterId.unwrap}${UniqueIdentifier.delimiter}$suffix"
    )
}

object TopologyBroadcastAddress {
  val recipient: Recipient = AllMembersOfDomain
}

final case class MemberRecipient(member: Member) extends Recipient {

  override def isAuthorized(snapshot: TopologySnapshot)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[Boolean] = snapshot.isMemberKnown(member)

  override def pretty: Pretty[MemberRecipient] =
    prettyOfClass(
      unnamedParam(_.member)
    )

  override def toLengthLimitedString: String300 = member.toLengthLimitedString
}

final case class ParticipantsOfParty(party: PartyId) extends GroupRecipient {

  override def isAuthorized(snapshot: TopologySnapshot)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[Boolean] = snapshot.activeParticipantsOf(party.toLf).map(_.nonEmpty)

  override def pretty: Pretty[ParticipantsOfParty] =
    prettyOfClass(
      unnamedParam(_.party)
    )

  override def code: GroupRecipientCode = ParticipantsOfParty.Code

  override def suffix: String = party.toProtoPrimitive
}

object ParticipantsOfParty {
  object Code extends GroupRecipientCode {
    val threeLetterId: String3 = String3.tryCreate("POP")
  }
}

case object SequencersOfDomain extends GroupRecipient {

  override def isAuthorized(
      snapshot: TopologySnapshot
  )(implicit traceContext: TraceContext, executionContext: ExecutionContext): Future[Boolean] =
    Future.successful(true)

  override def pretty: Pretty[SequencersOfDomain.type] =
    prettyOfObject[SequencersOfDomain.type]

  override def code: GroupRecipientCode = SequencersOfDomain.Code

  override def suffix: String = ""

  object Code extends GroupRecipientCode {
    val threeLetterId: String3 = String3.tryCreate("SOD")
  }
}

final case class MediatorGroupRecipient(group: MediatorGroupIndex) extends GroupRecipient {

  override def isAuthorized(snapshot: TopologySnapshot)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[Boolean] = snapshot.isMediatorActive(this)

  override def pretty: Pretty[MediatorGroupRecipient] =
    prettyOfClass(
      param("group", _.group)
    )

  override def code: GroupRecipientCode = MediatorGroupRecipient.Code

  override def suffix: String = group.toString
}

object MediatorGroupRecipient {
  object Code extends GroupRecipientCode {
    val threeLetterId: String3 = String3.tryCreate("MGR")
  }

  def fromProtoPrimitive(
      mediatorGroupRecipientP: String,
      fieldName: String,
  ): ParsingResult[MediatorGroupRecipient] =
    Recipient.fromProtoPrimitive(mediatorGroupRecipientP, fieldName).flatMap {
      case mod: MediatorGroupRecipient => Right(mod)
      case other =>
        Left(ValueConversionError(fieldName, s"Expected MediatorGroupRecipient, got $other"))
    }
}

case object AllMembersOfDomain extends GroupRecipient {

  override def isAuthorized(snapshot: TopologySnapshot)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[Boolean] = Future.successful(true)

  override def pretty: Pretty[AllMembersOfDomain.type] =
    prettyOfString(_ => suffix)

  override def code: GroupRecipientCode = Code

  override def suffix: String = "All"
  object Code extends GroupRecipientCode {
    val threeLetterId: String3 = String3.tryCreate("ALL")
  }
}
