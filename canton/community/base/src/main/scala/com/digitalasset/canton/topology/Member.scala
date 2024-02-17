// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.kernel.Order
import cats.syntax.either.*
import com.daml.ledger.javaapi.data.Party
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.ValueConversionError
import com.digitalasset.canton.config.CantonRequireTypes.{String255, String3, String300}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.RandomOps
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.util.HexString
import com.digitalasset.canton.{LedgerParticipantId, LfPartyId, ProtoDeserializationError}
import com.google.common.annotations.VisibleForTesting
import io.circe.Encoder
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

/** Top level trait representing an identity within the system */
sealed trait Identity extends Product with Serializable with PrettyPrinting {
  def uid: UniqueIdentifier

  def toProtoPrimitive: String = uid.toProtoPrimitive

  /** returns the string representation used in console filters (maps to the uid) */
  def filterString: String = uid.toProtoPrimitive

  override def pretty: Pretty[this.type] = prettyOfParam(_.uid)
}

sealed trait NodeIdentity extends Identity {
  def member: Member
}

sealed trait MemberCode {

  def threeLetterId: String3

  def toProtoPrimitive: String = threeLetterId.unwrap

}

object MemberCode {

  def fromProtoPrimitive_(code: String): Either[String, MemberCode] =
    String3.create(code).flatMap {
      case MediatorId.Code.threeLetterId => Right(MediatorId.Code)
      case DomainTopologyManagerId.Code.threeLetterId => Right(DomainTopologyManagerId.Code)
      case ParticipantId.Code.threeLetterId => Right(ParticipantId.Code)
      case SequencerId.Code.threeLetterId => Right(SequencerId.Code)
      case UnauthenticatedMemberId.Code.threeLetterId => Right(UnauthenticatedMemberId.Code)
      case _ => Left(s"Unknown three letter type $code")
    }

  def fromProtoPrimitive(
      code: String,
      field: String,
  ): ParsingResult[MemberCode] =
    fromProtoPrimitive_(code).leftMap(ValueConversionError(field, _))

}

/** A member in a domain such as a participant and or domain entities
  *
  * A member can be addressed and talked to on the transaction level
  * through the sequencer.
  */
sealed trait Member extends Identity with Product with Serializable {

  def code: MemberCode

  def description: String

  def isAuthenticated: Boolean

  override def toProtoPrimitive: String = toLengthLimitedString.unwrap

  def toLengthLimitedString: String300 =
    String300.tryCreate(
      s"${code.threeLetterId.unwrap}${SafeSimpleString.delimiter}${uid.toProtoPrimitive}"
    )

  override def pretty: Pretty[Member] =
    prettyOfString(inst =>
      inst.code.threeLetterId.unwrap + SafeSimpleString.delimiter + inst.uid.show
    )
}

object Member {

  def fromProtoPrimitive_(member: String): Either[String, Member] = {
    // The first three letters of the string identify the type of member
    val (typ, uidS) = member.splitAt(3)

    def mapToType(code: MemberCode, uid: UniqueIdentifier): Either[String, Member] = {
      code match {
        case MediatorId.Code => Right(MediatorId(uid))
        case DomainTopologyManagerId.Code => Right(DomainTopologyManagerId(uid))
        case ParticipantId.Code => Right(ParticipantId(uid))
        case SequencerId.Code => Right(SequencerId(uid))
        case UnauthenticatedMemberId.Code => Right(UnauthenticatedMemberId(uid))
      }
    }

    // expecting COD::<uid>
    val dlen = SafeSimpleString.delimiter.length

    for {
      _ <- Either.cond(
        member.length > 3 + (2 * dlen),
        (),
        s"Invalid member `$member`, expecting <three-letter-code>::id::fingerprint.",
      )
      _ <- Either.cond(
        member.substring(3, 3 + dlen) == SafeSimpleString.delimiter,
        (),
        s"Expected delimiter ${SafeSimpleString.delimiter} after three letter code of `$member`",
      )
      code <- MemberCode.fromProtoPrimitive_(typ)
      uid <- UniqueIdentifier.fromProtoPrimitive_(uidS.substring(dlen))
      member <- mapToType(code, uid)
    } yield member
  }

  def fromProtoPrimitive(
      member: String,
      fieldName: String,
  ): ParsingResult[Member] =
    fromProtoPrimitive_(member).leftMap(ValueConversionError(fieldName, _))

  // Use the same ordering as for what we use in the database
  implicit val memberOrdering: Ordering[Member] = Ordering.by(_.toLengthLimitedString.unwrap)

  /** Instances for slick to set and get members.
    * Not exposed by default as other types derived from [[Member]] have their own persistence schemes ([[ParticipantId]]).
    */
  object DbStorageImplicits {
    implicit val setParameterMember: SetParameter[Member] = (v: Member, pp) =>
      pp >> v.toLengthLimitedString

    implicit val getResultMember: GetResult[Member] = GetResult(r => {
      Member
        .fromProtoPrimitive_(r.nextString())
        .valueOr(err => throw new DbDeserializationException(err))
    })
  }

}

sealed trait AuthenticatedMember extends Member {
  override def code: AuthenticatedMemberCode
  override def isAuthenticated: Boolean = true
}

sealed trait AuthenticatedMemberCode extends MemberCode

final case class UnauthenticatedMemberId(uid: UniqueIdentifier) extends Member {
  override def code: MemberCode = UnauthenticatedMemberId.Code
  override val description: String = "unauthenticated member"
  override def isAuthenticated: Boolean = false
}

object UnauthenticatedMemberId {
  object Code extends MemberCode {
    val threeLetterId: String3 = String3.tryCreate("UNM")
  }

  private val RandomIdentifierNumberOfBytes = 20

  def tryCreate(namespace: Namespace)(randomOps: RandomOps): UnauthenticatedMemberId =
    UnauthenticatedMemberId(
      UniqueIdentifier.tryCreate(
        HexString.toHexString(randomOps.generateRandomByteString(RandomIdentifierNumberOfBytes)),
        namespace.fingerprint.unwrap,
      )
    )
}

final case class DomainId(uid: UniqueIdentifier) extends NodeIdentity {
  def unwrap: UniqueIdentifier = uid
  def toLengthLimitedString: String255 = uid.toLengthLimitedString

  // The member and member of a domain identity is the domain topology manager
  override def member: Member = DomainTopologyManagerId(uid)
}

object DomainId {

  implicit val orderDomainId: Order[DomainId] = Order.by[DomainId, String](_.toProtoPrimitive)
  implicit val domainIdEncoder: Encoder[DomainId] =
    Encoder.encodeString.contramap(_.unwrap.toProtoPrimitive)

  // Instances for slick (db) queries
  implicit val getResultDomainId: GetResult[DomainId] =
    UniqueIdentifier.getResult.andThen(DomainId(_))

  implicit val getResultDomainIdO: GetResult[Option[DomainId]] =
    UniqueIdentifier.getResultO.andThen(_.map(DomainId(_)))

  implicit val setParameterDomainId: SetParameter[DomainId] =
    (d: DomainId, pp: PositionedParameters) => pp >> d.toLengthLimitedString
  implicit val setParameterDomainIdO: SetParameter[Option[DomainId]] =
    (d: Option[DomainId], pp: PositionedParameters) => pp >> d.map(_.toLengthLimitedString)

  def fromProtoPrimitive(
      proto: String,
      fieldName: String,
  ): ParsingResult[DomainId] =
    UniqueIdentifier.fromProtoPrimitive(proto, fieldName).map(DomainId(_))

  def tryFromString(str: String): DomainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive(str))

  def fromString(str: String): Either[String, DomainId] =
    UniqueIdentifier.fromProtoPrimitive_(str).map(DomainId(_))

}

/** A participant identifier */
final case class ParticipantId(uid: UniqueIdentifier)
    extends AuthenticatedMember
    with NodeIdentity {

  override def code: AuthenticatedMemberCode = ParticipantId.Code

  override val description: String = "participant"

  def adminParty: PartyId = PartyId(uid)
  def toLf: LedgerParticipantId = LedgerParticipantId.assertFromString(uid.toProtoPrimitive)

  override def member: Member = this
}

object ParticipantId {
  object Code extends AuthenticatedMemberCode {
    val threeLetterId: String3 = String3.tryCreate("PAR")
  }
  def apply(identifier: Identifier, namespace: Namespace): ParticipantId =
    ParticipantId(UniqueIdentifier(identifier, namespace))

  /** create a participant from a string
    *
    * used in testing
    */
  @VisibleForTesting
  def apply(addr: String): ParticipantId = {
    ParticipantId(UniqueIdentifier.tryCreate(addr, "default"))
  }

  implicit val ordering: Ordering[ParticipantId] = Ordering.by(_.uid.toProtoPrimitive)

  def fromProtoPrimitive(
      proto: String,
      fieldName: String,
  ): ParsingResult[ParticipantId] =
    Member.fromProtoPrimitive(proto, fieldName).flatMap {
      case x: ParticipantId => Right(x)
      case y =>
        Left(
          ProtoDeserializationError
            .ValueDeserializationError(fieldName, s"Value $y is not of type `ParticipantId`")
        )
    }

  def fromLfParticipant(lfParticipant: LedgerParticipantId): Either[String, ParticipantId] =
    UniqueIdentifier.fromProtoPrimitive_(lfParticipant).map(ParticipantId(_))

  def tryFromLfParticipant(lfParticipant: LedgerParticipantId): ParticipantId =
    fromLfParticipant(lfParticipant).fold(
      e => throw new IllegalArgumentException(e),
      Predef.identity,
    )

  def tryFromProtoPrimitive(str: String): ParticipantId =
    fromProtoPrimitive(str, "").fold(
      err => throw new IllegalArgumentException(err.message),
      identity,
    )

  // Instances for slick (db) queries
  implicit val getResultParticipantId: GetResult[ParticipantId] =
    UniqueIdentifier.getResult.andThen(ParticipantId(_))
  implicit val setParameterParticipantId: SetParameter[ParticipantId] =
    (p: ParticipantId, pp: PositionedParameters) => pp >> p.uid.toLengthLimitedString
}

/** A party identifier based on a unique identifier
  */
final case class PartyId(uid: UniqueIdentifier) extends Identity {

  def toLf: LfPartyId = LfPartyId.assertFromString(uid.toProtoPrimitive)

  def toParty: Party = new Party(toLf)
}

object PartyId {

  implicit val ordering: Ordering[PartyId] = Ordering.by(x => x.toProtoPrimitive)
  implicit val getResultPartyId: GetResult[PartyId] =
    UniqueIdentifier.getResult.andThen(PartyId(_))
  implicit val setParameterPartyId: SetParameter[PartyId] =
    (p: PartyId, pp: PositionedParameters) => pp >> p.uid.toLengthLimitedString

  def apply(identifier: Identifier, namespace: Namespace): PartyId =
    PartyId(UniqueIdentifier(identifier, namespace))

  def fromLfParty(lfParty: LfPartyId): Either[String, PartyId] =
    UniqueIdentifier.fromProtoPrimitive_(lfParty).map(PartyId(_))

  def tryFromLfParty(lfParty: LfPartyId): PartyId =
    fromLfParty(lfParty) match {
      case Right(partyId) => partyId
      case Left(e) => throw new IllegalArgumentException(e)
    }

  def fromProtoPrimitive(str: String, fieldName: String): ParsingResult[PartyId] = (for {
    lfPartyId <- LfPartyId.fromString(str)
    partyId <- fromLfParty(lfPartyId)
  } yield partyId).leftMap(ValueConversionError(fieldName, _))

  def tryFromProtoPrimitive(str: String): PartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive(str)
  )

}

sealed trait DomainMember extends AuthenticatedMember

object DomainMember {

  /** List domain members for the given id, optionally including the sequencer. * */
  def list(id: DomainId, includeSequencer: Boolean): Set[DomainMember] = {
    // TODO(i7992) remove static mediator id
    val baseMembers = Set[DomainMember](DomainTopologyManagerId(id), MediatorId(id))
    if (includeSequencer) baseMembers + SequencerId(id)
    else baseMembers
  }

  /** List all domain members always including the sequencer. */
  def listAll(id: DomainId): Set[DomainMember] = list(id, includeSequencer = true)

}

/** @param index uniquely identifies the group, just like [[MediatorId]] for single mediators.
  * @param active the active mediators belonging to the group
  * @param passive the passive mediators belonging to the group
  * @param threshold the minimum size of a quorum
  */
final case class MediatorGroup(
    index: MediatorGroupIndex,
    active: Seq[MediatorId],
    passive: Seq[MediatorId],
    threshold: PositiveInt,
) {
  def isActive: Boolean = active.size >= threshold.value

  def all: Seq[MediatorId] = active ++ passive
}

object MediatorGroup {
  type MediatorGroupIndex = NonNegativeInt
  val MediatorGroupIndex = NonNegativeInt
}

final case class MediatorId(uid: UniqueIdentifier) extends DomainMember with NodeIdentity {
  override def code: AuthenticatedMemberCode = MediatorId.Code
  override val description: String = "mediator"
  override def member: Member = this
}

object MediatorId {
  object Code extends AuthenticatedMemberCode {
    val threeLetterId = String3.tryCreate("MED")
  }

  def apply(identifier: Identifier, namespace: Namespace): MediatorId =
    MediatorId(UniqueIdentifier(identifier, namespace))

  def apply(domainId: DomainId): MediatorId = MediatorId(domainId.unwrap)

  def fromProtoPrimitive(
      mediatorId: String,
      fieldName: String,
  ): ParsingResult[MediatorId] = Member.fromProtoPrimitive(mediatorId, fieldName).flatMap {
    case medId: MediatorId => Right(medId)
    case _ =>
      Left(
        ProtoDeserializationError
          .ValueDeserializationError(fieldName, s"Value `$mediatorId` is not of type MediatorId")
      )
  }

}

/** The domain topology manager id
  *
  * The domain manager is the topology manager of the domain. The read side
  * of the domain manager is the IdentityProvidingService.
  */
final case class DomainTopologyManagerId(uid: UniqueIdentifier) extends DomainMember {
  override def code: AuthenticatedMemberCode = DomainTopologyManagerId.Code
  override val description: String = "domain topology manager"
  lazy val domainId: DomainId = DomainId(uid)
}

object DomainTopologyManagerId {

  object Code extends AuthenticatedMemberCode {
    val threeLetterId = String3.tryCreate("DOM")
  }

  def apply(identifier: Identifier, namespace: Namespace): DomainTopologyManagerId =
    DomainTopologyManagerId(UniqueIdentifier(identifier, namespace))

  def apply(domainId: DomainId): DomainTopologyManagerId = DomainTopologyManagerId(domainId.unwrap)
}

final case class SequencerGroup(
    active: NonEmpty[Seq[SequencerId]],
    passive: Seq[SequencerId],
    threshold: PositiveInt,
)

final case class SequencerId(uid: UniqueIdentifier) extends DomainMember with NodeIdentity {
  override def code: AuthenticatedMemberCode = SequencerId.Code
  override val description: String = "sequencer"
  override def member: Member = this
}

object SequencerId {

  object Code extends AuthenticatedMemberCode {
    val threeLetterId = String3.tryCreate("SEQ")
  }

  def apply(identifier: Identifier, namespace: Namespace): SequencerId =
    SequencerId(UniqueIdentifier(identifier, namespace))

  def apply(domainId: DomainId): SequencerId = SequencerId(domainId.unwrap)

  def fromProtoPrimitive(
      proto: String,
      fieldName: String,
  ): ParsingResult[SequencerId] =
    Member.fromProtoPrimitive(proto, fieldName).flatMap {
      case x: SequencerId => Right(x)
      case y =>
        Left(
          ProtoDeserializationError
            .ValueDeserializationError(fieldName, s"Value $y is not of type `SequencerId`")
        )
    }
}
