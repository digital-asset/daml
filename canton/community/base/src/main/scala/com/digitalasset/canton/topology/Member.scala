// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.kernel.Order
import cats.syntax.either.*
import com.daml.ledger.javaapi.data.Party
import com.digitalasset.canton.ProtoDeserializationError.ValueConversionError
import com.digitalasset.canton.config.CantonRequireTypes.{String255, String3, String300}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{Fingerprint, RandomOps}
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
  def keyOwner: KeyOwner
  def member: Member
}

sealed trait KeyOwnerCode {

  def threeLetterId: String3

  def toProtoPrimitive: String = threeLetterId.unwrap

}

object KeyOwnerCode {

  def fromProtoPrimitive_(code: String): Either[String, KeyOwnerCode] =
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
  ): ParsingResult[KeyOwnerCode] =
    fromProtoPrimitive_(code).leftMap(ValueConversionError(field, _))

}

/** An identity within the system that owns a key */
@Deprecated(
  since =
    "2.6, use Member instead (as Member == KeyOwner) and we stopped to actually use KeyOwner separately"
)
sealed trait KeyOwner extends Identity {

  def code: KeyOwnerCode

  override def toProtoPrimitive: String = toLengthLimitedString.unwrap
  def toLengthLimitedString: String300 =
    String300.tryCreate(
      s"${code.threeLetterId.unwrap}${SafeSimpleString.delimiter}${uid.toProtoPrimitive}"
    )

  override def pretty: Pretty[KeyOwner] =
    prettyOfString(inst =>
      inst.code.threeLetterId.unwrap + SafeSimpleString.delimiter + inst.uid.show
    )
}

object KeyOwner {

  def fromProtoPrimitive_(keyOwner: String): Either[String, KeyOwner] = {
    // The first three letters of the string identify the type of member
    val (typ, uidS) = keyOwner.splitAt(3)

    def mapToType(code: KeyOwnerCode, uid: UniqueIdentifier): Either[String, KeyOwner] = {
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
        keyOwner.length > 3 + (2 * dlen),
        (),
        s"Invalid keyOwner `$keyOwner`, expecting <three-letter-code>::id::fingerprint.",
      )
      _ <- Either.cond(
        keyOwner.substring(3, 3 + dlen) == SafeSimpleString.delimiter,
        (),
        s"Expected delimiter ${SafeSimpleString.delimiter} after three letter code of `$keyOwner`",
      )
      code <- KeyOwnerCode.fromProtoPrimitive_(typ)
      uid <- UniqueIdentifier.fromProtoPrimitive_(uidS.substring(dlen))
      keyOwner <- mapToType(code, uid)
    } yield keyOwner
  }

  def fromProtoPrimitive(
      keyOwner: String,
      fieldName: String,
  ): ParsingResult[KeyOwner] =
    fromProtoPrimitive_(keyOwner).leftMap(ValueConversionError(fieldName, _))

}

/** A member in a domain such as a participant and or domain entities
  *
  * A member can be addressed and talked to on the transaction level
  * through the sequencer. Therefore every member is a KeyOwner. And the
  * sequencer is not a member, as he is one level below, dealing with
  * messages.
  */
// TODO(#15231) The sequencer is now also a member, so Member and KeyOwner are actually the same.
sealed trait Member extends KeyOwner with Product with Serializable {
  def isAuthenticated: Boolean
}

object Member {

  def fromProtoPrimitive(
      member: String,
      fieldName: String,
  ): ParsingResult[Member] =
    KeyOwner.fromProtoPrimitive(member, fieldName).flatMap {
      case x: Member => Right(x)
      case _ =>
        Left(
          ProtoDeserializationError
            .ValueDeserializationError(fieldName, s"Value `$member` is not of type Member")
        )
    }

  // Use the same ordering as for what we use in the database
  implicit val memberOrdering: Ordering[Member] = Ordering.by(_.toLengthLimitedString.unwrap)

  /** Instances for slick to set and get members.
    * Not exposed by default as other types derived from [[Member]] have their own persistence schemes ([[ParticipantId]]).
    */
  object DbStorageImplicits {
    implicit val setParameterMember: SetParameter[Member] = (v: Member, pp) =>
      pp >> v.toLengthLimitedString

    implicit val getResultMember: GetResult[Member] = GetResult(r => {
      KeyOwner
        .fromProtoPrimitive_(r.nextString())
        .fold(
          err => throw new DbDeserializationException(err),
          {
            case member: Member => member
            case _ => throw new DbDeserializationException("Unknown type of member")
          },
        )
    })
  }
}

sealed trait AuthenticatedMember extends Member {
  override def code: AuthenticatedMemberCode
  override def isAuthenticated: Boolean = true
}

sealed trait AuthenticatedMemberCode extends KeyOwnerCode

final case class UnauthenticatedMemberId(uid: UniqueIdentifier) extends Member {
  override def code: KeyOwnerCode = UnauthenticatedMemberId.Code
  override def isAuthenticated: Boolean = false
}

object UnauthenticatedMemberId {
  object Code extends KeyOwnerCode {
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

  // The key owner of a domain identity is the domain topology manager
  override def keyOwner: KeyOwner = DomainTopologyManagerId(uid)

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

  def adminParty: PartyId = PartyId(uid)
  def toLf: LedgerParticipantId = LedgerParticipantId.assertFromString(uid.toProtoPrimitive)

  override def keyOwner: KeyOwner = this

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
    KeyOwner.fromProtoPrimitive(proto, fieldName).flatMap {
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

  def apply(identifier: Identifier, namespace: Fingerprint): PartyId =
    PartyId(UniqueIdentifier(identifier, Namespace(namespace)))

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
}

final case class MediatorId(uid: UniqueIdentifier) extends DomainMember with NodeIdentity {
  override def code: AuthenticatedMemberCode = MediatorId.Code

  override def keyOwner: KeyOwner = this

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
    active: Seq[SequencerId],
    passive: Seq[SequencerId],
    threshold: PositiveInt,
)

final case class SequencerId(uid: UniqueIdentifier) extends DomainMember with NodeIdentity {
  override def code: AuthenticatedMemberCode = SequencerId.Code

  override def keyOwner: KeyOwner = this

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
    KeyOwner.fromProtoPrimitive(proto, fieldName).flatMap {
      case x: SequencerId => Right(x)
      case y =>
        Left(
          ProtoDeserializationError
            .ValueDeserializationError(fieldName, s"Value $y is not of type `SequencerId`")
        )
    }
}
