// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.kernel.Order
import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.{FieldNotSet, ValueConversionError}
import com.digitalasset.canton.config.CantonRequireTypes.{String255, String3, String300}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.admin.v30 as adminProtoV30
import com.digitalasset.canton.topology.admin.v30.Synchronizer.Kind
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LedgerParticipantId, LfPartyId, ProtoDeserializationError}
import com.google.common.annotations.VisibleForTesting
import io.circe.Encoder
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

/** Top level trait representing an identity within the system */
sealed trait Identity
    extends HasUniqueIdentifier
    with Product
    with Serializable
    with PrettyPrinting {
  def toProtoPrimitive: String = uid.toProtoPrimitive

  /** returns the string representation used in console filters (maps to the uid) */
  def filterString: String = uid.toProtoPrimitive

  override protected def pretty: Pretty[this.type] = prettyOfParam(_.uid)
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
      case ParticipantId.Code.threeLetterId => Right(ParticipantId.Code)
      case SequencerId.Code.threeLetterId => Right(SequencerId.Code)
      case _ => Left(s"Unknown three letter type $code")
    }

  def fromProtoPrimitive(
      code: String,
      field: String,
  ): ParsingResult[MemberCode] =
    fromProtoPrimitive_(code).leftMap(ValueConversionError(field, _))

}

/** A member in a synchronizer such as a participant and or synchronizer entities
  *
  * A member can be addressed and talked to on the transaction level through the sequencer.
  */
sealed trait Member extends Identity with Product with Serializable {

  def code: MemberCode

  def description: String

  override def toProtoPrimitive: String = toLengthLimitedString.unwrap

  def toLengthLimitedString: String300 =
    String300.tryCreate(
      s"${code.threeLetterId.unwrap}${UniqueIdentifier.delimiter}${uid.toProtoPrimitive}"
    )

  override protected def pretty: Pretty[Member] =
    prettyOfString(inst =>
      inst.code.threeLetterId.unwrap + UniqueIdentifier.delimiter + inst.uid.show
    )
}

object Member {

  def fromProtoPrimitive_(member: String): Either[String, Member] = {
    // The first three letters of the string identify the type of member
    val (typ, uidS) = member.splitAt(3)

    def mapToType(code: MemberCode, uid: UniqueIdentifier): Either[String, Member] =
      code match {
        case MediatorId.Code => Right(MediatorId(uid))
        case ParticipantId.Code => Right(ParticipantId(uid))
        case SequencerId.Code => Right(SequencerId(uid))
      }

    // expecting COD::<uid>
    val dlen = UniqueIdentifier.delimiter.length

    for {
      _ <- Either.cond(
        member.length > 3 + (2 * dlen),
        (),
        s"Invalid member `$member`, expecting <three-letter-code>::id::fingerprint.",
      )
      _ <- Either.cond(
        member.substring(3, 3 + dlen) == UniqueIdentifier.delimiter,
        (),
        s"Expected delimiter ${UniqueIdentifier.delimiter} after three letter code of `$member`",
      )
      code <- MemberCode.fromProtoPrimitive_(typ)
      uid <- UniqueIdentifier.fromProtoPrimitive_(uidS.substring(dlen)).leftMap(_.message)
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

  /** Instances for slick to set and get members. Not exposed by default as other types derived from
    * [[Member]] have their own persistence schemes ([[ParticipantId]]).
    */
  object DbStorageImplicits {
    implicit val setParameterMember: SetParameter[Member] = (v: Member, pp) =>
      pp >> v.toLengthLimitedString

    implicit val getResultMember: GetResult[Member] = GetResult { r =>
      Member
        .fromProtoPrimitive_(r.nextString())
        .valueOr(err => throw new DbDeserializationException(err))
    }
  }
}

sealed trait Synchronizer
    extends HasUniqueIdentifier
    with PrettyPrinting
    with Product
    with Serializable {

  def logical: SynchronizerId

  def isCompatibleWith(other: Synchronizer): Boolean = (this, other) match {
    case (a: PhysicalSynchronizerId, b: PhysicalSynchronizerId) => a == b
    case (a, b) => a.logical == b.logical
  }

  def toProtoV30: adminProtoV30.Synchronizer = {
    import adminProtoV30.Synchronizer.*

    this match {
      case lsid: SynchronizerId =>
        adminProtoV30.Synchronizer(Kind.Id(lsid.toProtoPrimitive))

      case psid: PhysicalSynchronizerId =>
        adminProtoV30.Synchronizer(Kind.PhysicalId(psid.toProtoPrimitive))
    }
  }
}

object Synchronizer {
  def fromProtoV30(proto: adminProtoV30.Synchronizer): ParsingResult[Synchronizer] =
    proto.kind match {
      case Kind.Empty => FieldNotSet("kind").asLeft
      case Kind.Id(lsidP) => SynchronizerId.fromProtoPrimitive(lsidP, "id")
      case Kind.PhysicalId(psidP) => PhysicalSynchronizerId.fromProtoPrimitive(psidP, "physical_id")
    }
}

final case class SynchronizerId(uid: UniqueIdentifier) extends Synchronizer with Identity {
  def unwrap: UniqueIdentifier = uid
  def toLengthLimitedString: String255 = uid.toLengthLimitedString

  override def logical: SynchronizerId = this
}

object SynchronizerId {
  implicit val orderSynchronizerId: Order[SynchronizerId] =
    Order.by[SynchronizerId, String](_.toProtoPrimitive)
  implicit val synchronizerIdEncoder: Encoder[SynchronizerId] =
    Encoder.encodeString.contramap(_.unwrap.toProtoPrimitive)

  // Instances for slick (db) queries
  implicit val getResultSynchronizerId: GetResult[SynchronizerId] =
    UniqueIdentifier.getResult.andThen(SynchronizerId(_))

  implicit val getResultSynchronizerIdO: GetResult[Option[SynchronizerId]] =
    UniqueIdentifier.getResultO.andThen(_.map(SynchronizerId(_)))

  implicit val setParameterSynchronizerId: SetParameter[SynchronizerId] =
    (d: SynchronizerId, pp: PositionedParameters) => pp >> d.toLengthLimitedString
  implicit val setParameterSynchronizerIdO: SetParameter[Option[SynchronizerId]] =
    (d: Option[SynchronizerId], pp: PositionedParameters) => pp >> d.map(_.toLengthLimitedString)

  def fromProtoPrimitive(
      proto: String,
      fieldName: String,
  ): ParsingResult[SynchronizerId] =
    UniqueIdentifier.fromProtoPrimitive(proto, fieldName).map(SynchronizerId(_))

  def tryFromString(str: String): SynchronizerId = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive(str)
  )

  def fromString(str: String): Either[String, SynchronizerId] =
    UniqueIdentifier.fromProtoPrimitive_(str).map(SynchronizerId(_)).leftMap(_.message)
}

final case class PhysicalSynchronizerId(
    logical: SynchronizerId,
    protocolVersion: ProtocolVersion,
    serial: NonNegativeInt,
) extends Synchronizer {
  def suffix: String = s"$protocolVersion${PhysicalSynchronizerId.secondaryDelimiter}$serial"

  def uid: UniqueIdentifier = logical.uid

  def toLengthLimitedString: String300 =
    String300.tryCreate(
      s"${logical.toLengthLimitedString}${PhysicalSynchronizerId.primaryDelimiter}$suffix"
    )

  def toProtoPrimitive: String = toLengthLimitedString.unwrap

  override protected def pretty: Pretty[PhysicalSynchronizerId.this.type] =
    prettyOfString(_ => toLengthLimitedString.unwrap)
}

object PhysicalSynchronizerId {
  private val primaryDelimiter: String = "::" // Between LSId and suffix
  private val secondaryDelimiter: String = "-" // Between components of the suffix

  def apply(
      synchronizerId: SynchronizerId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
  ): PhysicalSynchronizerId =
    PhysicalSynchronizerId(
      synchronizerId,
      staticSynchronizerParameters.protocolVersion,
      staticSynchronizerParameters.serial,
    )

  implicit val physicalSynchronizerIdOrdering: Ordering[PhysicalSynchronizerId] =
    Ordering.by(psid =>
      (psid.logical.toLengthLimitedString.unwrap, psid.protocolVersion, psid.serial)
    )

  def fromString(raw: String): Either[String, PhysicalSynchronizerId] = {
    val elements = raw.split(primaryDelimiter)
    val elementsCount = elements.sizeIs

    if (elementsCount == 3) {
      for {
        lsid <- SynchronizerId.fromString(elements.take(2).mkString(primaryDelimiter))
        suffix = elements(2)
        suffixComponents = suffix.split("-")
        _ <- Either.cond(
          suffixComponents.sizeIs == 2,
          (),
          s"Cannot parse $suffix as a physical synchronizer id suffix",
        )
        pv <- ProtocolVersion.create(suffixComponents(0))
        serialInt <- suffixComponents(1).toIntOption.toRight(
          s"Cannot parse ${suffixComponents(1)} to an int"
        )
        serial <- NonNegativeInt.create(serialInt).leftMap(_.message)
      } yield PhysicalSynchronizerId(lsid, pv, serial)
    } else
      Left(s"Unable to parse `$raw` as physical synchronizer id")
  }

  def fromProtoPrimitive(proto: String, field: String): ParsingResult[PhysicalSynchronizerId] =
    fromString(proto).leftMap(ValueConversionError(field, _))

  def tryFromString(raw: String): PhysicalSynchronizerId =
    fromString(raw).valueOr(err => throw new IllegalArgumentException(err))

  implicit val getResultSynchronizerId: GetResult[PhysicalSynchronizerId] = GetResult { r =>
    tryFromString(r.nextString())
  }

  implicit val getResultSynchronizerIdO: GetResult[Option[PhysicalSynchronizerId]] =
    GetResult { r =>
      r.nextStringOption().map(tryFromString)
    }

  implicit val setParameterSynchronizerId: SetParameter[PhysicalSynchronizerId] =
    (d: PhysicalSynchronizerId, pp: PositionedParameters) => pp >> d.toLengthLimitedString.unwrap
  implicit val setParameterSynchronizerIdO: SetParameter[Option[PhysicalSynchronizerId]] =
    (d: Option[PhysicalSynchronizerId], pp: PositionedParameters) =>
      pp >> d.map(_.toLengthLimitedString.unwrap)
}

/** A participant identifier */
final case class ParticipantId(uid: UniqueIdentifier) extends Member with NodeIdentity {

  override def code: MemberCode = ParticipantId.Code

  override val description: String = "participant"

  def adminParty: PartyId = PartyId(uid)
  def toLf: LedgerParticipantId = LedgerParticipantId.assertFromString(uid.toProtoPrimitive)

  override def member: Member = this
}

object ParticipantId {
  object Code extends MemberCode {
    val threeLetterId: String3 = String3.tryCreate("PAR")
  }
  def apply(identifier: String, namespace: Namespace): ParticipantId =
    ParticipantId(UniqueIdentifier.tryCreate(identifier, namespace))

  /** create a participant from a string
    *
    * used in testing
    */
  @VisibleForTesting
  def apply(addr: String): ParticipantId =
    ParticipantId(UniqueIdentifier.tryCreate(addr, "default"))

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
}

object PartyId {

  implicit val ordering: Ordering[PartyId] = Ordering.by(x => x.toProtoPrimitive)
  implicit val getResultPartyId: GetResult[PartyId] =
    UniqueIdentifier.getResult.andThen(PartyId(_))
  implicit val setParameterPartyId: SetParameter[PartyId] =
    (p: PartyId, pp: PositionedParameters) => pp >> p.uid.toLengthLimitedString

  def tryCreate(identifier: String, namespace: Namespace): PartyId =
    PartyId(UniqueIdentifier.tryCreate(identifier, namespace))
  def tryCreate(identifier: String, fingerprint: Fingerprint): PartyId =
    PartyId(UniqueIdentifier.tryCreate(identifier, fingerprint))

  def fromLfParty(lfParty: LfPartyId): Either[String, PartyId] =
    UniqueIdentifier.fromProtoPrimitive_(lfParty).map(PartyId(_)).leftMap(_.message)

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

/** Represents a mediator group, containing only mediators that have at least 1 signing key.
  * @param index
  *   uniquely identifies the group, just like [[MediatorId]] for single mediators.
  * @param active
  *   the active mediators belonging to the group
  * @param passive
  *   the passive mediators belonging to the group
  * @param threshold
  *   the minimum size of a quorum
  */
final case class MediatorGroup(
    index: MediatorGroupIndex,
    active: Seq[MediatorId],
    passive: Seq[MediatorId],
    threshold: PositiveInt,
) {
  def isActive: Boolean = active.sizeIs >= threshold.value

  def all: Seq[MediatorId] = active ++ passive
}

object MediatorGroup {
  type MediatorGroupIndex = NonNegativeInt
  val MediatorGroupIndex: NonNegativeInt.type = NonNegativeInt
}

final case class MediatorId(uid: UniqueIdentifier) extends Member with NodeIdentity {
  override def code: MemberCode = MediatorId.Code
  override val description: String = "mediator"
  override def member: Member = this
}

object MediatorId {
  object Code extends MemberCode {
    val threeLetterId = String3.tryCreate("MED")
  }

  def tryCreate(identifier: String, namespace: Namespace): MediatorId =
    MediatorId(UniqueIdentifier.tryCreate(identifier, namespace))

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

/** Contains only sequencers from SequencerSynchronizerState that also have at least 1 signing key.
  *
  * When reading `threshold`, recall the contract of `SequencerSynchronizerState`: The system must
  * tolerate up to `min(threshold - 1, (active.size - 1)/3)` malicious active sequencers.
  */
final case class SequencerGroup(
    active: Seq[SequencerId],
    passive: Seq[SequencerId],
    threshold: PositiveInt,
)

final case class SequencerId(uid: UniqueIdentifier) extends Member with NodeIdentity {
  override def code: MemberCode = SequencerId.Code
  override val description: String = "sequencer"
  override def member: Member = this
}

object SequencerId {

  object Code extends MemberCode {
    val threeLetterId = String3.tryCreate("SEQ")
  }

  implicit val sequencerIdOrdering: Ordering[SequencerId] =
    Ordering.by(_.toProtoPrimitive)

  def tryCreate(identifier: String, namespace: Namespace): SequencerId =
    SequencerId(UniqueIdentifier.tryCreate(identifier, namespace))

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
