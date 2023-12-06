// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.implicits.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.v4.EnvelopeContent
import com.digitalasset.canton.protocol.{SourceDomainId, TargetDomainId, TransferId, v0}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.version.{
  HasProtocolVersionedCompanion,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}

/** Causality messages are sent along with a transfer-in response. They propagate causality information on
  * the events a participant has "seen" for a party at the time of the transfer-out.
  * TODO(i9514): Encrypt the causality messages
  *
  * @param targetDomain The domain ID that the causality message is addressed to
  * @param transferId The ID of the transfer for which we are propagating causality information
  * @param clock The vector clock specifying causality information at the time of the transfer out
  */
final case class CausalityMessage private (
    targetDomain: TargetDomainId,
    transferId: TransferId,
    clock: VectorClock,
)(override val representativeProtocolVersion: RepresentativeProtocolVersion[CausalityMessage.type])
    extends UnsignedProtocolMessage
    with PrettyPrinting {

  val domainId = targetDomain.unwrap

  def toProtoV0: v0.CausalityMessage = v0.CausalityMessage(
    targetDomainId = domainId.toProtoPrimitive,
    transferId = Some(transferId.toProtoV0),
    clock = Some(clock.toProtoV0),
  )

  override protected[messages] def toProtoSomeEnvelopeContentV4
      : EnvelopeContent.SomeEnvelopeContent = throw new RuntimeException(
    "This should not be called"
  )

  override def pretty: Pretty[CausalityMessage.this.type] =
    prettyOfClass(
      param("Message domain ", _.domainId),
      param("Transfer ID ", _.transferId),
      param("Vector clock", _.clock),
    )

  @transient override protected lazy val companionObj: CausalityMessage.type = CausalityMessage
}

object CausalityMessage extends HasProtocolVersionedCompanion[CausalityMessage] {

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v30)(v0.CausalityMessage)(
      supportedProtoVersion(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  implicit val causalityMessageCast: ProtocolMessageContentCast[CausalityMessage] =
    ProtocolMessageContentCast.create[CausalityMessage]("CausalityMessage") {
      case cm: CausalityMessage => Some(cm)
      case _ => None
    }

  private[messages] def fromProtoV0(cmP: v0.CausalityMessage): ParsingResult[CausalityMessage] = {
    val v0.CausalityMessage(domainIdP, transferIdP, clockPO) = cmP
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "target_domain_id").map(TargetDomainId(_))
      clocks <- ProtoConverter.parseRequired(VectorClock.fromProtoV0, "clock", clockPO)
      tid <- ProtoConverter.parseRequired(TransferId.fromProtoV0, "transfer_id", transferIdP)
    } yield CausalityMessage(
      domainId,
      tid,
      clocks,
    )(protocolVersionRepresentativeFor(ProtoVersion(0)))
  }

  override def name: String = "CausalityMessage"
}

/** A vector clock represents the causal constraints that must be respected for a party at a certain point in time.
  * Vector clocks are maintained per-domain
  *
  * @param sourceDomainId The domain of the vector clock
  * @param localTs The timestamp on `sourceDomainId` specifying the time at which the causal constraints are valid
  * @param partyId The party who has seen the causal information specified by `clock`
  * @param clock The most recent timestamp on each domain that `partyId` has causally observed
  */
final case class VectorClock(
    sourceDomainId: SourceDomainId,
    localTs: CantonTimestamp,
    partyId: LfPartyId,
    clock: Map[DomainId, CantonTimestamp],
) extends PrettyPrinting {

  override def pretty: Pretty[VectorClock.this.type] =
    prettyOfClass(
      param("Domain for constraints ", _.sourceDomainId),
      param("Most recent timestamps", _.clock),
      param("Local timestamp", _.localTs),
      param("Party", _.partyId),
    )

  private[messages] def toProtoV0: v0.VectorClock = {
    v0.VectorClock(
      originDomainId = sourceDomainId.toProtoPrimitive,
      localTs = Some(localTs.toProtoPrimitive),
      partyId = partyId,
      clock = clock.map { case (did, cts) => did.toProtoPrimitive -> cts.toProtoPrimitive },
    )
  }
}

object VectorClock {
  private[messages] def fromProtoV0(vc: v0.VectorClock): ParsingResult[VectorClock] = {
    val v0.VectorClock(did, ts, partyId, clock) = vc
    for {
      localTs <- ProtoConverter.parseRequired(CantonTimestamp.fromProtoPrimitive, "local_ts", ts)
      sourceDomainId <- DomainId.fromProtoPrimitive(did, "origin_domain_id").map(SourceDomainId(_))
      party <- ProtoConverter.parseLfPartyId(partyId)
      domainTimestamps <- clock.toList.traverse { case (kProto, vProto) =>
        for {
          k <- DomainId.fromProtoPrimitive(kProto, "clock (key: DomainId)")
          v <- CantonTimestamp.fromProtoPrimitive(vProto)
        } yield k -> v
      }
    } yield {
      VectorClock(sourceDomainId, localTs, party, domainTimestamps.toMap)
    }
  }
}
