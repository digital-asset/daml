// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.{FullInformeeTree, Informee, ViewPosition, ViewType}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{RequestId, RootHash, ViewHash, v1, v4}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, MediatorRef}
import com.digitalasset.canton.version.{
  HasProtocolVersionedWithContextCompanion,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}

import java.util.UUID

/** The informee message to be sent to the mediator.
  */
// This class is a reference example of serialization best practices.
// It is a simple example for getting started with serialization.
// Please consult the team if you intend to change the design of serialization.
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class InformeeMessage(fullInformeeTree: FullInformeeTree)(
    protocolVersion: ProtocolVersion
) extends MediatorRequest
    // By default, we use ProtoBuf for serialization.
    // Serializable classes that have a corresponding Protobuf message should inherit from this trait to inherit common code and naming conventions.
    // If the corresponding Protobuf message of a class has multiple versions (e.g. `v0.InformeeMessage` and `v1.InformeeMessage`),
    with UnsignedProtocolMessage {

  override val representativeProtocolVersion: RepresentativeProtocolVersion[InformeeMessage.type] =
    InformeeMessage.protocolVersionRepresentativeFor(protocolVersion)

  def copy(fullInformeeTree: FullInformeeTree = this.fullInformeeTree): InformeeMessage =
    InformeeMessage(fullInformeeTree)(protocolVersion)

  override def requestUuid: UUID = fullInformeeTree.transactionUuid

  override def domainId: DomainId = fullInformeeTree.domainId

  override def mediator: MediatorRef = fullInformeeTree.mediator

  override def informeesAndThresholdByViewHash: Map[ViewHash, (Set[Informee], NonNegativeInt)] =
    fullInformeeTree.informeesAndThresholdByViewHash

  override def informeesAndThresholdByViewPosition
      : Map[ViewPosition, (Set[Informee], NonNegativeInt)] =
    fullInformeeTree.informeesAndThresholdByViewPosition

  override def createMediatorResult(
      requestId: RequestId,
      verdict: Verdict,
      recipientParties: Set[LfPartyId],
  ): TransactionResultMessage =
    TransactionResultMessage(
      requestId,
      verdict,
      fullInformeeTree.tree.rootHash,
      domainId,
      protocolVersion,
    )

  // Implementing a `toProto<version>` method allows us to compose serializable classes.
  // You should define the toProtoV0 method on the serializable class, because then it is easiest to find and use.
  // (Conversely, you should not define a separate proto converter class.)
  def toProtoV1: v1.InformeeMessage =
    v1.InformeeMessage(
      fullInformeeTree = Some(fullInformeeTree.toProtoV1),
      protocolVersion = protocolVersion.toProtoPrimitive,
    )

  override def toProtoSomeEnvelopeContentV4: v4.EnvelopeContent.SomeEnvelopeContent =
    v4.EnvelopeContent.SomeEnvelopeContent.InformeeMessage(toProtoV1)

  override def minimumThreshold(informees: Set[Informee]): NonNegativeInt =
    fullInformeeTree.confirmationPolicy.minimumThreshold(informees)

  override def rootHash: Option[RootHash] = Some(fullInformeeTree.transactionId.toRootHash)

  override def viewType: ViewType = ViewType.TransactionViewType

  override def pretty: Pretty[InformeeMessage] = prettyOfClass(unnamedParam(_.fullInformeeTree))

  @transient override protected lazy val companionObj: InformeeMessage.type = InformeeMessage
}

object InformeeMessage extends HasProtocolVersionedWithContextCompanion[InformeeMessage, HashOps] {

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v30)(v1.InformeeMessage)(
      supportedProtoVersion(_)((hashOps, proto) => fromProtoV1(hashOps)(proto)),
      _.toProtoV1.toByteString,
    )
  )

  // The inverse of "toProto<version>".
  //
  // On error, it returns `Left(...)` as callers cannot predict whether the conversion would succeed.
  // So the caller is forced to handle failing conversion.
  // Conversely, the method absolutely must not throw an exception, because this will likely kill the calling thread.
  // So it would be a DOS vulnerability.
  //
  // There is no agreed convention on which type to use for errors. In this class it is "ProtoDeserializationError",
  // but other classes use something else (e.g. "String").
  // In the end, it is most important that the errors are informative and this can be achieved in different ways.
  private[messages] def fromProtoV1(
      hashOps: HashOps
  )(informeeMessageP: v1.InformeeMessage): ParsingResult[InformeeMessage] = {
    // Use pattern matching to access the fields of v0.InformeeMessage,
    // because this will break if a field is forgotten.
    val v1.InformeeMessage(maybeFullInformeeTreeP, protocolVersionP) = informeeMessageP
    for {
      // Keep in mind that all fields of a proto class are optional. So the existence must be checked explicitly.
      fullInformeeTreeP <- ProtoConverter.required(
        "InformeeMessage.informeeTree",
        maybeFullInformeeTreeP,
      )
      fullInformeeTree <- FullInformeeTree.fromProtoV1(hashOps, fullInformeeTreeP)
      protocolVersion <- ProtocolVersion.fromProtoPrimitive(protocolVersionP)
    } yield new InformeeMessage(fullInformeeTree)(protocolVersion)
  }

  implicit val informeeMessageCast: ProtocolMessageContentCast[InformeeMessage] =
    ProtocolMessageContentCast.create[InformeeMessage]("InformeeMessage") {
      case im: InformeeMessage => Some(im)
      case _ => None
    }

  override def name: String = "InformeeMessage"
}
