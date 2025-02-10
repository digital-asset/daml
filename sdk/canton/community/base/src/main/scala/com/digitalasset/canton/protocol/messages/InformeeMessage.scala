// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.crypto.{HashOps, Signature}
import com.digitalasset.canton.data.{
  FullInformeeTree,
  ViewConfirmationParameters,
  ViewPosition,
  ViewType,
}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{RootHash, v30}
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.version.{
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanionContext,
}

import java.util.UUID

/** The informee message to be sent to the mediator.
  */
// This class is a reference example of serialization best practices.
// It is a simple example for getting started with serialization.
// Please consult the team if you intend to change the design of serialization.
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class InformeeMessage(
    fullInformeeTree: FullInformeeTree,
    override val submittingParticipantSignature: Signature,
)(
    protocolVersion: ProtocolVersion
) extends MediatorConfirmationRequest
    // By default, we use ProtoBuf for serialization.
    // Serializable classes that have a corresponding Protobuf message should inherit from this trait to inherit common code and naming conventions.
    // If the corresponding Protobuf message of a class has multiple versions (e.g. `InformeeMessage`),
    with UnsignedProtocolMessage {

  override val representativeProtocolVersion: RepresentativeProtocolVersion[InformeeMessage.type] =
    InformeeMessage.protocolVersionRepresentativeFor(protocolVersion)

  override def submittingParticipant: ParticipantId = fullInformeeTree.submittingParticipant

  def copy(
      fullInformeeTree: FullInformeeTree = this.fullInformeeTree,
      submittingParticipantSignature: Signature = this.submittingParticipantSignature,
  ): InformeeMessage =
    InformeeMessage(fullInformeeTree, submittingParticipantSignature)(protocolVersion)

  override def requestUuid: UUID = fullInformeeTree.transactionUuid

  override def synchronizerId: SynchronizerId = fullInformeeTree.synchronizerId

  override def mediator: MediatorGroupRecipient = fullInformeeTree.mediator

  override def informeesAndConfirmationParamsByViewPosition
      : Map[ViewPosition, ViewConfirmationParameters] =
    fullInformeeTree.informeesAndThresholdByViewPosition

  // Implementing a `toProto<version>` method allows us to compose serializable classes.
  // You should define the toProtoV30 method on the serializable class, because then it is easiest to find and use.
  // (Conversely, you should not define a separate proto converter class.)
  def toProtoV30: v30.InformeeMessage =
    v30.InformeeMessage(
      fullInformeeTree = Some(fullInformeeTree.toProtoV30),
      submittingParticipantSignature = Some(submittingParticipantSignature.toProtoV30),
    )

  override def toProtoSomeEnvelopeContentV30: v30.EnvelopeContent.SomeEnvelopeContent =
    v30.EnvelopeContent.SomeEnvelopeContent.InformeeMessage(toProtoV30)

  override def rootHash: RootHash = fullInformeeTree.transactionId.toRootHash

  override def viewType: ViewType = ViewType.TransactionViewType

  override def pretty: Pretty[InformeeMessage] = prettyOfClass(unnamedParam(_.fullInformeeTree))

  @transient override protected lazy val companionObj: InformeeMessage.type = InformeeMessage
}

object InformeeMessage
    extends VersioningCompanionContext[InformeeMessage, (HashOps, ProtocolVersion)] {

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v33)(v30.InformeeMessage)(
      supportedProtoVersion(_)((hashOps, proto) => fromProtoV30(hashOps)(proto)),
      _.toProtoV30,
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
  private[messages] def fromProtoV30(
      context: (HashOps, ProtocolVersion)
  )(informeeMessageP: v30.InformeeMessage): ParsingResult[InformeeMessage] = {
    val (_, protocolVersion) = context

    // Use pattern matching to access the fields of v0.InformeeMessage,
    // because this will break if a field is forgotten.
    val v30.InformeeMessage(
      maybeFullInformeeTreeP,
      submittingParticipantSignaturePO,
    ) = informeeMessageP
    for {
      // Keep in mind that all fields of a proto class are optional. So the existence must be checked explicitly.
      fullInformeeTreeP <- ProtoConverter.required(
        "InformeeMessage.informeeTree",
        maybeFullInformeeTreeP,
      )
      fullInformeeTree <- FullInformeeTree.fromProtoV30(context, fullInformeeTreeP)
      submittingParticipantSignature <- ProtoConverter
        .required(
          "InformeeMessage.submittingParticipantSignature",
          submittingParticipantSignaturePO,
        )
        .flatMap(Signature.fromProtoV30)
    } yield new InformeeMessage(fullInformeeTree, submittingParticipantSignature)(protocolVersion)
  }

  implicit val informeeMessageCast: ProtocolMessageContentCast[InformeeMessage] =
    ProtocolMessageContentCast.create[InformeeMessage]("InformeeMessage") {
      case im: InformeeMessage => Some(im)
      case _ => None
    }

  override def name: String = "InformeeMessage"
}
