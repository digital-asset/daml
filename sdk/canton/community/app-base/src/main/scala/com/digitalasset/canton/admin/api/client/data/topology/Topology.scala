// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data.topology

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.RefinedDurationConversionError
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.admin.v30
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import java.time.Instant

sealed trait TopologyResult[M <: TopologyMapping] {
  def context: BaseResult
  def item: M

  /** Convert to a topology transaction TODO(i25529): ideally this would be returned by the gRPC
    * server and not be done in the console macro
    */
  def toTopologyTransaction: TopologyTransaction[TopologyChangeOp, M] =
    TopologyTransaction[TopologyChangeOp, M](
      context.operation,
      context.serial,
      item,
      ProtocolVersion.latest,
    )
}

final case class BaseResult(
    storeId: TopologyStoreId,
    validFrom: Instant,
    validUntil: Option[Instant],
    sequenced: Instant,
    operation: TopologyChangeOp,
    transactionHash: ByteString,
    serial: PositiveInt,
    signedBy: NonEmpty[Seq[Fingerprint]],
)

object BaseResult {
  def fromProtoV30(value: v30.BaseResult): ParsingResult[BaseResult] =
    for {
      protoValidFrom <- ProtoConverter.required("valid_from", value.validFrom)
      validFrom <- ProtoConverter.InstantConverter.fromProtoPrimitive(protoValidFrom)
      validUntil <- value.validUntil.traverse(ProtoConverter.InstantConverter.fromProtoPrimitive)
      protoSequenced <- ProtoConverter.required("sequencer", value.sequenced)
      sequenced <- ProtoConverter.InstantConverter.fromProtoPrimitive(protoSequenced)
      operation <- ProtoConverter.parseEnum(
        TopologyChangeOp.fromProtoV30,
        "operation",
        value.operation,
      )
      serial <- PositiveInt
        .create(value.serial)
        .leftMap(e => RefinedDurationConversionError("serial", e.message))
      signedBy <-
        ProtoConverter.parseRequiredNonEmpty(
          Fingerprint.fromProtoPrimitive,
          "signed_by_fingerprints",
          value.signedByFingerprints,
        )

      store <- ProtoConverter.parseRequired(
        TopologyStoreId.fromProtoV30(_, "store"),
        "store",
        value.store,
      )
    } yield BaseResult(
      store,
      validFrom,
      validUntil,
      sequenced,
      operation,
      value.transactionHash,
      serial,
      signedBy,
    )
}

final case class ListNamespaceDelegationResult(
    context: BaseResult,
    item: NamespaceDelegation,
) extends TopologyResult[NamespaceDelegation]

object ListNamespaceDelegationResult {
  def fromProtoV30(
      value: v30.ListNamespaceDelegationResponse.Result
  ): ParsingResult[ListNamespaceDelegationResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- NamespaceDelegation.fromProtoV30(itemProto)
    } yield ListNamespaceDelegationResult(context, item)
}

final case class ListDecentralizedNamespaceDefinitionResult(
    context: BaseResult,
    item: DecentralizedNamespaceDefinition,
) extends TopologyResult[DecentralizedNamespaceDefinition]

object ListDecentralizedNamespaceDefinitionResult {
  def fromProtoV30(
      value: v30.ListDecentralizedNamespaceDefinitionResponse.Result
  ): ParsingResult[ListDecentralizedNamespaceDefinitionResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- DecentralizedNamespaceDefinition.fromProtoV30(itemProto)
    } yield ListDecentralizedNamespaceDefinitionResult(context, item)
}

final case class ListOwnerToKeyMappingResult(
    context: BaseResult,
    item: OwnerToKeyMapping,
) extends TopologyResult[OwnerToKeyMapping]

object ListOwnerToKeyMappingResult {
  def fromProtoV30(
      value: v30.ListOwnerToKeyMappingResponse.Result
  ): ParsingResult[ListOwnerToKeyMappingResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- OwnerToKeyMapping.fromProtoV30(itemProto)
    } yield ListOwnerToKeyMappingResult(context, item)
}

final case class ListPartyToKeyMappingResult(
    context: BaseResult,
    item: PartyToKeyMapping,
) extends TopologyResult[PartyToKeyMapping]

object ListPartyToKeyMappingResult {
  def fromProtoV30(
      value: v30.ListPartyToKeyMappingResponse.Result
  ): ParsingResult[ListPartyToKeyMappingResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- PartyToKeyMapping.fromProtoV30(itemProto)
    } yield ListPartyToKeyMappingResult(context, item)
}

final case class ListSynchronizerTrustCertificateResult(
    context: BaseResult,
    item: SynchronizerTrustCertificate,
) extends TopologyResult[SynchronizerTrustCertificate]

object ListSynchronizerTrustCertificateResult {
  def fromProtoV30(
      value: v30.ListSynchronizerTrustCertificateResponse.Result
  ): ParsingResult[ListSynchronizerTrustCertificateResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- SynchronizerTrustCertificate.fromProtoV30(itemProto)
    } yield ListSynchronizerTrustCertificateResult(context, item)
}

final case class ListParticipantSynchronizerPermissionResult(
    context: BaseResult,
    item: ParticipantSynchronizerPermission,
) extends TopologyResult[ParticipantSynchronizerPermission]

object ListParticipantSynchronizerPermissionResult {
  def fromProtoV30(
      value: v30.ListParticipantSynchronizerPermissionResponse.Result
  ): ParsingResult[ListParticipantSynchronizerPermissionResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- ParticipantSynchronizerPermission.fromProtoV30(itemProto)
    } yield ListParticipantSynchronizerPermissionResult(context, item)
}

final case class ListPartyHostingLimitsResult(
    context: BaseResult,
    item: PartyHostingLimits,
) extends TopologyResult[PartyHostingLimits]

object ListPartyHostingLimitsResult {
  def fromProtoV30(
      value: v30.ListPartyHostingLimitsResponse.Result
  ): ParsingResult[ListPartyHostingLimitsResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- PartyHostingLimits.fromProtoV30(itemProto)
    } yield ListPartyHostingLimitsResult(context, item)
}

final case class ListVettedPackagesResult(
    context: BaseResult,
    item: VettedPackages,
) extends TopologyResult[VettedPackages]

object ListVettedPackagesResult {
  def fromProtoV30(
      value: v30.ListVettedPackagesResponse.Result
  ): ParsingResult[ListVettedPackagesResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- VettedPackages.fromProtoV30(itemProto)
    } yield ListVettedPackagesResult(context, item)
}

final case class ListPartyToParticipantResult(
    context: BaseResult,
    item: PartyToParticipant,
) extends TopologyResult[PartyToParticipant]

object ListPartyToParticipantResult {
  def fromProtoV30(
      value: v30.ListPartyToParticipantResponse.Result
  ): ParsingResult[ListPartyToParticipantResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- PartyToParticipant.fromProtoV30(itemProto)
    } yield ListPartyToParticipantResult(context, item)
}

final case class ListSynchronizerParametersStateResult(
    context: BaseResult,
    item: DynamicSynchronizerParameters,
)

object ListSynchronizerParametersStateResult {
  def fromProtoV30(
      value: v30.ListSynchronizerParametersStateResponse.Result
  ): ParsingResult[ListSynchronizerParametersStateResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- DynamicSynchronizerParameters.fromProtoV30(itemProto)
    } yield ListSynchronizerParametersStateResult(context, item)
}

final case class ListMediatorSynchronizerStateResult(
    context: BaseResult,
    item: MediatorSynchronizerState,
) extends TopologyResult[MediatorSynchronizerState]

object ListMediatorSynchronizerStateResult {
  def fromProtoV30(
      value: v30.ListMediatorSynchronizerStateResponse.Result
  ): ParsingResult[ListMediatorSynchronizerStateResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- MediatorSynchronizerState.fromProtoV30(itemProto)
    } yield ListMediatorSynchronizerStateResult(context, item)
}

final case class ListSequencerSynchronizerStateResult(
    context: BaseResult,
    item: SequencerSynchronizerState,
) extends TopologyResult[SequencerSynchronizerState]

object ListSequencerSynchronizerStateResult {
  def fromProtoV30(
      value: v30.ListSequencerSynchronizerStateResponse.Result
  ): ParsingResult[ListSequencerSynchronizerStateResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- SequencerSynchronizerState.fromProtoV30(itemProto)
    } yield ListSequencerSynchronizerStateResult(context, item)
}

final case class ListPurgeTopologyTransactionResult(
    context: BaseResult,
    item: PurgeTopologyTransaction,
) extends TopologyResult[PurgeTopologyTransaction]

object ListPurgeTopologyTransactionResult {
  def fromProtoV30(
      value: v30.ListPurgeTopologyTransactionResponse.Result
  ): ParsingResult[ListPurgeTopologyTransactionResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- PurgeTopologyTransaction.fromProtoV30(itemProto)
    } yield ListPurgeTopologyTransactionResult(context, item)
}

final case class ListSynchronizerUpgradeAnnouncementResult(
    context: BaseResult,
    item: SynchronizerUpgradeAnnouncement,
)

object ListSynchronizerUpgradeAnnouncementResult {
  def fromProtoV30(
      value: v30.ListSynchronizerUpgradeAnnouncementResponse.Result
  ): ParsingResult[ListSynchronizerUpgradeAnnouncementResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- SynchronizerUpgradeAnnouncement.fromProtoV30(itemProto)
    } yield ListSynchronizerUpgradeAnnouncementResult(context, item)
}

final case class ListSequencerConnectionSuccessorResult(
    context: BaseResult,
    item: SequencerConnectionSuccessor,
)

object ListSequencerConnectionSuccessorResult {
  def fromProtoV30(
      value: v30.ListSequencerConnectionSuccessorResponse.Result
  ): ParsingResult[ListSequencerConnectionSuccessorResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- SequencerConnectionSuccessor.fromProtoV30(itemProto)
    } yield ListSequencerConnectionSuccessorResult(context, item)
}
