// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data.topology

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.RefinedDurationConversionError
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Fingerprint, Hash}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.admin.v30
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.version.ProtocolVersion

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
    transactionHash: TxHash,
    serial: PositiveInt,
    signedBy: NonEmpty[Seq[Fingerprint]],
)

object BaseResult {
  def fromProtoV30(value: v30.BaseResult): ParsingResult[BaseResult] = {
    val v30.BaseResult(
      storeId,
      sequenced,
      validFrom,
      validUntil,
      operation,
      transactionHash,
      serial,
      signedByFingerprints,
    ) = value
    for {
      protoValidFrom <- ProtoConverter.required("valid_from", validFrom)
      validFrom <- ProtoConverter.InstantConverter.fromProtoPrimitive(protoValidFrom)
      validUntil <- validUntil.traverse(ProtoConverter.InstantConverter.fromProtoPrimitive)
      protoSequenced <- ProtoConverter.required("sequenced", sequenced)
      sequenced <- ProtoConverter.InstantConverter.fromProtoPrimitive(protoSequenced)
      operation <- ProtoConverter.parseEnum(
        TopologyChangeOp.fromProtoV30,
        "operation",
        operation,
      )
      serial <- PositiveInt
        .create(serial)
        .leftMap(e => RefinedDurationConversionError("serial", e.message))
      signedBy <-
        ProtoConverter.parseRequiredNonEmpty(
          Fingerprint.fromProtoPrimitive,
          "signed_by_fingerprints",
          signedByFingerprints,
        )
      store <- ProtoConverter.parseRequired(
        TopologyStoreId.fromProtoV30(_, "store"),
        "store",
        storeId,
      )
      txHash <- Hash
        .fromByteString(transactionHash)
        .leftMap(ProtoDeserializationError.CryptoDeserializationError(_))
    } yield BaseResult(
      store,
      validFrom,
      validUntil,
      sequenced,
      operation,
      TxHash(txHash),
      serial,
      signedBy,
    )
  }
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

/** Console API class to conveniently represent a party to participant proposals
  *
  * @param txHash
  *   the hash of the proposal, required to approve it
  * @param party
  *   the party to be hosted
  * @param permission
  *   the permission for the currently selected participant
  * @param otherParticipants
  *   the other participants involved in the hosting relationship
  * @param threshold
  *   the signing threshold in case this is a multi-sig party
  */
final case class ListMultiHostingProposal(
    txHash: TxHash,
    party: PartyId,
    permission: ParticipantPermission,
    otherParticipants: Seq[HostingParticipant],
    threshold: PositiveInt,
) extends PrettyPrinting {
  override def pretty: Pretty[ListMultiHostingProposal] =
    prettyOfClass(
      param("txHash", _.txHash.hash),
      param("party", _.party),
      param("permission", _.permission.showType),
      param("others", _.otherParticipants.map(x => (x.participantId, x.permission.showType)).toMap),
      param("threshold", _.threshold),
    )
}

object ListMultiHostingProposal {
  def mapFilter(participantId: ParticipantId)(
      result: ListPartyToParticipantResult
  ): Option[ListMultiHostingProposal] =
    result.item.participants.find(_.participantId == participantId).map { res =>
      ListMultiHostingProposal(
        txHash = result.context.transactionHash,
        party = result.item.partyId,
        permission = res.permission,
        otherParticipants = result.item.participants.filter(_.participantId != participantId),
        threshold = result.item.threshold,
      )
    }

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
