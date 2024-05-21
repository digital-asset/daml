// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data.topology

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.RefinedDurationConversionError
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.admin.grpc.TopologyStore
import com.digitalasset.canton.topology.admin.v30
import com.digitalasset.canton.topology.transaction.*
import com.google.protobuf.ByteString

import java.time.Instant

final case class BaseResult(
    store: TopologyStore,
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
      operation <- TopologyChangeOp.fromProtoV30(value.operation)
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
        TopologyStore.fromProto(_, "store"),
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
)

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
)

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

final case class ListIdentifierDelegationResult(
    context: BaseResult,
    item: IdentifierDelegation,
)

object ListIdentifierDelegationResult {
  def fromProtoV30(
      value: v30.ListIdentifierDelegationResponse.Result
  ): ParsingResult[ListIdentifierDelegationResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- IdentifierDelegation.fromProtoV30(itemProto)
    } yield ListIdentifierDelegationResult(context, item)
}

final case class ListOwnerToKeyMappingResult(
    context: BaseResult,
    item: OwnerToKeyMapping,
)

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

final case class ListDomainTrustCertificateResult(
    context: BaseResult,
    item: DomainTrustCertificate,
)

object ListDomainTrustCertificateResult {
  def fromProtoV30(
      value: v30.ListDomainTrustCertificateResponse.Result
  ): ParsingResult[ListDomainTrustCertificateResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- DomainTrustCertificate.fromProtoV30(itemProto)
    } yield ListDomainTrustCertificateResult(context, item)
}

final case class ListParticipantDomainPermissionResult(
    context: BaseResult,
    item: ParticipantDomainPermission,
)

object ListParticipantDomainPermissionResult {
  def fromProtoV30(
      value: v30.ListParticipantDomainPermissionResponse.Result
  ): ParsingResult[ListParticipantDomainPermissionResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- ParticipantDomainPermission.fromProtoV30(itemProto)
    } yield ListParticipantDomainPermissionResult(context, item)
}

final case class ListPartyHostingLimitsResult(
    context: BaseResult,
    item: PartyHostingLimits,
)

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
)

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
)

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

final case class ListAuthorityOfResult(
    context: BaseResult,
    item: AuthorityOf,
)

object ListAuthorityOfResult {
  def fromProtoV30(
      value: v30.ListAuthorityOfResponse.Result
  ): ParsingResult[ListAuthorityOfResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- AuthorityOf.fromProtoV30(itemProto)
    } yield ListAuthorityOfResult(context, item)
}

final case class ListDomainParametersStateResult(
    context: BaseResult,
    item: DynamicDomainParameters,
)

object ListDomainParametersStateResult {
  def fromProtoV30(
      value: v30.ListDomainParametersStateResponse.Result
  ): ParsingResult[ListDomainParametersStateResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- DynamicDomainParameters.fromProtoV30(itemProto)
    } yield ListDomainParametersStateResult(context, item)
}

final case class ListMediatorDomainStateResult(
    context: BaseResult,
    item: MediatorDomainState,
)

object ListMediatorDomainStateResult {
  def fromProtoV30(
      value: v30.ListMediatorDomainStateResponse.Result
  ): ParsingResult[ListMediatorDomainStateResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- MediatorDomainState.fromProtoV30(itemProto)
    } yield ListMediatorDomainStateResult(context, item)
}

final case class ListSequencerDomainStateResult(
    context: BaseResult,
    item: SequencerDomainState,
)

object ListSequencerDomainStateResult {
  def fromProtoV30(
      value: v30.ListSequencerDomainStateResponse.Result
  ): ParsingResult[ListSequencerDomainStateResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- SequencerDomainState.fromProtoV30(itemProto)
    } yield ListSequencerDomainStateResult(context, item)
}

final case class ListPurgeTopologyTransactionResult(
    context: BaseResult,
    item: PurgeTopologyTransaction,
)

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
