// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data.topologyx

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
import com.digitalasset.canton.topology.admin.v1
import com.digitalasset.canton.topology.transaction.{
  AuthorityOfX,
  DecentralizedNamespaceDefinitionX,
  DomainTrustCertificateX,
  IdentifierDelegationX,
  MediatorDomainStateX,
  NamespaceDelegationX,
  OwnerToKeyMappingX,
  ParticipantDomainPermissionX,
  PartyHostingLimitsX,
  PartyToParticipantX,
  PurgeTopologyTransactionX,
  SequencerDomainStateX,
  TopologyChangeOpX,
  TrafficControlStateX,
  VettedPackagesX,
}
import com.google.protobuf.ByteString

import java.time.Instant

final case class BaseResult(
    store: TopologyStore,
    validFrom: Instant,
    validUntil: Option[Instant],
    operation: TopologyChangeOpX,
    transactionHash: ByteString,
    serial: PositiveInt,
    signedBy: NonEmpty[Seq[Fingerprint]],
)

object BaseResult {
  def fromProtoV1(value: v1.BaseResult): ParsingResult[BaseResult] =
    for {
      protoValidFrom <- ProtoConverter.required("valid_from", value.validFrom)
      validFrom <- ProtoConverter.InstantConverter.fromProtoPrimitive(protoValidFrom)
      validUntil <- value.validUntil.traverse(ProtoConverter.InstantConverter.fromProtoPrimitive)
      operation <- TopologyChangeOpX.fromProtoV2(value.operation)
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
      operation,
      value.transactionHash,
      serial,
      signedBy,
    )
}

final case class ListNamespaceDelegationResult(
    context: BaseResult,
    item: NamespaceDelegationX,
)

object ListNamespaceDelegationResult {
  def fromProtoV1(
      value: v1.ListNamespaceDelegationResult.Result
  ): ParsingResult[ListNamespaceDelegationResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV1(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- NamespaceDelegationX.fromProtoV2(itemProto)
    } yield ListNamespaceDelegationResult(context, item)
}

final case class ListDecentralizedNamespaceDefinitionResult(
    context: BaseResult,
    item: DecentralizedNamespaceDefinitionX,
)

object ListDecentralizedNamespaceDefinitionResult {
  def fromProtoV1(
      value: v1.ListDecentralizedNamespaceDefinitionResult.Result
  ): ParsingResult[ListDecentralizedNamespaceDefinitionResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV1(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- DecentralizedNamespaceDefinitionX.fromProtoV2(itemProto)
    } yield ListDecentralizedNamespaceDefinitionResult(context, item)
}

final case class ListIdentifierDelegationResult(
    context: BaseResult,
    item: IdentifierDelegationX,
)

object ListIdentifierDelegationResult {
  def fromProtoV1(
      value: v1.ListIdentifierDelegationResult.Result
  ): ParsingResult[ListIdentifierDelegationResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV1(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- IdentifierDelegationX.fromProtoV2(itemProto)
    } yield ListIdentifierDelegationResult(context, item)
}

final case class ListOwnerToKeyMappingResult(
    context: BaseResult,
    item: OwnerToKeyMappingX,
)

object ListOwnerToKeyMappingResult {
  def fromProtoV1(
      value: v1.ListOwnerToKeyMappingResult.Result
  ): ParsingResult[ListOwnerToKeyMappingResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV1(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- OwnerToKeyMappingX.fromProtoV2(itemProto)
    } yield ListOwnerToKeyMappingResult(context, item)
}

final case class ListDomainTrustCertificateResult(
    context: BaseResult,
    item: DomainTrustCertificateX,
)

object ListDomainTrustCertificateResult {
  def fromProtoV1(
      value: v1.ListDomainTrustCertificateResult.Result
  ): ParsingResult[ListDomainTrustCertificateResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV1(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- DomainTrustCertificateX.fromProtoV2(itemProto)
    } yield ListDomainTrustCertificateResult(context, item)
}

final case class ListParticipantDomainPermissionResult(
    context: BaseResult,
    item: ParticipantDomainPermissionX,
)

object ListParticipantDomainPermissionResult {
  def fromProtoV1(
      value: v1.ListParticipantDomainPermissionResult.Result
  ): ParsingResult[ListParticipantDomainPermissionResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV1(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- ParticipantDomainPermissionX.fromProtoV2(itemProto)
    } yield ListParticipantDomainPermissionResult(context, item)
}

final case class ListPartyHostingLimitsResult(
    context: BaseResult,
    item: PartyHostingLimitsX,
)

object ListPartyHostingLimitsResult {
  def fromProtoV1(
      value: v1.ListPartyHostingLimitsResult.Result
  ): ParsingResult[ListPartyHostingLimitsResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV1(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- PartyHostingLimitsX.fromProtoV2(itemProto)
    } yield ListPartyHostingLimitsResult(context, item)
}

final case class ListVettedPackagesResult(
    context: BaseResult,
    item: VettedPackagesX,
)

object ListVettedPackagesResult {
  def fromProtoV1(
      value: v1.ListVettedPackagesResult.Result
  ): ParsingResult[ListVettedPackagesResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV1(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- VettedPackagesX.fromProtoV2(itemProto)
    } yield ListVettedPackagesResult(context, item)
}

final case class ListPartyToParticipantResult(
    context: BaseResult,
    item: PartyToParticipantX,
)

object ListPartyToParticipantResult {
  def fromProtoV1(
      value: v1.ListPartyToParticipantResult.Result
  ): ParsingResult[ListPartyToParticipantResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV1(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- PartyToParticipantX.fromProtoV2(itemProto)
    } yield ListPartyToParticipantResult(context, item)
}

final case class ListAuthorityOfResult(
    context: BaseResult,
    item: AuthorityOfX,
)

object ListAuthorityOfResult {
  def fromProtoV1(
      value: v1.ListAuthorityOfResult.Result
  ): ParsingResult[ListAuthorityOfResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV1(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- AuthorityOfX.fromProtoV2(itemProto)
    } yield ListAuthorityOfResult(context, item)
}

final case class ListDomainParametersStateResult(
    context: BaseResult,
    item: DynamicDomainParameters,
)

object ListDomainParametersStateResult {
  def fromProtoV1(
      value: v1.ListDomainParametersStateResult.Result
  ): ParsingResult[ListDomainParametersStateResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV1(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- DynamicDomainParameters.fromProtoV2(itemProto)
    } yield ListDomainParametersStateResult(context, item)
}

final case class ListMediatorDomainStateResult(
    context: BaseResult,
    item: MediatorDomainStateX,
)

object ListMediatorDomainStateResult {
  def fromProtoV1(
      value: v1.ListMediatorDomainStateResult.Result
  ): ParsingResult[ListMediatorDomainStateResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV1(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- MediatorDomainStateX.fromProtoV2(itemProto)
    } yield ListMediatorDomainStateResult(context, item)
}

final case class ListSequencerDomainStateResult(
    context: BaseResult,
    item: SequencerDomainStateX,
)

object ListSequencerDomainStateResult {
  def fromProtoV1(
      value: v1.ListSequencerDomainStateResult.Result
  ): ParsingResult[ListSequencerDomainStateResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV1(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- SequencerDomainStateX.fromProtoV2(itemProto)
    } yield ListSequencerDomainStateResult(context, item)
}

final case class ListPurgeTopologyTransactionXResult(
    context: BaseResult,
    item: PurgeTopologyTransactionX,
)

object ListPurgeTopologyTransactionXResult {
  def fromProtoV1(
      value: v1.ListPurgeTopologyTransactionXResult.Result
  ): ParsingResult[ListPurgeTopologyTransactionXResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV1(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- PurgeTopologyTransactionX.fromProtoV2(itemProto)
    } yield ListPurgeTopologyTransactionXResult(context, item)
}

final case class ListTrafficStateResult(
    context: BaseResult,
    item: TrafficControlStateX,
)

object ListTrafficStateResult {
  def fromProtoV1(
      value: v1.ListTrafficStateResult.Result
  ): ParsingResult[ListTrafficStateResult] =
    for {
      context <- ProtoConverter.parseRequired(BaseResult.fromProtoV1, "context", value.context)
      item <- ProtoConverter.parseRequired(TrafficControlStateX.fromProtoV2, "item", value.item)
    } yield ListTrafficStateResult(context, item)
}
