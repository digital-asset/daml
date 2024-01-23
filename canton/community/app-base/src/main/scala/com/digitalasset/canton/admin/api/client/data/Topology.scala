// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.admin.api.client.data.ListPartiesResult.ParticipantDomains
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.protocol.DynamicDomainParameters as DynamicDomainParametersInternal
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.TopologyStore
import com.digitalasset.canton.topology.admin.v30old.ListDomainParametersChangesResult.Result.Parameters
import com.digitalasset.canton.topology.admin.{v30, v30old}
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.version.ProtocolVersionValidation
import com.google.protobuf.ByteString

import java.time.Instant

final case class ListPartiesResult(party: PartyId, participants: Seq[ParticipantDomains])

object ListPartiesResult {
  final case class DomainPermission(domain: DomainId, permission: ParticipantPermission)
  final case class ParticipantDomains(participant: ParticipantId, domains: Seq[DomainPermission])

  private def fromProtoV30(
      value: v30.ListPartiesResponse.Result.ParticipantDomains.DomainPermissions
  ): ParsingResult[DomainPermission] =
    for {
      domainId <- DomainId.fromProtoPrimitive(value.domain, "domain")
      permission <- ParticipantPermission.fromProtoEnum(value.permission)
    } yield DomainPermission(domainId, permission)

  private def fromProtoV30(
      value: v30.ListPartiesResponse.Result.ParticipantDomains
  ): ParsingResult[ParticipantDomains] =
    for {
      participantId <- ParticipantId.fromProtoPrimitive(value.participant, "participant")
      domains <- value.domains.traverse(fromProtoV30)
    } yield ParticipantDomains(participantId, domains)

  def fromProtoV30(
      value: v30.ListPartiesResponse.Result
  ): ParsingResult[ListPartiesResult] =
    for {
      partyUid <- UniqueIdentifier.fromProtoPrimitive(value.party, "party")
      participants <- value.participants.traverse(fromProtoV30)
    } yield ListPartiesResult(PartyId(partyUid), participants)
}

final case class ListKeyOwnersResult(
    store: DomainId,
    owner: Member,
    signingKeys: Seq[SigningPublicKey],
    encryptionKeys: Seq[EncryptionPublicKey],
) {
  def keys(purpose: KeyPurpose): Seq[PublicKey] = purpose match {
    case KeyPurpose.Signing => signingKeys
    case KeyPurpose.Encryption => encryptionKeys
  }
}

object ListKeyOwnersResult {
  def fromProtoV30(
      value: v30.ListKeyOwnersResponse.Result
  ): ParsingResult[ListKeyOwnersResult] =
    for {
      domain <- DomainId.fromProtoPrimitive(value.domain, "domain")
      owner <- Member.fromProtoPrimitive(value.keyOwner, "keyOwner")
      signingKeys <- value.signingKeys.traverse(SigningPublicKey.fromProtoV30)
      encryptionKeys <- value.encryptionKeys.traverse(EncryptionPublicKey.fromProtoV30)
    } yield ListKeyOwnersResult(domain, owner, signingKeys, encryptionKeys)
}

final case class BaseResult(
    store: TopologyStore,
    validFrom: Instant,
    validUntil: Option[Instant],
    operation: TopologyChangeOp,
    serialized: ByteString,
    signedBy: Fingerprint,
)

object BaseResult {
  def fromProtoV30(value: v30old.BaseResult): ParsingResult[BaseResult] =
    for {
      protoValidFrom <- ProtoConverter.required("valid_from", value.validFrom)
      validFrom <- ProtoConverter.InstantConverter.fromProtoPrimitive(protoValidFrom)
      validUntil <- value.validUntil.traverse(ProtoConverter.InstantConverter.fromProtoPrimitive)
      operation <- TopologyChangeOp.fromProtoV30(value.operation)
      signedBy <- Fingerprint.fromProtoPrimitive(value.signedByFingerprint)
      store <-
        if (value.store == AuthorizedStore.dbString.unwrap)
          Right(TopologyStore.Authorized)
        else
          DomainId.fromProtoPrimitive(value.store, "store").map(TopologyStore.Domain)
    } yield BaseResult(store, validFrom, validUntil, operation, value.serialized, signedBy)
}

final case class ListPartyToParticipantResult(context: BaseResult, item: PartyToParticipant)

object ListPartyToParticipantResult {
  def fromProtoV30(
      value: v30old.ListPartyToParticipantResult.Result
  ): ParsingResult[ListPartyToParticipantResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- PartyToParticipant.fromProtoV30(itemProto)
    } yield ListPartyToParticipantResult(context, item)
}

final case class ListOwnerToKeyMappingResult(
    context: BaseResult,
    item: OwnerToKeyMapping,
    key: Fingerprint,
)

object ListOwnerToKeyMappingResult {
  def fromProtoV30(
      value: v30old.ListOwnerToKeyMappingResult.Result
  ): ParsingResult[ListOwnerToKeyMappingResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- OwnerToKeyMapping.fromProtoV30(itemProto)
      key <- Fingerprint.fromProtoPrimitive(value.keyFingerprint)
    } yield ListOwnerToKeyMappingResult(context, item, key)
}

final case class ListNamespaceDelegationResult(
    context: BaseResult,
    item: NamespaceDelegation,
    targetKey: Fingerprint,
)

object ListNamespaceDelegationResult {
  def fromProtoV30(
      value: v30old.ListNamespaceDelegationResult.Result
  ): ParsingResult[ListNamespaceDelegationResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- NamespaceDelegation.fromProtoV30(itemProto)
      targetKey <- Fingerprint.fromProtoPrimitive(value.targetKeyFingerprint)
    } yield ListNamespaceDelegationResult(context, item, targetKey)
}

final case class ListIdentifierDelegationResult(
    context: BaseResult,
    item: IdentifierDelegation,
    targetKey: Fingerprint,
)

object ListIdentifierDelegationResult {
  def fromProtoV30(
      value: v30old.ListIdentifierDelegationResult.Result
  ): ParsingResult[ListIdentifierDelegationResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- IdentifierDelegation.fromProtoV30(itemProto)
      targetKey <- Fingerprint.fromProtoPrimitive(value.targetKeyFingerprint)
    } yield ListIdentifierDelegationResult(context, item, targetKey)
}

final case class ListSignedLegalIdentityClaimResult(context: BaseResult, item: LegalIdentityClaim)

object ListSignedLegalIdentityClaimResult {
  def fromProtoV30(
      value: v30old.ListSignedLegalIdentityClaimResult.Result
  ): ParsingResult[ListSignedLegalIdentityClaimResult] = {
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- SignedLegalIdentityClaim.fromProtoV30(
        ProtocolVersionValidation.NoValidation,
        itemProto,
      )
      claim <- LegalIdentityClaim.fromByteStringUnsafe(
        item.claim
      )
    } yield ListSignedLegalIdentityClaimResult(context, claim)
  }
}

final case class ListParticipantDomainStateResult(context: BaseResult, item: ParticipantState)

object ListParticipantDomainStateResult {
  def fromProtoV30(
      value: v30old.ListParticipantDomainStateResult.Result
  ): ParsingResult[ListParticipantDomainStateResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- ParticipantState.fromProtoV30(itemProto)
    } yield ListParticipantDomainStateResult(context, item)

}

final case class ListMediatorDomainStateResult(context: BaseResult, item: MediatorDomainState)

object ListMediatorDomainStateResult {
  def fromProtoV30(
      value: v30old.ListMediatorDomainStateResult.Result
  ): ParsingResult[ListMediatorDomainStateResult] =
    for {
      contextProto <- ProtoConverter.required("context", value.context)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", value.item)
      item <- MediatorDomainState.fromProtoV30(itemProto)
    } yield ListMediatorDomainStateResult(context, item)

}

final case class ListVettedPackagesResult(context: BaseResult, item: VettedPackages)

object ListVettedPackagesResult {
  def fromProtoV30(
      value: v30old.ListVettedPackagesResult.Result
  ): ParsingResult[ListVettedPackagesResult] = {
    val v30old.ListVettedPackagesResult.Result(contextPO, itemPO) = value
    for {
      contextProto <- ProtoConverter.required("context", contextPO)
      context <- BaseResult.fromProtoV30(contextProto)
      itemProto <- ProtoConverter.required("item", itemPO)
      item <- VettedPackages.fromProtoV30(itemProto)
    } yield ListVettedPackagesResult(context, item)
  }
}

final case class ListDomainParametersChangeResult(
    context: BaseResult,
    item: DynamicDomainParameters,
)

object ListDomainParametersChangeResult {
  def fromProtoV30(
      value: v30old.ListDomainParametersChangesResult.Result
  ): ParsingResult[ListDomainParametersChangeResult] = for {
    contextP <- value.context.toRight(ProtoDeserializationError.FieldNotSet("context"))
    context <- BaseResult.fromProtoV30(contextP)
    dynamicDomainParametersInternal <- value.parameters match {
      case Parameters.Empty => Left(ProtoDeserializationError.FieldNotSet("parameters"))
      case Parameters.V1(ddpX) => DynamicDomainParametersInternal.fromProtoV30(ddpX)
    }
    item = DynamicDomainParameters(dynamicDomainParametersInternal)
  } yield ListDomainParametersChangeResult(context, item)
}
