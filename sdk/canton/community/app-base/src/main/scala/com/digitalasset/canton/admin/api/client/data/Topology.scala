// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.traverse.*
import com.digitalasset.canton.admin.api.client.data.ListPartiesResult.ParticipantDomains
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.v30
import com.digitalasset.canton.topology.transaction.*

final case class ListPartiesResult(party: PartyId, participants: Seq[ParticipantDomains])

object ListPartiesResult {
  final case class DomainPermission(domain: DomainId, permission: ParticipantPermission)
  final case class ParticipantDomains(participant: ParticipantId, domains: Seq[DomainPermission])

  private def fromProtoV30(
      value: v30.ListPartiesResponse.Result.ParticipantDomains.DomainPermissions
  ): ParsingResult[DomainPermission] =
    for {
      domainId <- DomainId.fromProtoPrimitive(value.domain, "domain")
      permission <- ParticipantPermission.fromProtoV30(value.permission)
    } yield DomainPermission(domainId, permission)

  private def fromProtoV30(
      value: v30.ListPartiesResponse.Result.ParticipantDomains
  ): ParsingResult[ParticipantDomains] = {
    val participantIdNew = UniqueIdentifier
      .fromProtoPrimitive(value.participantUid, "participant_uid")
      .map(ParticipantId(_))

    // TODO(#16458) Remove this fallback which is used to allow 3.1 console
    // to talk to 3.0 nodes
    val participantIdOld = participantIdNew.orElse(
      ParticipantId.fromProtoPrimitive(value.participantUid, "participant_uid")
    )

    for {
      participantId <- participantIdNew.orElse(participantIdOld)

      domains <- value.domains.traverse(fromProtoV30)
    } yield ParticipantDomains(participantId, domains)
  }

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
