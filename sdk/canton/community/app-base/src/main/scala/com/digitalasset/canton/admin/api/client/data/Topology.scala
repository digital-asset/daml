// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.traverse.*
import com.digitalasset.canton.admin.api.client.data.ListPartiesResult.ParticipantSynchronizers
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.v30
import com.digitalasset.canton.topology.transaction.*

final case class ListPartiesResult(party: PartyId, participants: Seq[ParticipantSynchronizers])

object ListPartiesResult {
  final case class SynchronizerPermission(
      synchronizerId: SynchronizerId,
      permission: ParticipantPermission,
  )
  final case class ParticipantSynchronizers(
      participant: ParticipantId,
      synchronizers: Seq[SynchronizerPermission],
  )

  private def fromProtoV30(
      valueP: v30.ListPartiesResponse.Result.ParticipantSynchronizers.SynchronizerPermissions
  ): ParsingResult[SynchronizerPermission] =
    for {
      synchronizerId <- SynchronizerId.fromProtoPrimitive(valueP.synchronizerId, "synchronizer_id")
      permission <- ParticipantPermission.fromProtoV30(valueP.permission)
    } yield SynchronizerPermission(synchronizerId, permission)

  private def fromProtoV30(
      value: v30.ListPartiesResponse.Result.ParticipantSynchronizers
  ): ParsingResult[ParticipantSynchronizers] = {
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

      synchronizers <- value.synchronizers.traverse(fromProtoV30)
    } yield ParticipantSynchronizers(participantId, synchronizers)
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
    store: SynchronizerId,
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
      synchronizerId <- SynchronizerId.fromProtoPrimitive(value.synchronizerId, "synchronizer_id")
      owner <- Member.fromProtoPrimitive(value.keyOwner, "keyOwner")
      signingKeys <- value.signingKeys.traverse(SigningPublicKey.fromProtoV30)
      encryptionKeys <- value.encryptionKeys.traverse(EncryptionPublicKey.fromProtoV30)
    } yield ListKeyOwnersResult(synchronizerId, owner, signingKeys, encryptionKeys)
}
