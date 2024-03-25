// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.syntax.foldable.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{HashPurpose, SyncCryptoApiProvider}
import com.digitalasset.canton.protocol.{LfGlobalKey, LfGlobalKeyWithMaintainers, TransactionId}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot

import scala.concurrent.{ExecutionContext, Future}

package object repair {

  /** Cooks up a random dummy transaction id.
    *
    * With single-participant repair commands, we have little hope of coming up with a transactionId that matches up with
    * other participants. We can get away with differing transaction ids across participants because the
    * AcsCommitmentProcessor does not compare transaction ids.
    */
  private[repair] def randomTransactionId(syncCrypto: SyncCryptoApiProvider) = {
    // We take as much entropy as for a random UUID.
    // This should be enough to guard against clashes between the repair requests executed on a single participant.
    // We don't have to worry about clashes with ordinary transaction IDs as the hash purpose is different.
    val randomness = syncCrypto.pureCrypto.generateRandomByteString(16)
    val hash = syncCrypto.pureCrypto.digest(HashPurpose.RepairTransactionId, randomness)
    TransactionId(hash)
  }

  private[repair] def hostsParty(snapshot: TopologySnapshot, participantId: ParticipantId)(
      party: LfPartyId
  )(implicit executionContext: ExecutionContext): Future[Boolean] =
    snapshot.hostedOn(party, participantId).map(_.exists(_.permission.isActive))

  /*
    Return the key if one of the two conditions is true
      - the maintainer of the key is listed in `hostedPartiesO`
      - the topology snapshot `snapshot` contains an active participant hosting the maintainer
   */
  private[repair] def getKeyIfOneMaintainerIsLocal(
      snapshot: TopologySnapshot,
      participantId: ParticipantId,
      hostedPartiesO: Option[NonEmpty[Set[LfPartyId]]],
      keyO: Option[LfGlobalKeyWithMaintainers],
  )(implicit executionContext: ExecutionContext): Future[Option[LfGlobalKey]] = {
    keyO.collect { case LfGlobalKeyWithMaintainers(key, maintainers) =>
      (maintainers, key)
    } match {
      case None => Future.successful(None)
      case Some((maintainers, key)) =>
        def partyToParticipantExists(): Future[Boolean] =
          maintainers.toList.findM(hostsParty(snapshot, participantId)).map(_.isDefined)

        val isHostedF: Future[Boolean] = hostedPartiesO match {
          case Some(hostedParties) =>
            if (maintainers.toList.exists(hostedParties.contains))
              Future.successful(true)
            else partyToParticipantExists()

          case None =>
            partyToParticipantExists()
        }

        isHostedF.map(Option.when(_)(key))
    }
  }

}
