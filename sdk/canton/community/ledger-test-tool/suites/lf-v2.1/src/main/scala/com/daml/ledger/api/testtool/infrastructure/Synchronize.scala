// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.Party
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.javaapi.data.codegen.{ContractCompanion, ContractId}
import com.daml.ledger.test.java.model.test.Sync

import scala.concurrent.{ExecutionContext, Future}

object Synchronize {

  implicit val syncCompanion: ContractCompanion.WithoutKey[Sync.Contract, Sync.ContractId, Sync] =
    Sync.COMPANION

  /** Creates a synchronization point between two participants by
    *   - ensuring that a party created on each participant is visible on the other one,
    *   - ensuring those parties are each connected to the same number of synchronizers,
    *   - ensuring that a contract created on each participant is visible on the other one.
    *
    * Useful to ensure two parties distributed across participants both view the updates happened
    * _BEFORE_ the call to this method.
    *
    * This allows us to check that an earlier update which is not to be seen on either participant
    * by parties distributed across them is actually not visible and not a byproduct of interleaved
    * distributed calls.
    */
  final def synchronize(
      alpha: ParticipantTestContext,
      beta: ParticipantTestContext,
      connectedSynchronizers: Int,
  )(implicit
      ec: ExecutionContext
  ): Future[Unit] =
    for {
      alice <- alpha.allocateParty()
      bob <- beta.allocateParty()
      _ <- alpha.waitForPartiesOnOtherParticipants(Set(beta), Set(alice), connectedSynchronizers)
      _ <- beta.waitForPartiesOnOtherParticipants(Set(alpha), Set(bob), connectedSynchronizers)
      _ <- alpha.create(alice, new Sync(alice, bob)).flatMap(waitForContract(beta, alice, _))
      _ <- beta.create(bob, new Sync(bob, alice)).flatMap(waitForContract(alpha, alice, _))
    } yield {
      // Nothing to do, by flatmapping over this we know
      // the two participants are synchronized up to the
      // point before invoking this method
    }

  final def waitForContract[TCid <: ContractId[?]](
      participant: ParticipantTestContext,
      party: Party,
      contractId: TCid,
  )(implicit ec: ExecutionContext): Future[Unit] =
    eventually("Wait for contract to become active") {
      participant.activeContracts(Some(Seq(party))).map { events =>
        assert(
          events.exists(_.contractId == contractId.contractId),
          s"Could not find contract $contractId",
        )
      }
    }

}
