// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.javaapi.data.Party
import com.daml.ledger.javaapi.data.codegen.ContractId

import scala.concurrent.{ExecutionContext, Future}

object Synchronize {

  /** Create a synchronization point between two participants by ensuring that a
    * party created on one participant is visible on the other one.
    *
    * Useful to ensure two parties distributed across participants both view the
    * updates happened _BEFORE_ the call to this method.
    *
    * This allows us to check that an earlier update which is not to be seen on either
    * participant by parties distributed across them is actually not visible and not
    * a byproduct of interleaved distributed calls.
    */
  final def synchronize(alpha: ParticipantTestContext, beta: ParticipantTestContext)(implicit
      ec: ExecutionContext
  ): Future[Unit] =
    for {
      alice <- alpha.allocateParty()
      bob <- beta.allocateParty()
      _ <- alpha.waitForParties(Set(beta), Set(alice, bob))
      _ <- beta.waitForParties(Set(alpha), Set(alice, bob))
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
      participant.activeContracts(party).map { events =>
        assert(
          events.exists(_.contractId == contractId.contractId),
          s"Could not find contract $contractId",
        )
      }
    }

}
