// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.digitalasset.ledger.test_stable.Test.AgreementFactory
import com.digitalasset.ledger.test_stable.Test.AgreementFactory._

import scala.concurrent.{ExecutionContext, Future}

object Synchronize {

  /**
    * Create a synchronization point between two participants by ensuring that a
    * contract with two distributed stakeholders both see an update on a shared contract.
    *
    * Useful to ensure two parties distributed across participants both view the
    * updates happened _BEFORE_ the call to this method.
    *
    * This allows us to check that an earlier update which is not to be seen on either
    * participant by parties distributed across them is actually not visible and not
    * a byproduct of interleaved distributed calls.
    *
    * FIXME This will _NOT_ work with distributed committers
    */
  final def synchronize(alpha: ParticipantTestContext, beta: ParticipantTestContext)(
      implicit ec: ExecutionContext,
  ): Future[Unit] = {
    for {
      alice <- alpha.allocateParty()
      bob <- beta.allocateParty()
      _ <- alpha.waitForParties(Set(beta), Set(alice, bob))
      _ <- beta.waitForParties(Set(alpha), Set(alice, bob))
      factory <- alpha.create(alice, AgreementFactory(bob, alice))
      agreement <- eventually { beta.exercise(bob, factory.exerciseCreateAgreement) }
      _ <- eventually { alpha.transactionTreeById(agreement.transactionId, alice) }
    } yield {
      // Nothing to do, by flatmapping over this we know
      // the two participants are synchronized up to the
      // point before invoking this method
    }
  }
}
