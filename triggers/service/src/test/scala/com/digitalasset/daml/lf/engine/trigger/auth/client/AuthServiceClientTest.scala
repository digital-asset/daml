// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.auth.client

import akka.actor.ActorSystem
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import org.scalatest.concurrent.Eventually
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.concurrent.ExecutionContext

class AuthServiceClientTest extends AsyncFlatSpec with Eventually with Matchers {

  def testId: String = this.getClass.getSimpleName
  implicit val system: ActorSystem = ActorSystem(testId)
  implicit val esf: ExecutionSequencerFactory = new AkkaExecutionSequencerPool(testId)(system)
  implicit val ec: ExecutionContext = system.dispatcher

  private val testLedgerId = "test-ledger-id"

  it should "authorize a user and request a service account" in
    AuthServiceFixture.withAuthServiceClient(testId) { authServiceClient =>
      for {
        authServiceToken <- authServiceClient.authorize("username", "password")
        _ <- authServiceToken.token should not be empty
        success <- authServiceClient.requestServiceAccount(authServiceToken, testLedgerId)
      } yield assert(success)
    }

  it should "get a service account and a new credential" in
    AuthServiceFixture.withAuthServiceClient(testId) { authServiceClient =>
      for {
        authServiceToken <- authServiceClient.authorize("username", "password")
        _ <- authServiceToken.token should not be empty
        saReqSuccess <- authServiceClient.requestServiceAccount(authServiceToken, testLedgerId)
        _ <- assert(saReqSuccess)
        Some(sa) <- authServiceClient.getServiceAccount(authServiceToken)
        _ <- sa.serviceAccount should not be empty
        _ <- sa.creds should equal(List())
        Some(credId) <- authServiceClient.getNewCredentialId(authServiceToken, sa.serviceAccount)
        _ <- credId.credId should not be empty
      } yield succeed
    }
}
