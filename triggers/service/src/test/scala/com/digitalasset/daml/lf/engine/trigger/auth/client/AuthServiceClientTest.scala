// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.auth.client

import akka.actor.ActorSystem
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.concurrent.ExecutionContext

class AuthServiceClientTest extends AsyncFlatSpec with Eventually with Matchers with ScalaFutures {

  def testId: String = this.getClass.getSimpleName
  implicit val system: ActorSystem = ActorSystem(testId)
  implicit val esf: ExecutionSequencerFactory = new AkkaExecutionSequencerPool(testId)(system)
  implicit val ec: ExecutionContext = system.dispatcher

  private val testLedgerId = "test-ledger-id"

  it should "complete auth flow from service account to ledger access token" in
    AuthServiceFixture.withAuthServiceClient(testId) { authServiceClient =>
      for {
        authServiceToken <- authServiceClient.authorize("username", "password")
        _ <- authServiceToken.token should not be empty
        () <- authServiceClient.requestServiceAccount(authServiceToken, testLedgerId)
        sa <- authServiceClient.getServiceAccount(authServiceToken)
        _ <- sa.serviceAccount should not be empty
        _ <- sa.creds should equal(List())
        credId <- authServiceClient.getNewCredentialId(authServiceToken, sa.serviceAccount)
        _ <- credId.credId should not be empty
        cred <- authServiceClient.getCredential(authServiceToken, credId)
        _ <- cred.cred should not be empty
        _ <- cred.credId should equal(credId.credId)
        ledgerAccessToken <- authServiceClient.login(cred)
        _ <- ledgerAccessToken.token should not be empty
      } yield succeed
    }

  it should "fail to get service account without a request" in
    AuthServiceFixture.withAuthServiceClient(testId) { authServiceClient =>
      for {
        authServiceToken <- authServiceClient.authorize("username", "password")
        _ <- authServiceToken.token should not be empty
        getSa = authServiceClient.getServiceAccount(authServiceToken)
        error <- getSa.failed
        _ <- assert(error.isInstanceOf[NoSuchElementException])
      } yield succeed
    }
}
