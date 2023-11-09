// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import com.daml.grpc.adapter.{PekkoExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.http.HttpServiceTestFixture.UseTls
import com.daml.http.util.Logging.{InstanceUUID, instanceUUIDLogCtx}
import com.daml.http.util.SandboxTestLedger
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContextOf
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authorization
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.OptionValues._

import scala.concurrent.{ExecutionContext, Future}

class AuthorizationTest
    extends AsyncFlatSpec
    with BeforeAndAfterAll
    with Matchers
    with SandboxTestLedger
    with SuiteResourceManagementAroundAll {

  protected val testId: String = this.getClass.getSimpleName
  override def useTls = UseTls.NoTls
  override lazy protected val authSecret: Option[String] = Some("secret")

  implicit val asys: ActorSystem = ActorSystem(testId)
  implicit val mat: Materializer = Materializer(asys)
  implicit val aesf: ExecutionSequencerFactory = new PekkoExecutionSequencerPool(testId)(asys)
  implicit val ec: ExecutionContext = asys.dispatcher

  private val emptyJWTToken = config.getToken(Ref.UserId.assertFromString("empty"))

  private val authorizationSecurity: SecurityTest =
    SecurityTest(property = Authorization, asset = "HTTP JSON API Service")

  override def packageFiles = List()

  protected def withLedger[A](testFn: DamlLedgerClient => LedgerId => Future[A]): Future[A] = {
    usingLedger[A](config.adminToken) { case (_, client, ledgerId) =>
      testFn(client)(ledgerId)
    }
  }

  private def packageService(
      client: DamlLedgerClient
  )(implicit lc: LoggingContextOf[InstanceUUID]): PackageService = {
    val loadCache = com.daml.ledger.service.LedgerReader.LoadCache.freshCache()
    new PackageService(HttpService.doLoad(client.packageClient, loadCache))
  }

  behavior of "PackageService against an authenticated sandbox"

  it should "fail updating the package service immediately with insufficient authorization" taggedAs authorizationSecurity
    .setAttack(
      Attack(
        "Ledger client",
        "does not provide an auth token",
        "refuse updating the package service with a failure",
      )
    ) in withLedger { client => ledgerId =>
    instanceUUIDLogCtx(implicit lc =>
      packageService(client).reload(Jwt(emptyJWTToken.value), ledgerId).failed.map(_ => succeed)
    )
  }

  it should "succeed updating the package service with sufficient authorization" taggedAs authorizationSecurity
    .setHappyCase(
      "A ledger client can update the package service when authorized"
    ) in withLedger { client => ledgerId =>
    instanceUUIDLogCtx(implicit lc =>
      packageService(client).reload(Jwt(config.adminToken.value), ledgerId).map(_ => succeed)
    )
  }

}
