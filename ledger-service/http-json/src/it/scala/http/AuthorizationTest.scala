// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.http.HttpServiceTestFixture.UseTls
import com.daml.http.util.Logging.instanceUUIDLogCtx
import com.daml.http.util.SandboxTestLedger
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.auth.{AuthServiceStatic, Claim, ClaimPublic, ClaimSet}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authorization
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import java.nio.file.Files
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

final class AuthorizationTest
    extends AsyncFlatSpec
    with BeforeAndAfterAll
    with Matchers
    with SandboxTestLedger
    with SuiteResourceManagementAroundAll {

  protected val testId: String = this.getClass.getSimpleName
  override def useTls = UseTls.NoTls

  implicit val asys: ActorSystem = ActorSystem(testId)
  implicit val mat: Materializer = Materializer(asys)
  implicit val aesf: ExecutionSequencerFactory = new AkkaExecutionSequencerPool(testId)(asys)
  implicit val ec: ExecutionContext = asys.dispatcher

  private val publicTokenValue = "public"
  private val emptyTokenValue = "empty"

  private val mockedAuthService = Option(AuthServiceStatic {
    case `publicTokenValue` => ClaimSet.Claims.Empty.copy(claims = Seq[Claim](ClaimPublic))
    case `emptyTokenValue` => ClaimSet.Unauthenticated
  })

  private val accessTokenFile = Files.createTempFile("Extractor", "AuthSpec")

  private val authorizationSecurity: SecurityTest =
    SecurityTest(property = Authorization, asset = "HTTP JSON API Service")

  override def authService = mockedAuthService
  override def packageFiles = List()

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      Files.delete(accessTokenFile)
    } catch {
      case NonFatal(e) =>
        LoggerFactory
          .getLogger(classOf[AuthorizationTest])
          .warn("Unable to delete temporary token file", e)
    }
  }

  protected def withLedger[A](testFn: DamlLedgerClient => LedgerId => Future[A]): Future[A] = {
    usingLedger[A](testId, Some(publicTokenValue)) { case (_, client, ledgerId) =>
      testFn(client)(ledgerId)
    }
  }

  private def packageService(client: DamlLedgerClient): PackageService =
    new PackageService(HttpService.doLoad(client.packageClient))

  behavior of "PackageService against an authenticated sandbox"

  it should "fail updating the package service immediately with insufficient authorization" taggedAs authorizationSecurity.setAttack(
    Attack("Ledger client", "does not provide an auth token", "refuse updating the package service with a failure")
  ) in withLedger {
    client => ledgerId =>
      instanceUUIDLogCtx(implicit lc =>
        packageService(client).reload(Jwt(emptyTokenValue), ledgerId).failed.map(_ => succeed)
      )
  }

  it should "succeed updating the package service with sufficient authorization" taggedAs authorizationSecurity.setHappyCase(
    "A ledger client can update the package service when authorized"
  ) in withLedger {
    client => ledgerId =>
      instanceUUIDLogCtx(implicit lc =>
        packageService(client).reload(Jwt(publicTokenValue), ledgerId).map(_ => succeed)
      )
  }

}
