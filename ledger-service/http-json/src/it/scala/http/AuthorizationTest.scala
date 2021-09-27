// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.nio.file.Files
import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.http.HttpServiceTestFixture.UseTls
import com.daml.http.util.TestUtil.requiredFile
import com.daml.http.util.Logging.instanceUUIDLogCtx
import com.daml.http.util.SandboxNextTestLedger
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.auth.{AuthServiceStatic, Claim, ClaimPublic, ClaimSet}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
import com.daml.ledger.resources.ResourceContext
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

final class AuthorizationTest
    extends AsyncFlatSpec
    with BeforeAndAfterAll
    with Matchers
    with SandboxNextTestLedger
    with SuiteResourceManagementAroundAll {

  private val dar = requiredFile(rlocation("docs/quickstart-model.dar"))
    .fold(e => throw new IllegalStateException(e), identity)

  protected val testId: String = this.getClass.getSimpleName
  override def useTls = UseTls.NoTls

  implicit val asys: ActorSystem = ActorSystem(testId)
  implicit val mat: Materializer = Materializer(asys)
  implicit val aesf: ExecutionSequencerFactory = new AkkaExecutionSequencerPool(testId)(asys)
  implicit val ec: ExecutionContext = asys.dispatcher
  implicit val resourceContext: ResourceContext = ResourceContext(ec)

  private val publicToken = "public"
  private val emptyToken = "empty"
  private val mockedAuthService = Option(AuthServiceStatic {
    case `publicToken` => ClaimSet.Claims(Seq[Claim](ClaimPublic))
    case `emptyToken` => ClaimSet.Unauthenticated
  })

  private val accessTokenFile = Files.createTempFile("Extractor", "AuthSpec")

  override def authService = mockedAuthService
  override def packageFiles = List(dar)

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
    usingLedger[A](testId, Some(publicToken)) { case (_, client, ledgerId) =>
      testFn(client)(ledgerId)
    }
  }

  private def packageService(client: DamlLedgerClient): PackageService =
    new PackageService(HttpService.doLoad(client.packageClient))

  behavior of "PackageService against an authenticated sandbox"

  it should "fail immediately if the authorization is insufficient" in withLedger {
    client => ledgerId =>
      instanceUUIDLogCtx(implicit lc =>
        packageService(client).reload(Jwt(emptyToken), ledgerId).failed.map(_ => succeed)
      )
  }

  it should "succeed if the authorization is sufficient" in withLedger { client => ledgerId =>
    instanceUUIDLogCtx(implicit lc =>
      packageService(client).reload(Jwt(publicToken), ledgerId).map(_ => succeed)
    )
  }

}
