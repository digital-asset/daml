// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.nio.file.Files
import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.auth.TokenHolder
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.http.util.TestUtil.requiredFile
import com.daml.http.util.Logging.correlationIdLogCtx
import com.daml.ledger.api.auth.{AuthServiceStatic, Claim, ClaimPublic, ClaimSet}
import com.daml.ledger.client.LedgerClient
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

final class AuthorizationTest extends AsyncFlatSpec with BeforeAndAfterAll with Matchers {

  private val dar = requiredFile(rlocation("docs/quickstart-model.dar"))
    .fold(e => throw new IllegalStateException(e), identity)

  private val testId: String = this.getClass.getSimpleName

  implicit val asys: ActorSystem = ActorSystem(testId)
  implicit val mat: Materializer = Materializer(asys)
  implicit val aesf: ExecutionSequencerFactory = new AkkaExecutionSequencerPool(testId)(asys)
  implicit val ec: ExecutionContext = asys.dispatcher

  private val publicToken = "public"
  private val emptyToken = "empty"
  private val mockedAuthService = Option(AuthServiceStatic {
    case `publicToken` => ClaimSet.Claims(Seq[Claim](ClaimPublic))
    case `emptyToken` => ClaimSet.Unauthenticated
  })

  private val accessTokenFile = Files.createTempFile("Extractor", "AuthSpec")
  private val tokenHolder = Option(new TokenHolder(accessTokenFile))

  private def setToken(string: String): Unit = {
    val _ = Files.write(accessTokenFile, string.getBytes())
  }

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

  protected def withLedger[A](testFn: LedgerClient => Future[A]): Future[A] =
    HttpServiceTestFixture
      .withLedger[A](List(dar), testId, Option(publicToken), authService = mockedAuthService) {
        case (_, client) => testFn(client)
      }

  private def packageService(client: LedgerClient): PackageService =
    new PackageService(HttpService.loadPackageStoreUpdates(client.packageClient, tokenHolder))

  behavior of "PackageService against an authenticated sandbox"

  it should "fail immediately if the authorization is insufficient" in withLedger { client =>
    setToken(emptyToken)
    correlationIdLogCtx(implicit lc => packageService(client).reload.failed.map(_ => succeed))
  }

  it should "succeed if the authorization is sufficient" in withLedger { client =>
    setToken(publicToken)
    correlationIdLogCtx(implicit lc => packageService(client).reload.map(_ => succeed))
  }

}
