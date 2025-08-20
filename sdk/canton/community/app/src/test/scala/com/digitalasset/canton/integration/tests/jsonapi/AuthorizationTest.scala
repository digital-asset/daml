// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.grpc.adapter.{ExecutionSequencerFactory, PekkoExecutionSequencerPool}
import com.daml.jwt.Jwt
import com.daml.logging.LoggingContextOf
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authorization
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest}
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.http.json.v1.{LedgerReader, PackageService, V1Routes}
import com.digitalasset.canton.http.util.Logging.{InstanceUUID, instanceUUIDLogCtx}
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.ledger.client.LedgerClient as DamlLedgerClient
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.daml.lf.data.Ref
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.slf4j.event.Level

import scala.concurrent.{ExecutionContext, Future}

final class AuthorizationTest extends HttpJsonApiTestBase {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  private val testId: String = this.getClass.getSimpleName
  private implicit val asys: ActorSystem = ActorSystem(testId)
  private implicit val mat: Materializer = Materializer(asys)
  private implicit val aesf: ExecutionSequencerFactory =
    new PekkoExecutionSequencerPool(testId)(asys)
  private implicit val ec: ExecutionContext = asys.dispatcher

  private val emptyJWTToken = getToken(Ref.UserId.assertFromString("empty"), authSecret)

  private val authorizationSecurity: SecurityTest =
    SecurityTest(property = Authorization, asset = "HTTP JSON API Service")

  override def afterAll(): Unit = {
    aesf.close()
    mat.shutdown()
    asys.terminate().futureValue
    super.afterAll()
  }

  override def authSecret: Option[String] = Some("secret")

  protected def withLedger[A](
      testFn: DamlLedgerClient => Future[A]
  ): FixtureParam => A =
    usingLedger[A](Some(toHeader(adminToken))) { case (_, client) =>
      testFn(client).futureValue
    }

  private def packageService(
      client: DamlLedgerClient
  )(implicit lc: LoggingContextOf[InstanceUUID]): PackageService = {
    val loadCache = LedgerReader.LoadCache.freshCache()
    new PackageService(
      reloadPackageStoreIfChanged =
        V1Routes.doLoad(client.packageService, LedgerReader(loggerFactory), loadCache),
      loggerFactory = loggerFactory,
    )
  }

  val AuthInterceptorSuppressionRule: SuppressionRule =
    SuppressionRule.LoggerNameContains("AuthInterceptor") &&
      SuppressionRule.Level(Level.WARN)

  "PackageService against an authenticated sandbox" should {
    "fail updating the package service immediately with insufficient authorization" taggedAs authorizationSecurity
      .setAttack(
        Attack(
          "Ledger client",
          "does not provide an auth token",
          "refuse updating the package service with a failure",
        )
      ) in withLedger { client =>
      loggerFactory.suppress(AuthInterceptorSuppressionRule) {
        instanceUUIDLogCtx(implicit lc =>
          packageService(client).reload(Jwt(emptyJWTToken.value)).failed.map(_ => succeed)
        )
      }
    }

    "succeed updating the package service with sufficient authorization" taggedAs authorizationSecurity
      .setHappyCase(
        "A ledger client can update the package service when authorized"
      ) in withLedger { client =>
      instanceUUIDLogCtx(implicit lc =>
        packageService(client)
          .reload(
            Jwt(toHeader(adminToken, authSecret.getOrElse(jwtSecret.unwrap)))
          )
          .map(_ => succeed)
      )
    }
  }
}
