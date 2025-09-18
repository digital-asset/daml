// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.ledger.api.v2.value as v
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.http
import com.digitalasset.canton.http.json.JsonError
import com.digitalasset.canton.http.util.Logging.instanceUUIDLogCtx
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.jsonapi.AbstractHttpServiceIntegrationTestFuns.HttpServiceTestFixtureData
import com.digitalasset.canton.integration.tests.jsonapi.HttpServiceTestFixture.UseTls
import com.digitalasset.canton.ledger.api.refinements.ApiTypes as lar
import org.scalatest.Assertion
import scalaz.std.scalaFuture.*
import scalaz.syntax.apply.*
import scalaz.syntax.bifunctor.*
import scalaz.syntax.show.*
import scalaz.{EitherT, \/}
import spray.json.JsValue

import scala.concurrent.Future

/** Tests that are exercising features independently of both user authentication and the query
  * store.
  */
class SecureHttpServiceIntegrationTest
    extends AbstractHttpServiceIntegrationTest
    with AbstractHttpServiceIntegrationTestFunsUserToken {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  import com.digitalasset.canton.http.json.JsonProtocol.*

  override def useTls: UseTls = UseTls.Tls

  "should be able to serialize and deserialize synchronizer commands" in httpTestFixture {
    fixture =>
      (testCreateCommandEncodingDecoding(fixture) *>
        testExerciseCommandEncodingDecoding(fixture)): Future[Assertion]
  }

  private def testCreateCommandEncodingDecoding(
      fixture: HttpServiceTestFixtureData
  ): Future[Assertion] = instanceUUIDLogCtx { implicit lc =>
    import fixture.{uri, encoder, decoder}
    import com.digitalasset.canton.http.util.ErrorOps.*
    import com.daml.jwt.Jwt

    val command0: http.CreateCommand[v.Record, http.ContractTypeId.Template.RequiredPkg] =
      iouCreateCommand(http.Party("Alice"))

    type F[A] = EitherT[Future, JsonError, A]
    val x: F[Assertion] = for {
      jsVal <- EitherT.either(
        encoder.encodeCreateCommand(command0).liftErr(JsonError)
      ): F[JsValue]
      command1 <- (EitherT.rightT(fixture.jwt(uri)): F[Jwt])
        .flatMap(decoder.decodeCreateCommand(jsVal, _))
    } yield command1.bimap(removeRecordId, identity) should ===(command0)

    (x.run: Future[JsonError \/ Assertion]).map(_.fold(e => fail(e.shows), identity))
  }

  private def testExerciseCommandEncodingDecoding(
      fixture: HttpServiceTestFixtureData
  ): Future[Assertion] = {
    import fixture.{uri, encoder, decoder}
    val command0 = iouExerciseTransferCommand(lar.ContractId("#a-contract-ID"), http.Party("Bob"))
    val jsVal: JsValue = encodeExercise(encoder)(command0)
    val command1 =
      fixture.jwt(uri).flatMap(decodeExercise(decoder, _)(jsVal))
    command1.map(_.bimap(removeRecordId, identity) should ===(command0))
  }
}
