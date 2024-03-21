// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.io.File
import java.nio.file.Files

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, StatusCodes, Uri}
import com.daml.http.dbbackend.JdbcConfig
import com.daml.ledger.api.v1.{value => v}
import com.daml.lf.data.Ref
import com.daml.lf.value.test.TypedValueGenerators.{RNil, ValueAddend => VA}
import com.daml.scalautil.Statement.discard
import com.daml.http.util.TestUtil.writeToFile
import org.scalacheck.Gen
import org.scalatest.{Assertion, BeforeAndAfterAll}
import shapeless.record.{Record => ShRecord}
import spray.json.JsValue

import scala.concurrent.Future

abstract class HttpServiceIntegrationTest
    extends AbstractHttpServiceIntegrationTestTokenIndependent
    with BeforeAndAfterAll {
  import AbstractHttpServiceIntegrationTestFuns.ciouDar

  private val staticContent: String = "static"

  private val staticContentDir: File =
    Files.createTempDirectory("integration-test-static-content").toFile

  override def staticContentConfig: Option[StaticContentConfig] =
    Some(StaticContentConfig(prefix = staticContent, directory = staticContentDir))

  override def jdbcConfig: Option[JdbcConfig] = None

  override def wsConfig: Option[WebsocketConfig] = None

  private val expectedDummyContent: String = Gen
    .listOfN(100, Gen.identifier)
    .map(_.mkString(" "))
    .sample
    .getOrElse(throw new IllegalStateException(s"Cannot create dummy text content"))

  private val dummyFile: File =
    writeToFile(new File(staticContentDir, "dummy.txt"), expectedDummyContent).get
  require(dummyFile.exists)

  override protected def afterAll(): Unit = {
    // clean up temp directory
    discard { dummyFile.delete() }
    discard { staticContentDir.delete() }
    super.afterAll()
  }

  "should serve static content from configured directory" in withHttpService {
    (uri: Uri, _, _, _) =>
      Http()
        .singleRequest(
          HttpRequest(
            method = HttpMethods.GET,
            uri = uri.withPath(Uri.Path(s"/$staticContent/${dummyFile.getName}")),
          )
        )
        .flatMap { resp =>
          discard { resp.status shouldBe StatusCodes.OK }
          val bodyF: Future[String] = util.TestUtil.getResponseDataBytes(resp, debug = false)
          bodyF.flatMap { body =>
            body shouldBe expectedDummyContent
          }
        }: Future[Assertion]
  }

  // TODO(#13668) Redesign the test once the issue is fixed
  "pick up new package's inherited interfaces" ignore withHttpService { fixture =>
    import fixture.encoder
    import json.JsonProtocol._
    def createIouAndExerciseTransfer(
        initialTplId: domain.TemplateId.OptionalPkg,
        exerciseBy: domain.TemplateId.OptionalPkg,
    ) = for {
      aliceH <- fixture.getUniquePartyAndAuthHeaders("Alice")
      (alice, aliceHeaders) = aliceH
      createTest <- postCreateCommand(
        iouCommand(alice, initialTplId),
        fixture,
        aliceHeaders,
      )
      testIIouID = inside(createTest) { case (StatusCodes.OK, domain.OkResponse(result, _, _)) =>
        result.contractId
      }
      bobH <- fixture.getUniquePartyAndAuthHeaders("Bob")
      (bob, _) = bobH
      exerciseTest <- fixture
        .postJsonRequest(
          Uri.Path("/v1/exercise"),
          encodeExercise(encoder)(
            iouTransfer(domain.EnrichedContractId(Some(exerciseBy), testIIouID), bob)
          ),
          aliceHeaders,
        )
        .parseResponse[JsValue]
    } yield inside(exerciseTest) {
      case (StatusCodes.OK, domain.OkResponse(_, None, StatusCodes.OK)) => succeed
    }

    for {
      _ <- uploadPackage(fixture)(ciouDar)
      // first, use IIou only
      _ <- createIouAndExerciseTransfer(
        initialTplId = domain.TemplateId(None, "IIou", "TestIIou"),
        // whether we can exercise by interface-ID
        exerciseBy = TpId.IIou.IIou,
      )
      // ideally we would upload IIou.daml only above, then upload ciou here;
      // however tests currently don't play well with reload -SC
      // next, use CIou
      _ <- createIouAndExerciseTransfer(
        initialTplId = domain.TemplateId(None, "CIou", "CIou"),
        // whether we can exercise inherited by concrete template ID
        exerciseBy = domain.TemplateId(None, "CIou", "CIou"),
      )
    } yield succeed
  }

  "fail to exercise by key with interface ID" in withHttpService { fixture =>
    import fixture.encoder
    import json.JsonProtocol._
    for {
      _ <- uploadPackage(fixture)(ciouDar)
      aliceH <- fixture.getUniquePartyAndAuthHeaders("Alice")
      (alice, aliceHeaders) = aliceH
      createTest <- postCreateCommand(
        iouCommand(alice, domain.TemplateId(None, "CIou", "CIou")),
        fixture,
        aliceHeaders,
      )
      _ = createTest._1 should ===(StatusCodes.OK)
      bobH <- fixture.getUniquePartyAndAuthHeaders("Bob")
      (bob, _) = bobH
      exerciseTest <- fixture
        .postJsonRequest(
          Uri.Path("/v1/exercise"),
          encodeExercise(encoder)(
            iouTransfer(
              domain.EnrichedContractKey(
                TpId.IIou.IIou,
                v.Value(v.Value.Sum.Party(domain.Party unwrap alice)),
              ),
              bob,
            )
          ),
          aliceHeaders,
        )
        .parseResponse[JsValue]
    } yield inside(exerciseTest) {
      case (
            StatusCodes.BadRequest,
            domain.ErrorResponse(Seq(lookup), None, StatusCodes.BadRequest, _),
          ) =>
        lookup should include regex raw"Cannot resolve Template Key type, given: TemplateId\([0-9a-f]{64},IIou,IIou\)"
    }
  }

  private[this] val (_, ciouVA) = {
    import shapeless.syntax.singleton._
    val iouT = Symbol("issuer") ->> VA.party ::
      Symbol("owner") ->> VA.party ::
      Symbol("amount") ->> VA.text ::
      RNil
    VA.record(Ref.Identifier assertFromString "none:Iou:Iou", iouT)
  }

  private[this] def iouCommand(party: domain.Party, templateId: domain.TemplateId.OptionalPkg) = {
    val issuer = Ref.Party assertFromString domain.Party.unwrap(party)
    val iouT = argToApi(ciouVA)(
      ShRecord(
        issuer = issuer,
        owner = issuer,
        amount = "42",
      )
    )
    domain.CreateCommand(templateId, iouT, None)
  }

  private[this] def iouTransfer(
      locator: domain.ContractLocator[v.Value],
      to: domain.Party,
  ) = {
    val payload = recordFromFields(ShRecord(to = v.Value.Sum.Party(domain.Party unwrap to)))
    domain.ExerciseCommand(
      locator,
      domain.Choice("Transfer"),
      v.Value(v.Value.Sum.Record(payload)),
      None,
    )
  }
}

final class HttpServiceIntegrationTestCustomToken
    extends HttpServiceIntegrationTest
    with AbstractHttpServiceIntegrationTestFunsCustomToken
