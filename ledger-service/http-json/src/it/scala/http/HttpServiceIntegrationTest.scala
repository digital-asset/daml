// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.io.File
import java.nio.file.Files

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, StatusCodes, Uri}
import com.daml.bazeltools.BazelRunfiles.requiredResource
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

object HttpServiceIntegrationTest {

  // private val iiouDar = requiredResource("ledger-service/http-json/IIou.dar")
  private val ciouDar = requiredResource("ledger-service/http-json/CIou.dar")
}

abstract class HttpServiceIntegrationTest
    extends AbstractHttpServiceIntegrationTestTokenIndependent
    with BeforeAndAfterAll {
  import HttpServiceIntegrationTest._

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
          val bodyF: Future[String] = getResponseDataBytes(resp, debug = false)
          bodyF.flatMap { body =>
            body shouldBe expectedDummyContent
          }
        }: Future[Assertion]
  }

  "pick up new package's inherited interfaces" in withHttpService { (uri, encoder, _, _) =>
    import json.JsonProtocol._
    def createIouAndExerciseTransfer(
        initialTplId: domain.TemplateId.OptionalPkg,
        exerciseBy: domain.TemplateId.OptionalPkg,
    ) = for {
      aliceH <- getUniquePartyAndAuthHeaders(uri)("Alice")
      (alice, aliceHeaders) = aliceH
      createTest <- postCreateCommand(
        iouCommand(alice, initialTplId),
        encoder,
        uri,
        aliceHeaders,
      )
      testIIouID = {
        discard { createTest._1 should ===(StatusCodes.OK) }
        createTest._2.convertTo[domain.OkResponse[domain.ActiveContract[JsValue]]].result.contractId
      }
      bobH <- getUniquePartyAndAuthHeaders(uri)("Bob")
      (bob, bobHeaders) = bobH
      exerciseTest <- postJsonRequest(
        uri withPath Uri.Path("/v1/exercise"),
        encodeExercise(encoder)(
          iouTransfer(testIIouID, bob, exerciseBy)
        ),
        aliceHeaders,
      )
    } yield exerciseTest._1 should ===(StatusCodes.OK)

    for {
      // _ <- uploadPackage(uri)(iiouDar)
      _ <- uploadPackage(uri)(ciouDar)
      // first, use IIou only
      _ <- createIouAndExerciseTransfer(
        initialTplId = domain.TemplateId(None, "IIou", "TestIIou"),
        // whether we can exercise by interface-ID
        exerciseBy = domain.TemplateId(None, "IIou", "IIou"),
      )
      // next, use CIou
      _ <- createIouAndExerciseTransfer(
        initialTplId = domain.TemplateId(None, "CIou", "CIou"),
        // whether we can exercise inherited by concrete template ID
        exerciseBy = domain.TemplateId(None, "CIou", "CIou"),
      )
    } yield succeed
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
      cid: domain.ContractId,
      to: domain.Party,
      templateId: domain.TemplateId.OptionalPkg,
  ) = {
    val payload = recordFromFields(ShRecord(to = v.Value.Sum.Party(domain.Party unwrap to)))
    domain.ExerciseCommand(
      domain.EnrichedContractId(Some(templateId), cid),
      domain.Choice("Transfer"),
      v.Value(v.Value.Sum.Record(payload)),
      None,
    )
  }
}

final class HttpServiceIntegrationTestCustomToken
    extends HttpServiceIntegrationTest
    with AbstractHttpServiceIntegrationTestFunsCustomToken
