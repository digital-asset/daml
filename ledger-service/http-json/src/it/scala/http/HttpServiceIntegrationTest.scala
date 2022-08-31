// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.io.File
import java.nio.file.Files

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, StatusCode, StatusCodes, Uri}
import com.daml.http.dbbackend.JdbcConfig
import com.daml.ledger.api.v1.{value => v}
import com.daml.lf.data.Ref
import com.daml.lf.value.test.TypedValueGenerators.{ValueAddend => VA}
import com.daml.scalautil.Statement.discard
import json.SprayJson.{decode => jdecode}
import com.daml.http.util.TestUtil.writeToFile
import org.scalacheck.Gen
import org.scalatest.{Assertion, BeforeAndAfterAll}
import scalaz.\/-
import shapeless.record.{Record => ShRecord}
import spray.json.JsValue
import spray.json._

import scala.concurrent.Future

abstract class HttpServiceIntegrationTest
    extends AbstractHttpServiceIntegrationTestTokenIndependent
    with BeforeAndAfterAll {
  import HttpServiceIntegrationTest._
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

  // nested like this similar to TpId so the references look nicer
  object CIou {
    val CIou: domain.TemplateId.OptionalPkg = domain.TemplateId(None, "CIou", "CIou")
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

  "exercise interface choices" - {
    import json.JsonProtocol._
    import AbstractHttpServiceIntegrationTestFuns.{UriFixture, EncoderFixture}

    def createIouAndExerciseTransfer(
        fixture: UriFixture with EncoderFixture,
        initialTplId: domain.TemplateId.OptionalPkg,
        exerciseTid: domain.TemplateId.OptionalPkg,
        choice: TExercise[_] = tExercise(choiceArgType = echoTextVA)(echoTextSample),
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
      exerciseTest <- fixture
        .postJsonRequest(
          Uri.Path("/v1/exercise"),
          encodeExercise(fixture.encoder)(
            iouTransfer(
              domain.EnrichedContractId(Some(exerciseTid), testIIouID),
              choice,
            )
          ),
          aliceHeaders,
        )
        .parseResponse[domain.ExerciseResponse[JsValue]]
    } yield exerciseTest

    def exerciseSucceeded[A](
        exerciseTest: (StatusCode, domain.SyncResponse[domain.ExerciseResponse[JsValue]])
    ) =
      inside(exerciseTest) { case (StatusCodes.OK, domain.OkResponse(er, None, StatusCodes.OK)) =>
        inside(jdecode[String](er.exerciseResult)) { case \/-(decoded) => decoded }
      }
    object Transferrable {
      val Transferrable: domain.ContractTypeId.Interface.OptionalPkg =
        domain.ContractTypeId.Interface(None, "Transferrable", "Transferrable")
    }

    "templateId = interface ID" in withHttpService { fixture =>
      for {
        _ <- uploadPackage(fixture)(ciouDar)
        result <- createIouAndExerciseTransfer(
          fixture,
          initialTplId = domain.TemplateId(None, "IIou", "TestIIou"),
          // whether we can exercise by interface-ID
          exerciseTid = TpId.IIou.IIou,
        ) map exerciseSucceeded
      } yield result should ===("Bob invoked IIou.Transfer")
    }

    // ideally we would upload IIou.daml, then force a reload, then upload ciou;
    // however tests currently don't play well with reload -SC
    "templateId = template ID" in withHttpService { fixture =>
      for {
        _ <- uploadPackage(fixture)(ciouDar)
        result <- createIouAndExerciseTransfer(
          fixture,
          initialTplId = CIou.CIou,
          // whether we can exercise inherited by concrete template ID
          exerciseTid = CIou.CIou,
        ) map exerciseSucceeded
      } yield result should ===("Bob invoked IIou.Transfer")
    }

    "templateId = template ID, choiceInterfaceId = interface ID" in withHttpService { fixture =>
      for {
        _ <- uploadPackage(fixture)(ciouDar)
        result <- createIouAndExerciseTransfer(
          fixture,
          initialTplId = CIou.CIou,
          exerciseTid = CIou.CIou,
          choice = tExercise(choiceInterfaceId = Some(TpId.IIou.IIou), choiceArgType = echoTextVA)(
            echoTextSample
          ),
        ) map exerciseSucceeded
      } yield result should ===("Bob invoked IIou.Transfer")
    }

    "templateId = template, no choiceInterfaceId, picks template Overridden" in withHttpService {
      fixture =>
        for {
          _ <- uploadPackage(fixture)(ciouDar)
          result <- createIouAndExerciseTransfer(
            fixture,
            initialTplId = CIou.CIou,
            exerciseTid = CIou.CIou,
            choice = tExercise(choiceName = "Overridden", choiceArgType = echoTextPairVA)(
              ShRecord(echo = ShRecord(_1 = "yes", _2 = "no"))
            ),
          ) map exerciseSucceeded
        } yield result should ===("(\"yes\",\"no\") invoked CIou.Overridden")
    }

    "templateId = template, choiceInterfaceId = interface, picks interface Overridden" in withHttpService {
      fixture =>
        for {
          _ <- uploadPackage(fixture)(ciouDar)
          result <- createIouAndExerciseTransfer(
            fixture,
            initialTplId = CIou.CIou,
            exerciseTid = CIou.CIou,
            choice = tExercise(Some(Transferrable.Transferrable), "Overridden", echoTextVA)(
              ShRecord(echo = "yesyes")
            ),
          ) map exerciseSucceeded
        } yield result should ===("yesyes invoked Transferrable.Overridden")
    }

    "templateId = template, no choiceInterfaceId, ambiguous" in withHttpService { fixture =>
      for {
        _ <- uploadPackage(fixture)(ciouDar)
        response <- createIouAndExerciseTransfer(
          fixture,
          initialTplId = CIou.CIou,
          exerciseTid = CIou.CIou,
          choice = tExercise(choiceName = "Ambiguous", choiceArgType = echoTextVA)(
            ShRecord(echo = "ambiguous-test")
          ),
        )
      } yield inside(response) {
        case (
              StatusCodes.BadRequest,
              domain.ErrorResponse(Seq(onlyError), None, StatusCodes.BadRequest, None),
            ) =>
          (onlyError should include regex
            raw"Cannot resolve Choice Argument type, given: \(TemplateId\([0-9a-f]{64},CIou,CIou\), Ambiguous\)")
      }
    }

    "templateId = template ID, retroactive implements choice" in withHttpService { fixture =>
      for {
        _ <- uploadPackage(fixture)(ciouDar)
        result <- createIouAndExerciseTransfer(
          fixture,
          initialTplId = CIou.CIou,
          exerciseTid = CIou.CIou,
          choice = tExercise(choiceName = "TransferPlease", choiceArgType = echoTextVA)(
            echoTextSample
          ),
        ) map exerciseSucceeded
      } yield result should ===("Bob invoked RIIou.TransferPlease")
    }
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
      exerciseTest <- fixture
        .postJsonRequest(
          Uri.Path("/v1/exercise"),
          encodeExercise(encoder)(
            iouTransfer(
              domain.EnrichedContractKey(
                TpId.unsafeCoerce[domain.ContractTypeId.Template, Option[String]](TpId.IIou.IIou),
                v.Value(v.Value.Sum.Party(domain.Party unwrap alice)),
              ),
              tExercise()(ShRecord(echo = "bob")),
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
        lookup should include regex raw"Cannot resolve Template Key type, given: InterfaceId\([0-9a-f]{64},IIou,IIou\)"
    }
  }

  "query POST with empty query" - {
    "with an interface ID" in withHttpService { fixture =>
      import com.daml.http.json.JsonProtocol._
      for {
        _ <- uploadPackage(fixture)(ciouDar)
        aliceH <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (alice, aliceHeaders) = aliceH
        _ <- postCreateCommand(
          iouCommand(alice, CIou.CIou),
          fixture,
          aliceHeaders,
        )

        searchResp <- search(
          List.empty,
          Map(
            "templateIds" -> Seq(TpId.IIou.IIou).toJson,
            "query" -> spray.json.JsObject(),
          ).toJson.asJsObject,
          fixture,
          aliceHeaders,
        )
      } yield inside(searchResp) {
        case domain.OkResponse(acl, None, StatusCodes.OK) => {
          discard { acl.size shouldBe 1 }
          discard { acl.head.templateId shouldBe TpId.IIou.IIou.copy(packageId = acl.head.templateId.packageId) }
          acl.head.payload shouldBe spray.json.JsObject()
        }
      }
    }
  }


  private[this] val (_, ciouVA) = {
    val iouT = ShRecord(issuer = VA.party, owner = VA.party, amount = VA.text)
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

  private[this] def iouTransfer[Inj](
      locator: domain.ContractLocator[v.Value],
      choice: TExercise[Inj],
  ) = {
    import choice.{choiceInterfaceId, choiceName, choiceArgType, choiceArg}
    val payload = argToApi(choiceArgType)(choiceArg)
    domain.ExerciseCommand(
      locator,
      domain.Choice(choiceName),
      v.Value(v.Value.Sum.Record(payload)),
      choiceInterfaceId,
      None,
    )
  }
}

object HttpServiceIntegrationTest {
  private[this] val irrelevant = Ref.Identifier assertFromString "none:Discarded:Identifier"

  private val (_, echoTextVA) =
    VA.record(irrelevant, ShRecord(echo = VA.text))

  private val (_, echoTextPairVA) =
    VA.record(
      irrelevant,
      ShRecord(echo = VA.record(irrelevant, ShRecord(_1 = VA.text, _2 = VA.text))._2),
    )

  private val echoTextSample: echoTextVA.Inj = ShRecord(echo = "Bob")

  private def tExercise(
      choiceInterfaceId: Option[domain.ContractTypeId.Interface.OptionalPkg] = None,
      choiceName: String = "Transfer",
      choiceArgType: VA = echoTextVA,
  )(
      choiceArg: choiceArgType.Inj
  ): TExercise[choiceArgType.Inj] =
    TExercise(choiceInterfaceId, choiceName, choiceArgType, choiceArg)

  private final case class TExercise[Inj](
      choiceInterfaceId: Option[domain.ContractTypeId.Interface.OptionalPkg],
      choiceName: String,
      choiceArgType: VA.Aux[Inj],
      choiceArg: Inj,
  )
}

final class HttpServiceIntegrationTestCustomToken
    extends HttpServiceIntegrationTest
    with AbstractHttpServiceIntegrationTestFunsCustomToken
