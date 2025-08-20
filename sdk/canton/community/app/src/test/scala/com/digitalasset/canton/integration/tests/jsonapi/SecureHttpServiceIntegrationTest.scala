// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.ledger.api.v2.value as v
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.http
import com.digitalasset.canton.http.CommandMeta
import com.digitalasset.canton.http.json.JsonError
import com.digitalasset.canton.http.json.SprayJson.decode as jdecode
import com.digitalasset.canton.http.util.Logging.instanceUUIDLogCtx
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.jsonapi.AbstractHttpServiceIntegrationTestFuns.HttpServiceTestFixtureData
import com.digitalasset.canton.integration.tests.jsonapi.HttpServiceTestFixture.UseTls
import com.digitalasset.canton.ledger.api.refinements.ApiTypes as lar
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.test.TypedValueGenerators.ValueAddend as VA
import org.apache.pekko.http.scaladsl.model.{StatusCodes, Uri}
import org.scalatest.Assertion
import scalaz.std.scalaFuture.*
import scalaz.syntax.apply.*
import scalaz.syntax.bifunctor.*
import scalaz.syntax.show.*
import scalaz.{EitherT, \/, \/-}
import shapeless.record.Record as ShRecord
import spray.json.JsValue

import scala.concurrent.Future

/** Tests that are exercising features independently of both user authentication and the query
  * store.
  */
class SecureHttpServiceIntegrationTest
    extends AbstractHttpServiceIntegrationTest
    with AbstractHttpServiceIntegrationTestFunsUserToken {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  import SecureHttpServiceIntegrationTest.*
  import com.digitalasset.canton.http.json.JsonProtocol.*
  import com.digitalasset.canton.integration.tests.jsonapi.AbstractHttpServiceIntegrationTestFuns.ciouDar

  override def useTls: UseTls = UseTls.Tls

  "query with invalid JSON query should return error" in httpTestFixture { fixture =>
    fixture
      .postJsonStringRequest(Uri.Path("/v1/query"), "{NOT A VALID JSON OBJECT")
      .parseResponse[JsValue]
      .map(inside(_) { case http.ErrorResponse(_, _, StatusCodes.BadRequest, _) =>
        succeed
      }): Future[Assertion]
  }

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

  "submit commands" should {
    import com.digitalasset.canton.integration.tests.jsonapi.AbstractHttpServiceIntegrationTestFuns.{
      UriFixture,
      EncoderFixture,
    }
    def submitCommand(
        fixture: UriFixture with EncoderFixture,
        synchronizerId: Option[SynchronizerId],
    ) =
      for {
        aliceH <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (alice, aliceHeaders) = aliceH
        response <- postCreateCommand(
          iouCommand(alice, TpId.IIou.TestIIou)
            .copy(meta =
              Some(
                CommandMeta(
                  None,
                  None,
                  None,
                  None,
                  None,
                  None,
                  None,
                  synchronizerId,
                  None,
                )
              )
            ),
          fixture,
          aliceHeaders,
        )
      } yield response

    "succeed for no synchronizer id (automatic synchronizer routing)" in httpTestFixture {
      fixture =>
        for {
          _ <- uploadPackage(fixture)(ciouDar)
          response <- submitCommand(fixture, synchronizerId = None)
        } yield inside(response) { case http.OkResponse(_, None, StatusCodes.OK) =>
          succeed
        }
    }

    "succeed for valid synchronizer id" in httpTestFixture { fixture =>
      for {
        _ <- uploadPackage(fixture)(ciouDar)
        response <- submitCommand(fixture, synchronizerId = Some(validSynchronizerId))
      } yield inside(response) { case http.OkResponse(_, None, StatusCodes.OK) =>
        succeed
      }
    }

    "fail for invalid synchronizer id" in httpTestFixture { fixture =>
      val invalidId = SynchronizerId.tryFromString("invalid::synchronizerid")
      for {
        _ <- uploadPackage(fixture)(ciouDar)
        response <- submitCommand(
          fixture,
          synchronizerId = Some(invalidId),
        )
      } yield inside(response) {
        case http.ErrorResponse(Seq(onlyError), None, StatusCodes.BadRequest, Some(lapiError)) =>
          val errMsg = s"Cannot submit transaction to prescribed synchronizer `$invalidId`"
          onlyError should include(errMsg)
          lapiError.message should include(errMsg)
      }
    }
  }

  "exercise interface choices" should {
    import com.digitalasset.canton.integration.tests.jsonapi.AbstractHttpServiceIntegrationTestFuns.{
      UriFixture,
      EncoderFixture,
    }

    def createIouAndExerciseTransfer(
        fixture: UriFixture with EncoderFixture,
        initialTplId: http.ContractTypeId.Template.RequiredPkg,
        exerciseTid: http.ContractTypeId.RequiredPkg,
        choice: TExercise[_] = tExercise(choiceArgType = echoTextVA)(echoTextSample),
    ) = for {
      aliceH <- fixture.getUniquePartyAndAuthHeaders("Alice")
      (alice, aliceHeaders) = aliceH
      createTest <- postCreateCommand(
        iouCommand(alice, initialTplId),
        fixture,
        aliceHeaders,
      )
      testIIouID = resultContractId(createTest)
      exerciseTest <- fixture
        .postJsonRequest(
          Uri.Path("/v1/exercise"),
          encodeExercise(fixture.encoder)(
            iouTransfer(
              http.EnrichedContractId(Some(exerciseTid), testIIouID),
              choice,
            )
          ),
          aliceHeaders,
        )
        .parseResponse[http.ExerciseResponse[JsValue]]
    } yield exerciseTest

    def exerciseSucceeded[A](
        exerciseTest: http.SyncResponse[http.ExerciseResponse[JsValue]]
    ) =
      inside(exerciseTest) { case http.OkResponse(er, None, StatusCodes.OK) =>
        inside(jdecode[String](er.exerciseResult)) { case \/-(decoded) => decoded }
      }

    "templateId = interface ID" in httpTestFixture { fixture =>
      for {
        _ <- uploadPackage(fixture)(ciouDar)
        result <- createIouAndExerciseTransfer(
          fixture,
          initialTplId = TpId.IIou.TestIIou,
          // whether we can exercise by interface-ID
          exerciseTid = TpId.IIou.IIou,
        ) map exerciseSucceeded
      } yield result should ===("Bob invoked IIou.Transfer")
    }

    // ideally we would upload IIou.daml, then force a reload, then upload ciou;
    // however tests currently don't play well with reload -SC
    "templateId = template ID" in httpTestFixture { fixture =>
      for {
        _ <- uploadPackage(fixture)(ciouDar)
        result <- createIouAndExerciseTransfer(
          fixture,
          initialTplId = TpId.CIou.CIou,
          // whether we can exercise inherited by concrete template ID
          exerciseTid = TpId.CIou.CIou,
        ) map exerciseSucceeded
      } yield result should ===("Bob invoked IIou.Transfer")
    }

    "templateId = template ID, choiceInterfaceId = interface ID" in httpTestFixture { fixture =>
      for {
        _ <- uploadPackage(fixture)(ciouDar)
        result <- createIouAndExerciseTransfer(
          fixture,
          initialTplId = TpId.CIou.CIou,
          exerciseTid = TpId.CIou.CIou,
          choice = tExercise(choiceInterfaceId = Some(TpId.IIou.IIou), choiceArgType = echoTextVA)(
            echoTextSample
          ),
        ) map exerciseSucceeded
      } yield result should ===("Bob invoked IIou.Transfer")
    }

    "templateId = template, no choiceInterfaceId, picks template Overridden" in httpTestFixture {
      fixture =>
        for {
          _ <- uploadPackage(fixture)(ciouDar)
          result <- createIouAndExerciseTransfer(
            fixture,
            initialTplId = TpId.CIou.CIou,
            exerciseTid = TpId.CIou.CIou,
            choice = tExercise(choiceName = "Overridden", choiceArgType = echoTextPairVA)(
              ShRecord(echo = ShRecord(_1 = "yes", _2 = "no"))
            ),
          ) map exerciseSucceeded
        } yield result should ===("(\"yes\",\"no\") invoked CIou.Overridden")
    }

    "templateId = template, choiceInterfaceId = interface, picks interface Overridden" in httpTestFixture {
      fixture =>
        for {
          _ <- uploadPackage(fixture)(ciouDar)
          result <- createIouAndExerciseTransfer(
            fixture,
            initialTplId = TpId.CIou.CIou,
            exerciseTid = TpId.CIou.CIou,
            choice = tExercise(Some(TpId.Transferrable.Transferrable), "Overridden", echoTextVA)(
              ShRecord(echo = "yesyes")
            ),
          ) map exerciseSucceeded
        } yield result should ===("yesyes invoked Transferrable.Overridden")
    }

    "templateId = template, no choiceInterfaceId, ambiguous" in httpTestFixture { fixture =>
      for {
        _ <- uploadPackage(fixture)(ciouDar)
        response <- createIouAndExerciseTransfer(
          fixture,
          initialTplId = TpId.CIou.CIou,
          exerciseTid = TpId.CIou.CIou,
          choice = tExercise(choiceName = "Ambiguous", choiceArgType = echoTextVA)(
            ShRecord(echo = "ambiguous-test")
          ),
        )
      } yield inside(response) {
        case http.ErrorResponse(Seq(onlyError), None, StatusCodes.BadRequest, None) =>
          (onlyError should include regex
            raw"Cannot resolve Choice Argument type, given: \(TemplateId\([0-9a-f]{64},CIou,CIou\), Ambiguous\)")
      }
    }
  }

  "fail to exercise by key with interface ID" in httpTestFixture { fixture =>
    import fixture.encoder
    for {
      _ <- uploadPackage(fixture)(ciouDar)
      aliceH <- fixture.getUniquePartyAndAuthHeaders("Alice")
      (alice, aliceHeaders) = aliceH
      createTest <- postCreateCommand(
        iouCommand(alice, TpId.CIou.CIou),
        fixture,
        aliceHeaders,
      )
      _ = createTest.status should ===(StatusCodes.OK)
      exerciseTest <- fixture
        .postJsonRequest(
          Uri.Path("/v1/exercise"),
          encodeExercise(encoder)(
            iouTransfer(
              http.EnrichedContractKey(
                TpId.unsafeCoerce[http.ContractTypeId.Template, Ref.PackageRef](TpId.IIou.IIou),
                v.Value(v.Value.Sum.Party(http.Party unwrap alice)),
              ),
              tExercise()(ShRecord(echo = "bob")),
            )
          ),
          aliceHeaders,
        )
        .parseResponse[JsValue]
    } yield inside(exerciseTest) {
      case http.ErrorResponse(Seq(lookup), None, StatusCodes.BadRequest, _) =>
        lookup should include regex raw"Cannot resolve template ID, given: TemplateId\(\w+,IIou,IIou\)"
    }
  }

  private[this] def iouTransfer[Inj](
      locator: http.ContractLocator[v.Value],
      choice: TExercise[Inj],
  ) = {
    import choice.{choiceInterfaceId, choiceName, choiceArgType, choiceArg}
    val payload = argToApi(choiceArgType)(choiceArg)
    http.ExerciseCommand(
      locator,
      http.Choice(choiceName),
      v.Value(v.Value.Sum.Record(payload)),
      choiceInterfaceId,
      None,
    )
  }
}

object SecureHttpServiceIntegrationTest {
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
      choiceInterfaceId: Option[http.ContractTypeId.Interface.RequiredPkg] = None,
      choiceName: String = "Transfer",
      choiceArgType: VA = echoTextVA,
  )(
      choiceArg: choiceArgType.Inj
  ): TExercise[choiceArgType.Inj] =
    TExercise(choiceInterfaceId, choiceName, choiceArgType, choiceArg)

  private final case class TExercise[Inj](
      choiceInterfaceId: Option[http.ContractTypeId.Interface.RequiredPkg],
      choiceName: String,
      choiceArgType: VA.Aux[Inj],
      choiceArg: Inj,
  )
}
