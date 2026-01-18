// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{EventFormat, Filters, TransactionFormat}
import com.daml.ledger.api.v2.value.Identifier
import com.digitalasset.canton.http.Party
import com.digitalasset.canton.http.json.v2.JsCommand
import com.digitalasset.canton.http.json.v2.JsCommandServiceCodecs.*
import com.digitalasset.canton.http.json.v2.JsSchema.JsServicesCommonCodecs.*
import com.digitalasset.canton.integration.EnvironmentDefinition
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.jsonapi.AbstractHttpServiceIntegrationTestFuns.{
  HttpServiceTestFixtureData,
  dar1,
}
import com.digitalasset.canton.ledger.service.MetadataReader
import com.digitalasset.daml.lf.data.Ref
import io.circe.Json
import io.circe.syntax.*
import org.apache.pekko.http.scaladsl.model.{HttpHeader, StatusCode, StatusCodes, Uri}
import scalaz.syntax.tag.*

import java.io.File
import scala.concurrent.Future

class JsonErrorHandlingOfDamlVariantTest
    extends AbstractHttpServiceIntegrationTestFuns
    with HttpServiceUserFixture.UserToken {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def packageFiles: List[File] = List(dar1)

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .withSetup { implicit env =>
        import env.*
        Seq(participant1).foreach { p =>
          p.synchronizers.connect_local(sequencer1, alias = daName)
        }
        createChannel(participant1)
        darFiles.foreach(path => participant1.dars.upload(path.toFile.getAbsolutePath))
      }

  "json transcode layer" should {
    "return error upon misspelled tag field in variant" in httpTestFixture { http =>
      http.getUniquePartyAndAuthHeaders("Alice").flatMap { case (party, headers) =>
        for {
          (status, result) <- submitCommand(
            http,
            party,
            headers,
            """{ "misspeledTag":  "VTInteger", "value": "42"  }""",
          )
        } yield {
          status should be(StatusCodes.BadRequest)
          result.asJson.toString() should include("tag")
        }
      }
    }
    "return error upon wrong tag value in variant" in httpTestFixture { http =>
      http.getUniquePartyAndAuthHeaders("Alice").flatMap { case (party, headers) =>
        for {
          (status, result) <- submitCommand(
            http,
            party,
            headers,
            """{ "tag":  "Incorrect", "value": "42"  }""",
          )
        } yield {
          status should be(StatusCodes.BadRequest)
          result.asJson.toString() should include("tag")
        }
      }
    }
  }

  private def submitCommand(
      http: HttpServiceTestFixtureData,
      party: Party,
      headers: List[HttpHeader],
      tagFieldEncoding: String,
  ): Future[(StatusCode, Json)] = {
    val metadataDar1 =
      MetadataReader.readFromDar(dar1).valueOr(e => fail(s"Cannot read dar1 metadata: $e"))
    val iouPkgId = MetadataReader
      .templateByName(metadataDar1)(Ref.QualifiedName.assertFromString("Iou:Iou"))
      .head
      ._1

    val createCommand = JsCommand.CreateCommand(
      templateId = Identifier(iouPkgId, "Test", "VariantTest"),
      createArguments = Json.obj(
        "owner" -> Json.fromString(party.unwrap),
        "variantField" -> Json.fromString(tagFieldEncoding),
      ),
    )
    val jsCommandJson = Json.obj(
      ("commands", Json.arr(Json.obj("CreateCommand" -> createCommand.asJson))),
      ("commandId", Json.fromString("test-command-id")),
      ("actAs", Json.arr(Json.fromString(party.unwrap))),
      ("userId", Json.fromString("alice")),
    )

    val jsSubmitAndWaitForTransactionRequest = Json.obj(
      "commands" -> jsCommandJson,
      "transactionFormat" ->
        TransactionFormat(
          eventFormat = Some(
            EventFormat(
              filtersByParty = Map(party.unwrap -> Filters(Nil)),
              filtersForAnyParty = None,
              verbose = true,
            )
          ),
          transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
        ).asJson,
    )

    postJsonRequest(
      uri = http.uri.withPath(Uri.Path("/v2/commands/submit-and-wait-for-transaction")),
      json = jsSubmitAndWaitForTransactionRequest,
      headers = headers,
    )
  }
}
