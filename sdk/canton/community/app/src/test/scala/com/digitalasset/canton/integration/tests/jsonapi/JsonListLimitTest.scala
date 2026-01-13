// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{EventFormat, Filters, TransactionFormat}
import com.daml.ledger.api.v2.value.Identifier
import com.daml.ledger.api.v2.{state_service, transaction_filter}
import com.digitalasset.canton.http.json.v2.JsCommandServiceCodecs.*
import com.digitalasset.canton.http.json.v2.JsSchema.JsServicesCommonCodecs.*
import com.digitalasset.canton.http.json.v2.JsStateServiceCodecs.*
import com.digitalasset.canton.http.json.v2.{
  JsCommand,
  JsGetActiveContractsResponse,
  JsSubmitAndWaitForTransactionResponse,
}
import com.digitalasset.canton.http.{Party, WebsocketConfig}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.jsonapi.AbstractHttpServiceIntegrationTestFuns.{
  HttpServiceTestFixtureData,
  dar1,
}
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}
import com.digitalasset.canton.ledger.service.MetadataReader
import com.digitalasset.daml.lf.data.Ref
import io.circe.parser.decode
import io.circe.syntax.*
import io.circe.{Json, parser}
import org.apache.pekko.http.scaladsl.model.Uri.Query
import org.apache.pekko.http.scaladsl.model.{HttpHeader, StatusCodes, Uri}
import scalaz.syntax.tag.*

import java.io.File
import scala.concurrent.Future

class JsonListLimitTest
    extends AbstractHttpServiceIntegrationTestFuns
    with HttpServiceUserFixture.UserToken {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def packageFiles: List[File] = List(dar1)

  private val listLimit: Long = 10

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
      .prependConfigTransform(
        ConfigTransforms.enableHttpLedgerApi(
          "participant1",
          Some(WebsocketConfig(httpListMaxElementsLimit = listLimit)),
        )
      )

  "json service" should {
    "return error when list is beyond the node limit" in httpTestFixture { http =>
      http.getUniquePartyAndAuthHeaders("Alice").flatMap { case (party, headers) =>
        for {
          endOffset <- submitMultipleCommands(
            http,
            party,
            headers,
            numberOfCommands = listLimit + 1,
          )
          status <- http
            .postJsonRequest(
              Uri.Path("/v2/state/active-contracts"),
              acsAtOffsetRequest(party, endOffset),
              headers,
            )
            .map(_._1)
        } yield {
          status shouldBe StatusCodes.ContentTooLarge
        }
      }
    }

    "return list fragment when limit is provided in query" in httpTestFixture { http =>
      http.getUniquePartyAndAuthHeaders("Alice").flatMap { case (party, headers) =>
        for {
          endOffset <- submitMultipleCommands(
            http,
            party,
            headers,
            numberOfCommands = listLimit + 1,
          )
          (status, result) <-
            postRequest(
              http.uri withPath Uri.Path("/v2/state/active-contracts") withQuery Query(
                ("limit", listLimit.toString)
              ),
              acsAtOffsetRequest(party, endOffset),
              headers,
            )

        } yield {
          status should be(StatusCodes.OK)
          decode[Seq[JsGetActiveContractsResponse]](result.toString).value.size should be(
            listLimit
          )
        }
      }
    }

    "return list fragment when limit is provided in query and there are fewer results" in httpTestFixture {
      http =>
        http.getUniquePartyAndAuthHeaders("Alice").flatMap { case (party, headers) =>
          for {
            endOffset <- submitMultipleCommands(
              http,
              party,
              headers,
              numberOfCommands = listLimit - 5,
            )
            (status, result) <-
              postRequest(
                http.uri withPath Uri.Path("/v2/state/active-contracts") withQuery Query(
                  ("limit", listLimit.toString)
                ),
                acsAtOffsetRequest(party, endOffset),
                headers,
              )

          } yield {
            status should be(StatusCodes.OK)
            decode[Seq[JsGetActiveContractsResponse]](result.toString).value.size should be(
              listLimit - 5
            )
          }
        }
    }

    "return list when size is below node limit" in httpTestFixture { http =>
      http.getUniquePartyAndAuthHeaders("Alice").flatMap { case (party, headers) =>
        for {
          endOffset <- submitMultipleCommands(
            http,
            party,
            headers,
            numberOfCommands = listLimit - 5,
          )
          (status, result) <-
            postRequest(
              http.uri withPath Uri.Path("/v2/state/active-contracts"),
              acsAtOffsetRequest(party, endOffset),
              headers,
            )

        } yield {
          status should be(StatusCodes.OK)
          decode[Seq[JsGetActiveContractsResponse]](result.toString).value.size should be(
            listLimit - 5
          )
        }
      }
    }

  }

  private def acsAtOffsetRequest(party: Party, endOffset: Long): Json =
    state_service
      .GetActiveContractsRequest(
        activeAtOffset = endOffset,
        eventFormat = Some(partyTransactions(party)),
      )
      .asJson

  private def submitMultipleCommands(
      http: HttpServiceTestFixtureData,
      party: Party,
      headers: List[HttpHeader],
      numberOfCommands: Long,
  ): Future[Long] = for {
    _ <- (1L to numberOfCommands).foldLeft(Future.successful(())) { (acc, idx) =>
      acc.flatMap(_ => submitCommand(http, party, headers, s"commandId-$idx"))
    }
    endOffset <- http.client.stateService.getLedgerEndOffset()
  } yield endOffset

  private def submitCommand(
      http: HttpServiceTestFixtureData,
      party: Party,
      headers: List[HttpHeader],
      commandId: String,
  ): Future[Unit] = {
    val metadataDar1 =
      MetadataReader.readFromDar(dar1).valueOr(e => fail(s"Cannot read dar1 metadata: $e"))
    val iouPkgId = MetadataReader
      .templateByName(metadataDar1)(Ref.QualifiedName.assertFromString("Iou:Iou"))
      .head
      ._1

    val createCommand = JsCommand.CreateCommand(
      templateId = Identifier(iouPkgId, "Iou", "Iou"),
      createArguments = parser
        .parse(
          s"""{"observers":[],"issuer":"$party","amount":"999.99","currency":"USD","owner":"$party"}"""
        )
        .value,
    )
    val jsCommandJson: Json = Json.obj(
      ("commands", Json.arr(Json.obj("CreateCommand" -> createCommand.asJson))),
      ("commandId", Json.fromString(commandId)),
      ("actAs", Json.arr(Json.fromString(party.unwrap))),
      ("userId", Json.fromString("alice")),
    )

    val jsSubmitAndWaitForTransactionRequest: Json = Json.obj(
      ("commands", jsCommandJson),
      (
        "transactionFormat",
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
      ),
    )

    postJsonRequest(
      uri = http.uri.withPath(Uri.Path("/v2/commands/submit-and-wait-for-transaction")),
      json = jsSubmitAndWaitForTransactionRequest,
      headers = headers,
    ).map { case (statusCode, result) =>
      assert(
        statusCode == StatusCodes.OK,
        s"Expected OK, got $statusCode with response: $result",
      )

      decode[JsSubmitAndWaitForTransactionResponse](result.toString()).value

    }
  }

  private def partyTransactions(party: Party) = transaction_filter.EventFormat(
    filtersByParty = Map(
      party.unwrap -> Filters(cumulative =
        Seq(
          transaction_filter.CumulativeFilter(
            identifierFilter = transaction_filter.CumulativeFilter.IdentifierFilter
              .WildcardFilter(
                transaction_filter.WildcardFilter(includeCreatedEventBlob = false)
              )
          )
        )
      )
    ),
    filtersForAnyParty = None,
    verbose = false,
  )
}
