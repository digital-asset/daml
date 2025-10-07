// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.ledger.api.v2.command_service.SubmitAndWaitResponse
import com.daml.ledger.api.v2.{state_service, transaction_filter}
import com.daml.ledger.test.java.model.iou.Iou
import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.http.json.SprayJson
import com.digitalasset.canton.http.json.v2.JsCommandServiceCodecs.*
import com.digitalasset.canton.http.json.v2.JsStateServiceCodecs.*
import com.digitalasset.canton.http.json.v2.{JsCommand, JsCommands, JsGetActiveContractsResponse}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.util.MonadUtil
import io.circe.parser.decode
import io.circe.syntax.*
import org.apache.pekko.http.scaladsl.model.{StatusCodes, Uri}
import scalaz.syntax.tag.*

import scala.concurrent.Future

/** Test makes few calls to obtain ACS and checks if the average response time is not exceeded.
  */
class JsonStreamAsListPerformanceTests
    extends AbstractHttpServiceIntegrationTestFuns
    with HttpServiceUserFixture.UserToken {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  private val numberOfRepeatedCalls: Int = 5
  private val maxAverageTimeForCall: Long = 1000
  "active contract" should {
    "give answer in a reasonable time" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val createJsCommand: JsCommand.Command = JsCommand.CreateCommand(
          templateId = TemplateId.fromJavaProtoIdentifier(Iou.TEMPLATE_ID.toProto).toIdentifier,
          createArguments = io.circe.parser
            .parse(
              s"""{"observers":[],"issuer":"$alice","amount":"999.99","currency":"USD","owner":"$alice"}"""
            )
            .value,
        )
        val command = JsCommands(
          commands = Seq(createJsCommand),
          workflowId = None,
          commandId = "somecommandid",
          userId = Some("userId"),
          actAs = Seq(alice.unwrap),
          synchronizerId = None,
        )

        def acsCall(completionOffset: Long): Future[Unit] =
          fixture
            .postJsonRequest(
              Uri.Path("/v2/state/active-contracts"),
              SprayJson
                .parse(
                  state_service
                    .GetActiveContractsRequest(
                      activeAtOffset = completionOffset,
                      eventFormat = Some(
                        transaction_filter.EventFormat(
                          filtersByParty = Map.empty,
                          filtersForAnyParty = Some(
                            transaction_filter.Filters(
                              cumulative = Seq(
                                transaction_filter.CumulativeFilter(
                                  identifierFilter =
                                    transaction_filter.CumulativeFilter.IdentifierFilter
                                      .WildcardFilter(
                                        transaction_filter
                                          .WildcardFilter(includeCreatedEventBlob = true)
                                      )
                                )
                              )
                            )
                          ),
                          verbose = false,
                        )
                      ),
                    )
                    .asJson
                    .toString()
                )
                .valueOr(err => fail(s"$err")),
              headers,
            )
            .map { case (_, result) =>
              decode[Seq[JsGetActiveContractsResponse]](result.toString()).value.size should be(1)
            }

        for {
          completionOffset <- postJsonRequest(
            uri = fixture.uri.withPath(Uri.Path("/v2/commands/submit-and-wait")),
            json = SprayJson
              .parse(command.asJson.noSpaces)
              .valueOr(err => fail(s"$err")),
            headers = headers,
          ).map { case (statusCode, result) =>
            statusCode should be(StatusCodes.OK)
            decode[SubmitAndWaitResponse](result.toString()).value.completionOffset
          }
          startTime = System.currentTimeMillis()
          _ <- MonadUtil
            .repeatFlatmap(
              Future.unit,
              (_: Unit) => acsCall(completionOffset),
              numberOfRepeatedCalls,
            )
        } yield {
          val endTime = System.currentTimeMillis()
          val totalTime: Long = endTime - startTime
          totalTime should be < (numberOfRepeatedCalls * maxAverageTimeForCall)
        }
      }
    }
  }
}
