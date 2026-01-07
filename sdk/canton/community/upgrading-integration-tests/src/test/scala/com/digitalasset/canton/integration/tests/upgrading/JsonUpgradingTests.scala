// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.daml.ledger.api.v2.commands.Commands.DeduplicationPeriod
import com.daml.ledger.api.v2.{state_service, transaction_filter}
import com.daml.ledger.javaapi.data.Identifier
import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.damltests.upgrade.v1.java as v1
import com.digitalasset.canton.damltests.upgrade.v2.java as v2
import com.digitalasset.canton.http
import com.digitalasset.canton.http.json.v2.JsCommandServiceCodecs.*
import com.digitalasset.canton.http.json.v2.JsContractEntry.JsActiveContract
import com.digitalasset.canton.http.json.v2.JsStateServiceCodecs.*
import com.digitalasset.canton.http.json.v2.{JsCommand, JsCommands, JsGetActiveContractsResponse}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.jsonapi.AbstractHttpServiceIntegrationTestFuns.HttpServiceTestFixtureData
import com.digitalasset.canton.integration.tests.jsonapi.{HttpServiceUserFixture, HttpTestFuns}
import com.digitalasset.canton.integration.tests.upgrading.UpgradingBaseTest.{UpgradeV1, UpgradeV2}
import com.digitalasset.canton.topology.PartyId
import io.circe
import io.circe.Json
import io.circe.parser.*
import io.circe.syntax.*
import org.apache.pekko.http.scaladsl.model.ws.TextMessage
import org.apache.pekko.http.scaladsl.model.{StatusCode, StatusCodes, Uri}

import java.util.UUID
import scala.concurrent.Future

/** Smart contract upgrading JSON API integration tests.
  */
class JsonUpgradingTests extends HttpTestFuns with HttpServiceUserFixture.UserToken {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  private def party(name: String)(implicit env: TestConsoleEnvironment): PartyId =
    env.participant1.parties.list(name).headOption.valueOrFail("where is " + name).party

  override lazy val environmentDefinition = super.environmentDefinition.withSetup { implicit env =>
    import env.*
    participant1.parties.enable("alice")
    participant1.dars.upload(UpgradeV1)
    participant1.dars.upload(UpgradeV2)
  }

  "JSON API" when {
    def testUpDowngrading(fixture: HttpServiceTestFixtureData, alice: PartyId)(
        targetTemplateId: Identifier,
        packageIdSelectionPreference: Seq[String] = Seq.empty,
    )(jsonDamlValue: String)(asserts: (StatusCode, Json) => Unit) =
      fixture.headersWithAuth.flatMap { headers =>
        val jsCommand = JsCommand.CreateCommand(
          templateId = TemplateId.fromJavaIdentifier(targetTemplateId).toIdentifier,
          createArguments =
            io.circe.parser.parse(jsonDamlValue).valueOrFail("unparseable test data"),
        )

        val cmds = JsCommands(
          commands = Seq(jsCommand),
          workflowId = None,
          userId = Some(s"CantonConsole"),
          commandId = s"commandid_${UUID.randomUUID()}",
          deduplicationPeriod = Some(DeduplicationPeriod.Empty),
          actAs = Seq(alice.toLf),
          readAs = Seq.empty,
          submissionId = None,
          synchronizerId = None,
          minLedgerTimeAbs = None,
          minLedgerTimeRel = None,
          disclosedContracts = Seq.empty,
          packageIdSelectionPreference = packageIdSelectionPreference,
        )
        for {
          _ <- postJsonRequest(
            uri = fixture.uri.withPath(Uri.Path("/v2/commands/submit-and-wait")),
            json = cmds.asJson,
            headers = headers,
          ).map { case (statusCode, result) =>
            asserts(statusCode, result)
          }
        } yield ()
      }

    "be able to upgrade a Daml data payload" in httpTestFixture { fixture =>
      implicit val env: TestConsoleEnvironment = provideEnvironment

      val alice = party("alice")
      testUpDowngrading(fixture = fixture, alice = alice)(targetTemplateId =
        v2.upgrade.Upgrading.TEMPLATE_ID_WITH_PACKAGE_ID
      )(
        jsonDamlValue = s"""{"issuer":"${alice.toLf}", "owner":"${alice.toLf}", "field": "1337" }"""
      ) { case (status, _) => status should be(StatusCodes.OK) }
    }

    "be able to downgrade a payload Daml data payload" in httpTestFixture { fixture =>
      implicit val env: TestConsoleEnvironment = provideEnvironment
      val alice = party("alice")

      testUpDowngrading(fixture = fixture, alice = alice)(targetTemplateId =
        v1.upgrade.Upgrading.TEMPLATE_ID_WITH_PACKAGE_ID
      )(
        jsonDamlValue =
          s"""{"issuer":"${alice.toLf}", "owner":"${alice.toLf}", "field": "1337", "more": null }"""
      ) { case (status, _) => status should be(StatusCodes.OK) }
    }

    "reject a downgrade with a populated optional field" in httpTestFixture { fixture =>
      implicit val env: TestConsoleEnvironment = provideEnvironment

      val alice = party("alice")

      testUpDowngrading(fixture = fixture, alice = alice)(targetTemplateId =
        v1.upgrade.Upgrading.TEMPLATE_ID_WITH_PACKAGE_ID
      )(
        jsonDamlValue =
          s"""{"issuer":"${alice.toLf}", "owner":"${alice.toLf}", "field": "1337", "more": "don't ignore meee!!" }"""
      ) { case (status, result) =>
        status should be(StatusCodes.BadRequest)
        result.toString() should include(
          "The submitted request has invalid arguments: Unexpected fields: more"
        )
      }
    }

    "consider the Commands.packageIdSelectionPreferences" in httpTestFixture { fixture =>
      implicit val env: TestConsoleEnvironment = provideEnvironment

      val alice = party("alice")

      testUpDowngrading(fixture = fixture, alice = alice)(
        targetTemplateId = v1.upgrade.Upgrading.TEMPLATE_ID,
        packageIdSelectionPreference = Seq(v1.upgrade.Upgrading.PACKAGE_ID),
      )(
        jsonDamlValue =
          s"""{"issuer":"${alice.toLf}", "owner":"${alice.toLf}", "field": "1337", "more": "don't ignore meee!!" }"""
      ) { case (status, result) =>
        status should be(StatusCodes.BadRequest)
        result.toString() should include(
          "The submitted request has invalid arguments: Unexpected fields: more"
        )
      }
    }

    "accept package name for create command" in httpTestFixture { fixture =>
      implicit val env: TestConsoleEnvironment = provideEnvironment
      val alice = party("alice")

      testUpDowngrading(fixture = fixture, alice = alice)(targetTemplateId =
        v1.upgrade.Upgrading.TEMPLATE_ID
      )(
        jsonDamlValue =
          s"""{"issuer":"${alice.toLf}", "owner":"${alice.toLf}", "field": "1337", "more": null }"""
      ) { case (status, resullt) =>
        status should be(StatusCodes.OK)
      }
    }

    "be able to query using package name" in httpTestFixture { fixture =>
      implicit val env: TestConsoleEnvironment = provideEnvironment
      val alice = party("alice")
      for {
        _ <- testUpDowngrading(fixture = fixture, alice = alice)(targetTemplateId =
          v1.upgrade.Upgrading.TEMPLATE_ID_WITH_PACKAGE_ID
        )(
          jsonDamlValue =
            s"""{"issuer":"${alice.toLf}", "owner":"${alice.toLf}", "field": "42", "more": null }"""
        ) { case (status, _) => status should be(StatusCodes.OK) }
        jwt <- jwtForParties(fixture.uri)(List(http.Party(alice.toLf)), List())
        eventFormat = transaction_filter.EventFormat(
          filtersByParty = Map.empty,
          filtersForAnyParty = Some(
            transaction_filter.Filters(
              cumulative = Seq(
                transaction_filter.CumulativeFilter(
                  identifierFilter = transaction_filter.CumulativeFilter.IdentifierFilter
                    .TemplateFilter(
                      transaction_filter.TemplateFilter(
                        templateId = Some(
                          TemplateId
                            .fromJavaIdentifier(v1.upgrade.Upgrading.TEMPLATE_ID)
                            .toIdentifier
                        ),
                        includeCreatedEventBlob = false,
                      )
                    )
                )
              )
            )
          ),
          verbose = false,
        )
        endOffset = env.participant1.ledger_api.state.end()
        messagesStream <- fixture.getStream(
          path = Uri.Path("/v2/state/active-contracts"),
          jwt = jwt,
          message = TextMessage(
            state_service
              .GetActiveContractsRequest(
                activeAtOffset = endOffset,
                eventFormat = Some(eventFormat),
              )
              .asJson
              .noSpaces
          ),
          decoder = decode[JsGetActiveContractsResponse],
          filter = { (m: Either[circe.Error, JsGetActiveContractsResponse]) =>
            m match {
              case Right(activeContract) =>
                activeContract.contractEntry match {
                  case active: JsActiveContract =>
                    active.createdEvent.createArgument.value.noSpaces
                      .contains(""""field":"42"""")
                  case _ => false
                }
              case _ => false
            }
          },
        )
        messages <- Future(messagesStream)
      } yield inside(messages.head.value.contractEntry) {
        case JsActiveContract(created_event, _, _) =>
          created_event.templateId.packageId should be(
            v1.upgrade.Upgrading.PACKAGE_ID
          )
      }
    }

    "fail on wrong package reference" in httpTestFixture { fixture =>
      implicit val env: TestConsoleEnvironment = provideEnvironment

      val alice = party("alice")
      testUpDowngrading(fixture = fixture, alice = alice)(targetTemplateId =
        new Identifier(
          s"węry wrong package reference",
          v1.upgrade.Upgrading.TEMPLATE_ID.getModuleName,
          v1.upgrade.Upgrading.TEMPLATE_ID.getEntityName,
        )
      )(
        jsonDamlValue = s"""{"issuer":"${alice.toLf}", "owner":"${alice.toLf}", "field": "1337" }"""
      ) { case (status, result) =>
        status should be(StatusCodes.BadRequest)
        result.toString() should include(
          "The submitted request has invalid arguments: Value does not match the package-id or package-name formats"
        )
      }
    }

    // TODO(#25385): Add tests for showcasing package selection on incompatible vetted templates
    //               once package upgrade compatibility checks can be disabled.
  }
}
