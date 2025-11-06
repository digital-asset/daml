// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.submission.BaseInteractiveSubmissionTest.ParticipantSelector
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  TestConsoleEnvironment,
}
import org.apache.pekko.http.scaladsl.model.{StatusCodes, Uri}

class JsonPathPrefixTests
    extends AbstractHttpServiceIntegrationTestFuns
    with HttpServiceUserFixture.UserToken {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  private lazy val testCases: Map[String, Option[String]] = Map(
    "participant1" -> Some("/any/company/prefix"),
    "participant2" -> Some("any/company/prefix"),
    "participant3" -> None,
  )

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*
        Seq(participant1, participant2, participant3).foreach { p =>
          p.synchronizers.connect_local(sequencer1, alias = daName)
        }
        createChannel(participant1)
      }
      .prependConfigTransforms(
        testCases.toSeq.map { case (participant, prefix) =>
          ConfigTransforms.enableHttpLedgerApi(participant, pathPrefix = prefix)
        }*
      )

  def testService(participantSelector: ParticipantSelector, prefix: Option[String])(implicit
      env: TestConsoleEnvironment
  ) = {
    import env.*
    val prefixString = prefix.getOrElse("")
    val normalizedPrefix = s"/$prefixString/".replaceAll("//", "/")
    (for {
      http <- adHocHttp(participantSelector)
      (status, _) <- http.getRequestString(Uri.Path(s"${normalizedPrefix}v2/idps"), List.empty)
    } yield {
      status should be(StatusCodes.OK)
    }).futureValue
  }

  "http service" should {
    "use configured prefix" in { implicit env =>
      forAll(testCases) { case (participantName, prefix) =>
        testService(env => env.lp(participantName), prefix)
      }
    }
  }
}
