// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import better.files.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.util.TextFileUtil
import io.circe.Json

trait ConfigGenerationTest extends CommunityIntegrationTest with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.withSetup(initializeParties(_))

  "util.generate_daml_script_participants_conf" in { implicit env =>
    import env.*

    File.usingTemporaryDirectory("participant-config-gen-test") { tempDir =>
      // Given two participants, each hosting one party

      // And an exported participant config
      val participantConfigFile = tempDir / "participant-config.json"
      utils.generate_daml_script_participants_conf(
        Some(participantConfigFile.toString()),
        useParticipantAlias = false,
        defaultParticipant = Some(participant1),
      )

      // Inspect json content of file and inspect the content manually because we cannot invoke
      // the daml-sdk, as it is no longer available in the canton repo since daml-2.0
      val rawJson = TextFileUtil
        .readStringFromFile(participantConfigFile.toJava)
        .getOrElse(fail("Failed to read participant-config.json"))

      def participantId(p: ParticipantReference): String = p.uid.toProtoPrimitive

      io.circe.parser.parse(rawJson) shouldBe Right(
        Json.fromFields(
          Vector(
            "default_participant" -> participantJsonConfig(participant1),
            "participants" -> Json.fromFields(
              Vector(
                participantId(participant1) -> participantJsonConfig(participant1),
                participantId(participant2) -> participantJsonConfig(participant2),
              )
            ),
            "party_participants" -> Json.fromFields(
              Vector(participant1, participant2).flatMap(p =>
                p.parties
                  .list(filterParticipant = p.filterString)
                  .map(x => x.party.uid.toProtoPrimitive -> Json.fromString(participantId(p)))
              )
            ),
          )
        )
      )
    }
  }
  "util.generate_daml_script_participants_conf with participant aliases and without any default participants" in {
    implicit env =>
      import env.*

      File.usingTemporaryDirectory("participant-config-gen-test") { tempDir =>
        // Given two participants, each hosting one party

        // And an exported participant config
        val participantConfigFile = tempDir / "participant-config.json"
        utils.generate_daml_script_participants_conf(Some(participantConfigFile.toString()))

        // Inspect json content of file and inspect the content manually because we cannot invoke
        // the daml-sdk, as it is no longer available in the canton repo since daml-2.0
        val rawJson = TextFileUtil
          .readStringFromFile(participantConfigFile.toJava)
          .getOrElse(fail("Failed to read participant-config.json"))

        io.circe.parser.parse(rawJson) shouldBe Right(
          Json.fromFields(
            Vector(
              "default_participant" -> Json.Null,
              "participants" -> Json.fromFields(
                Vector(
                  participant1.name -> participantJsonConfig(participant1),
                  participant2.name -> participantJsonConfig(participant2),
                )
              ),
              "party_participants" -> Json.fromFields(
                Vector(participant1, participant2).flatMap(p =>
                  p.parties
                    .list(filterParticipant = p.filterString)
                    .map(x => x.party.uid.toProtoPrimitive -> Json.fromString(p.name))
                )
              ),
            )
          )
        )
      }
  }

  private def initializeParties(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    participant1.synchronizers.connect_local(sequencer1, alias = daName)
    participant2.synchronizers.connect_local(sequencer1, alias = daName)

    participants.all.dars.upload(CantonTestsPath)

    val parties = Seq((participant1, "Alice"), (participant2, "Bob"))
    parties.map { case (participant, partyName) =>
      participant.parties.enable(name = partyName)
    }
  }

  private def participantJsonConfig(p: ParticipantReference): Json =
    Json.fromFields(
      Vector(
        "host" -> Json.fromString(p.config.clientLedgerApi.address),
        "port" -> Json.fromInt(p.config.clientLedgerApi.port.unwrap),
      )
    )
}

//class ConfigGenerationTestDefault extends ConfigGenerationTest

class ConfigGenerationTestPostgres extends ConfigGenerationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
