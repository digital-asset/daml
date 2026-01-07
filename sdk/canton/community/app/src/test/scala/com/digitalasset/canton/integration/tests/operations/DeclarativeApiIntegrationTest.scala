// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.operations

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.participant.admin.AdminWorkflowServices
import com.digitalasset.canton.participant.config.{
  DeclarativeConnectionConfig,
  DeclarativeDarConfig,
  DeclarativeIdpConfig,
  DeclarativeParticipantConfig,
  DeclarativePartyConfig,
  DeclarativeUserConfig,
  DeclarativeUserRightsConfig,
  ParticipantPermissionConfig,
}
import com.digitalasset.canton.{HasActorSystem, HasExecutionContext, UniquePortGenerator}
import monocle.macros.syntax.lens.*
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives.{Remaining, complete, get, path}
import org.apache.pekko.stream.scaladsl.FileIO

import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicReference

final class DeclarativeApiIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasActorSystem
    with HasExecutionContext {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory
    )
  )

  private val httpPort = UniquePortGenerator.next

  private val declarativeP1 = new AtomicReference(
    DeclarativeParticipantConfig(
      dars = Seq(
        s"DOESNTEXIST"
      ).map(x => DeclarativeDarConfig(location = x))
    )
  )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M2_Manual
      .addConfigTransforms(
        ConfigTransforms.updateParticipantConfig("participant1")(
          _.focus(_.alphaDynamic).replace(declarativeP1.get())
        ),
        _.focus(_.parameters.enableAlphaStateViaConfig).replace(true),
      )

  override def beforeAll(): Unit = {
    super.beforeAll()
    Http()
      .newServerAt("localhost", httpPort.unwrap)
      .bind {
        get {
          path(Remaining) { fileName =>
            val filePath = Paths.get("./community/common/target/scala-2.13/classes/" + fileName)
            if (filePath.toFile.exists()) {
              complete(
                HttpResponse(entity =
                  HttpEntity(
                    ContentTypes.`application/octet-stream`,
                    filePath.toFile.length(),
                    FileIO.fromPath(filePath),
                  )
                )
              )
            } else {
              complete(StatusCodes.NotFound -> s"File $fileName not found.")
            }
          }
        }
      }
      .discard
  }

  protected def updateConfig(env: TestConsoleEnvironment, participant: String)(
      update: DeclarativeParticipantConfig => DeclarativeParticipantConfig
  ): Unit = {
    val newConfig = ConfigTransforms.updateParticipantConfig(participant)(
      _.focus(_.alphaDynamic).modify(update)
    )(env.environment.config)
    env.environment.pokeOrUpdateConfig(Some(Right(newConfig)))
  }

  "starting with an invalid config fails appropriately" in { implicit env =>
    import env.*

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.start(),
      // TODO(#25043) nicer error reporting, should be only one error message, not two
      _.errorMessage should include("Failed to run background update due to unhandled exception"),
      _.errorMessage should include("Initial declarative state refresh for participant1 failed"),
    )
  }

  "starting with initially empty config succeeds" in { implicit env =>
    import env.*
    updateConfig(env, "participant1")(
      _.focus(_.dars).replace(Seq.empty)
    )
    participant1.start()
  }

  "adding idps" in { implicit env =>
    import env.*
    loggerFactory.assertLogs(
      updateConfig(env, "participant1")(
        _.focus(_.idps)
          .replace(
            Seq(
              DeclarativeIdpConfig(
                identityProviderId = "IDP1",
                jwksUrl = "http://localhost:1234",
                issuer = "foo",
              ),
              DeclarativeIdpConfig(
                identityProviderId = "IDP2",
                jwksUrl = "http://localhost:1234",
                issuer = "foo",
              ),
            )
          )
      ),
      _.warningMessage should include("add failed for idps with key=IDP2"),
    )

    participant1.ledger_api.identity_provider_config
      .list()
      .map(_.identityProviderId.value) shouldBe Seq(
      "IDP1"
    )

  }

  "adding idps for real" in { implicit env =>
    import env.*
    updateConfig(env, "participant1")(
      _.focus(_.idps).replace(
        Seq(
          DeclarativeIdpConfig(
            identityProviderId = "IDP1",
            jwksUrl = "http://localhost:1234",
            issuer = "foo",
          ),
          DeclarativeIdpConfig(
            identityProviderId = "IDP2",
            jwksUrl = "http://localhost:1234",
            issuer = "bar",
          ),
        )
      )
    )
    participant1.ledger_api.identity_provider_config
      .list()
      .map(_.identityProviderId.value) shouldBe Seq(
      "IDP1",
      "IDP2",
    )
  }

  "adding users" in { implicit env =>
    import env.*
    updateConfig(env, "participant1")(
      _.focus(_.parties)
        .replace(Seq("Alice", "Bob", "Charlie").map(p => DeclarativePartyConfig(party = p)))
        .focus(_.users)
        .replace(
          Seq(
            DeclarativeUserConfig(
              user = "User1",
              primaryParty = Some("Alice"),
              annotations = Map("who" -> "Arnold"),
              rights = DeclarativeUserRightsConfig(
                actAs = Set("Bob"),
                readAs = Set("Charlie"),
                participantAdmin = true,
                identityProviderAdmin = true,
              ),
            )(),
            DeclarativeUserConfig(
              user = "User2",
              identityProviderId = "IDP1",
              rights = DeclarativeUserRightsConfig(
                readAsAnyParty = true,
                identityProviderAdmin = true,
              ),
            )(),
          )
        )
    )

    val user1 = participant1.ledger_api.users
      .list(filterUser = "User1")
      .users
      .headOption
      .valueOrFail("must be here")

    user1.id shouldBe "User1"
    user1.identityProviderId shouldBe ""
    user1.primaryParty shouldBe empty // party is not allocated yet as we don't have a sync connection
    user1.annotations shouldBe Map("who" -> "Arnold")

    val rights1 = participant1.ledger_api.users.rights.list(id = "User1")
    rights1.participantAdmin shouldBe true
    rights1.identityProviderAdmin shouldBe true
    // as the parties are not allocated / known to the ledger api server yet, these are empty
    rights1.readAs shouldBe empty
    rights1.actAs shouldBe empty

    val user2 = participant1.ledger_api.users
      .list(filterUser = "User2", identityProviderId = "IDP1")
      .users
      .headOption
      .valueOrFail("must be here")

    user2.identityProviderId shouldBe "IDP1"
    val rights2 =
      participant1.ledger_api.users.rights.list(id = "User2", identityProviderId = "IDP1")
    rights2.identityProviderAdmin shouldBe true
    rights2.readAsAnyParty shouldBe true

  }

  "bootstrapping sequencer" in { implicit env =>
    import env.*
    sequencers.local.start()
    mediators.local.start()

    NetworkBootstrapper(
      Seq(
        NetworkTopologyDescription(
          daName,
          synchronizerOwners = Seq(sequencer1),
          synchronizerThreshold = PositiveInt.one,
          sequencers = Seq(sequencer1, sequencer2),
          mediators = Seq(mediator1, mediator2),
        )
      )
    )(env).bootstrap()
  }

  "adding a ledger connection" in { implicit env =>
    import env.*
    updateConfig(env, "participant1")(
      _.focus(_.connections).replace(
        Seq(
          DeclarativeConnectionConfig.tryCreate(
            "mysync",
            Map("primary" -> Endpoint(host = "localhost", port = sequencer1.config.publicApi.port)),
          )
        )
      )
    )

    // verify ledger connection exists
    participant1.synchronizers.list_connected() should not be empty

    // parties should be registered now
    participant1.parties
      .list(filterParticipant = participant1.id.filterString)
      .map(_.party.uid.identifier)
      .toSet should contain allElementsOf Set("Alice", "Bob", "Charlie")

    // parties should be associated to the user
    val user = participant1.ledger_api.users
      .get(id = "User1")
    user.primaryParty.map(_.uid.identifier.str) should contain("Alice")
    user.annotations shouldBe Map("who" -> "Arnold")
    val rights = participant1.ledger_api.users.rights.list(id = "User1")
    rights.actAs.map(_.uid.identifier) shouldBe Set("Bob")
    rights.readAs.map(_.uid.identifier) shouldBe Set("Charlie")
  }

  "modifying ledger connections" in { implicit env =>
    import env.*
    updateConfig(env, "participant1")(
      _.focus(_.connections).replace(
        Seq(
          DeclarativeConnectionConfig.tryCreate(
            "mysync",
            Map(
              "primary" -> Endpoint(host = "localhost", port = sequencer1.config.publicApi.port),
              "secondary" -> Endpoint(host = "localhost", port = sequencer2.config.publicApi.port),
            ),
          )
        )
      )
    )

    participant1.synchronizers
      .config("mysync")
      .valueOrFail("shouldBethere")
      .sequencerConnections
      .aliasToConnection
      .forgetNE
      .toSeq should have length (2)
  }

  "add dars" in { implicit env =>
    import env.*
    updateConfig(env, "participant1")(
      _.focus(_.dars)
        .replace(
          Seq(
            s"http://localhost:${httpPort.unwrap}/CantonExamples.dar",
            "./community/participant/target/scala-2.13/classes/canton-builtin-admin-workflow-party-replication-alpha.dar",
          ).map(location => DeclarativeDarConfig(location = location))
        )
        .focus(_.parties)
        .replace(Seq("Alice", "Bob", "Charlie").map(p => DeclarativePartyConfig(party = p)))
    )
    participant1.start()

    participant1.dars.list().map(_.name).toSet shouldBe Set(
      AdminWorkflowServices.PingDarResourceName,
      AdminWorkflowServices.PartyReplicationDarResourceName,
      "CantonExamples",
    )

  }

  "removing parties and users" in { implicit env =>
    import env.*
    // here we remove a party that is registered with a user, so the order must be right
    updateConfig(env, "participant1")(
      _.focus(_.parties)
        .replace(
          Seq(
            ("Alice", Seq.empty, ParticipantPermissionConfig.Submission),
            ("Bob", Seq("mysync"), ParticipantPermissionConfig.Confirmation),
            // removed charly
            ("David", Seq.empty, ParticipantPermissionConfig.Confirmation),
            // edward should not appear
            ("Edward", Seq("invalidSync"), ParticipantPermissionConfig.Confirmation),
          ).map { case (p, s, r) =>
            DeclarativePartyConfig(party = p, synchronizers = s, permission = r)
          }
        )
        .focus(_.removeParties)
        .replace(true)
        .focus(_.idps)
        .replace(
          Seq(
            DeclarativeIdpConfig(
              identityProviderId = "IDP1",
              jwksUrl = "http://localhost:1234",
              issuer = "foo",
              audience = Some("audience"),
            ),
            // removed IDP2
            DeclarativeIdpConfig(
              identityProviderId = "IDP3",
              jwksUrl = "http://localhost:1234",
              issuer = "foobar",
            ),
          )
        )
        .focus(_.removeIdps)
        .replace(true)
        .focus(_.users)
        .replace(
          Seq(
            DeclarativeUserConfig(
              user = "User1",
              primaryParty = Some("Alice"),
              annotations = Map("who" -> "Barney"),
              identityProviderId = "IDP3",
              rights = DeclarativeUserRightsConfig(
                actAs = Set("Bob", "Edward"),
                readAs = Set("David"),
                participantAdmin = true,
                identityProviderAdmin = true,
              ),
            )(),
            DeclarativeUserConfig(
              user = "User3",
              rights = DeclarativeUserRightsConfig(
                readAsAnyParty = true,
                identityProviderAdmin = true,
              ),
            )(),
          )
        )
        .focus(_.removeUsers)
        .replace(true)
    )

    participant1.parties.list().map(_.party.uid.identifier).toSet shouldBe Set(
      "participant1",
      "Alice",
      "Bob",
      "David",
    )

    // idp2 is removed
    participant1.ledger_api.identity_provider_config
      .list()
      .map(_.identityProviderId.value)
      .toSet shouldBe Set("IDP1", "IDP3")

    // user1 got updated
    participant1.ledger_api.users
      .get("User1", identityProviderId = "IDP3")
      .annotations shouldBe Map("who" -> "Barney")

    // user1 privs are changed
    val rights =
      participant1.ledger_api.users.rights.list(id = "User1", identityProviderId = "IDP3")
    rights.readAs.map(_.uid.identifier) shouldBe Set("David")
    rights.actAs.map(_.uid.identifier) shouldBe Set("Bob")

    // user2 is gone
    participant1.ledger_api.users.list(filterUser = "User2").users shouldBe empty

  }

}
