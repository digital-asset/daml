// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package platform
package sandbox

import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.grpc.adapter.{ExecutionSequencerFactory, SingleThreadExecutionSequencerPool}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands.{Command, Commands, CreateCommand}
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.daml.ledger.resources.TestResourceContext
import com.daml.ledger.runner.common.Config.{SandboxParticipantConfig, SandboxParticipantId}
import com.daml.ledger.sandbox.{BridgeConfig, ConfigConverter, NewSandboxServer}
import com.daml.lf.VersionRange
import com.daml.lf.language.LanguageVersion
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.sandbox.fixture.SandboxFixture
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.ports.Port
import com.google.protobuf
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.util.{Failure, Success}

class EngineModeIT
    extends AsyncWordSpec
    with Matchers
    with Inside
    with TestResourceContext
    with SandboxFixture {
  private[this] implicit val esf: ExecutionSequencerFactory =
    new SingleThreadExecutionSequencerPool("testSequencerPool")

  private[this] val List(maxStableVersion, previewVersion, devVersion) =
    List(
      LanguageVersion.StableVersions.max,
      LanguageVersion.EarlyAccessVersions.max,
      LanguageVersion.DevVersions.max,
    )

  private[this] val applicationId = ApplicationId("EngineModeIT")

  private[this] def ledgerClientConfiguration =
    ledger.client.configuration.LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(applicationId),
      ledgerIdRequirement = ledger.client.configuration.LedgerIdRequirement.none,
      commandClient = ledger.client.configuration.CommandClientConfiguration.default,
      token = None,
    )

  private[this] def buildRequest(pkgId: String, ledgerId: LedgerId) = {
    import scalaz.syntax.tag._
    val party = "Alice"
    val tmplId = Some(Identifier(pkgId, "UnitMod", "Box"))
    val cmd = Command().withCreate(
      CreateCommand(
        templateId = tmplId,
        createArguments = Some(
          Record(
            tmplId,
            Seq(
              RecordField(value = Some(Value().withUnit(protobuf.empty.Empty()))),
              RecordField(value = Some(Value().withParty(party))),
            ),
          )
        ),
      )
    )
    SubmitAndWaitRequest(
      Some(
        Commands(
          party = party,
          applicationId = applicationId.unwrap,
          ledgerId = ledgerId.unwrap,
          commandId = UUID.randomUUID.toString,
          commands = Seq(cmd),
        )
      )
    )
  }

  private[this] def run(darPath: Path, serverPort: Port) =
    (
      for {
        channel <- GrpcClientResource.owner(serverPort).acquire().asFuture
        client <- ledger.client.LedgerClient.apply(channel, ledgerClientConfiguration)
        darContent = protobuf.ByteString.copyFrom(Files.readAllBytes(darPath))
        pkgsBefore <- client.packageManagementClient.listKnownPackages()
        _ = pkgsBefore shouldBe empty
        _ <- client.packageManagementClient.uploadDarFile(darContent)
        pkgsAfter <- client.packageManagementClient.listKnownPackages()
        _ = pkgsAfter.size shouldBe 1
        // Uploading the package is not enough.
        // We have to submit a request that forces the engine to load the package.
        request = buildRequest(pkgsAfter.head.packageId, client.ledgerId)
        resp <- client.commandServiceClient.submitAndWaitForTransactionId(request)
      } yield Success(resp.transactionId)
    ).recover { case x => Failure(x) }

  "SandboxServer" should {
    def buildServer(versions: VersionRange[LanguageVersion]) = {

      def sandboxConfig(): NewSandboxServer.CustomConfig = NewSandboxServer.CustomConfig(
        genericConfig = com.daml.ledger.runner.common.Config.SandboxDefault.copy(
          ledgerId = "ledger-server",
          engine = com.daml.ledger.runner.common.Config.SandboxDefault.engine.copy(
            allowedLanguageVersions = versions
          ),
          participants = Map(
            SandboxParticipantId -> SandboxParticipantConfig.copy(apiServer =
              SandboxParticipantConfig.apiServer.copy(
                seeding = Seeding.Weak
              )
            )
          ),
          dataSource = Map(
            SandboxParticipantId -> ParticipantDataSourceConfig(
              ConfigConverter.defaultH2SandboxJdbcUrl()
            )
          ),
        ),
        bridgeConfig = BridgeConfig(),
      )

      NewSandboxServer.owner(
        sandboxConfig()
      )
    }

    def load(langVersion: LanguageVersion, mode: VersionRange[LanguageVersion]) =
      buildServer(mode).use(
        run(Paths.get(rlocation(s"daml-lf/encoder/test-${langVersion.pretty}.dar")), _)
      )

    def accept(langVersion: LanguageVersion, range: VersionRange[LanguageVersion], mode: String) =
      s"accept LF ${langVersion.pretty} when $mode mode is used" in
        load(langVersion, range).map {
          inside(_) { case Success(_) =>
            succeed
          }
        }

    def reject(langVersion: LanguageVersion, range: VersionRange[LanguageVersion], mode: String) =
      s"reject LF ${langVersion.pretty} when $mode mode is used" in
        load(langVersion, range).map {
          inside(_) { case Failure(exception) =>
            exception.getMessage should include("Disallowed language version")
          }
        }

    accept(maxStableVersion, LanguageVersion.StableVersions, "stable")
    accept(maxStableVersion, LanguageVersion.EarlyAccessVersions, "early access")
    accept(maxStableVersion, LanguageVersion.DevVersions, "dev")

    if (LanguageVersion.EarlyAccessVersions != LanguageVersion.StableVersions) {
      // a preview version is currently available
      reject(previewVersion, LanguageVersion.StableVersions, "stable")
      accept(previewVersion, LanguageVersion.EarlyAccessVersions, "early access")
      accept(previewVersion, LanguageVersion.DevVersions, "dev")
    }

    reject(devVersion, LanguageVersion.StableVersions, "stable")
    reject(devVersion, LanguageVersion.EarlyAccessVersions, "early access")
    accept(devVersion, LanguageVersion.DevVersions, "dev")

  }

}
