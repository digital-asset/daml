// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.commands.{Command, Commands, CreateCommand}
import com.daml.ledger.api.v2.value.{Identifier, Record, RecordField, Value}
import com.daml.ledger.resources.ResourceContext
import com.daml.ports.Port
import com.digitalasset.canton.ProtocolVersionChecksFixtureAnyWordSpec
import com.digitalasset.canton.config.AuthServiceConfig.Wildcard
import com.digitalasset.canton.config.{CantonConfig, DbConfig}
import com.digitalasset.canton.integration.ConfigTransforms.{
  enableAlphaVersionSupport,
  setBetaSupport,
}
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.CantonFixtureIsolated
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentSetupPlugin}
import com.digitalasset.canton.ledger.api.grpc.GrpcClientResource
import com.digitalasset.canton.ledger.api.refinements.ApiTypes.UserId
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.testing.utils.TestModels
import com.digitalasset.canton.util.JarResourceUtils
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.google.protobuf
import monocle.macros.syntax.lens.*

import java.nio.file.{Files, Path}
import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

abstract class BaseEngineModeIT(supportDevLanguageVersions: Boolean)
    extends CantonFixtureIsolated
    with ProtocolVersionChecksFixtureAnyWordSpec {

  registerPlugin(EngineModePlugin(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  private[this] val userId = UserId("EngineModeIT")

  private[this] val party = UUID.randomUUID.toString

  private[this] implicit def resourceContext(implicit ec: ExecutionContext): ResourceContext =
    ResourceContext(ec)

  private[this] def ledgerClientConfiguration =
    LedgerClientConfiguration(
      userId = UserId.unwrap(userId),
      commandClient = CommandClientConfiguration.default,
      token = () => None,
    )

  private[this] def buildRequest(pkgId: String, party: String) = {
    import scalaz.syntax.tag.*
    val tmplId = Some(Identifier(pkgId, "UnitMod", "Box"))
    val cmd = Command.defaultInstance.withCreate(
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
    Commands.defaultInstance.copy(
      actAs = Seq(party),
      userId = userId.unwrap,
      commandId = UUID.randomUUID.toString,
      commands = Seq(cmd),
    )
  }

  private[this] def run(darPath: Path, serverPort: Port)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
  ) =
    GrpcClientResource.owner(serverPort).use { channel =>
      (
        for {
          client <- LedgerClient.apply(channel, ledgerClientConfiguration, loggerFactory)
          darContent = protobuf.ByteString.copyFrom(Files.readAllBytes(darPath))
          pkgsBefore <- client.packageManagementClient.listKnownPackages()
          sizeBefore = pkgsBefore.size
          _ <- client.packageManagementClient.uploadDarFile(darContent)
          pkgsAfter <- client.packageManagementClient.listKnownPackages()
          _ = pkgsAfter.size shouldBe sizeBefore + 1
          partyDetails <- client.partyManagementClient.allocateParty(Some(party))
          // Uploading the package is not enough.
          // We have to submit a request that forces the engine to load the package.
          request = buildRequest(
            (pkgsAfter diff pkgsBefore).head.packageId,
            partyDetails.party,
          )
          resp <- client.commandService.submitAndWaitForTransaction(request)
        } yield {
          resp.fold(
            err => Failure(new RuntimeException(err.message)),
            res => Success(res.getTransaction.updateId),
          )

        }
      ).recover { case x => Failure(x) }
    }

  "SandboxServer" should {
    def load(langVersion: LanguageVersion, serverPort: Port)(implicit
        ec: ExecutionContext,
        esf: ExecutionSequencerFactory,
    ) =
      run(
        JarResourceUtils
          .resourceFile(TestModels.daml_lf_encoder_test_dar(langVersion.pretty))
          .toPath,
        serverPort,
      )

    def accept(langVersion: LanguageVersion, version: String, mode: String) = {
      val protocolVersion =
        if (LanguageVersion.StableVersions(LanguageMajorVersion.V2).contains(langVersion))
          ProtocolVersion.latest
        else if (LanguageVersion.EarlyAccessVersions(LanguageMajorVersion.V2).contains(langVersion))
          ProtocolVersion.beta.lastOption.getOrElse(ProtocolVersion.dev)
        else if (supportDevLanguageVersions) ProtocolVersion.dev
        else fail(s"Unsupported language version: $langVersion")

      s"accept LF ${langVersion.pretty} ($version) when $mode mode is used" onlyRunWith protocolVersion in {
        env =>
          import env.*
          val serverPort = participant1.config.ledgerApi.clientConfig.port
          load(langVersion, Port(serverPort.unwrap)).map {
            inside(_) { case Success(_) => succeed }
          }.futureValue
      }
    }
    def reject(langVersion: LanguageVersion, version: String, mode: String) =
      s"reject LF ${langVersion.pretty} ($version) when $mode mode is used" onlyRunWith ProtocolVersion.latest in {
        env =>
          import env.*
          val serverPort = participant1.config.ledgerApi.clientConfig.port
          load(langVersion, Port(serverPort.unwrap)).map {
            inside(_) { case Failure(exception) =>
              exception.getMessage should include("Disallowed language version")
            }
          }.futureValue
      }

    inside(
      List(
        LanguageVersion.StableVersions(LanguageVersion.Major.V2).max,
        LanguageVersion.EarlyAccessVersions(LanguageVersion.Major.V2).max,
        LanguageVersion.v2_dev,
      )
    ) { case List(maxStableVersion, betaVersion, devVersion) =>
      if (!supportDevLanguageVersions) {
        accept(maxStableVersion, "stable", "stable")
        if (
          LanguageVersion.EarlyAccessVersions(LanguageVersion.Major.V2) == LanguageVersion
            .StableVersions(LanguageVersion.Major.V2)
        )
          accept(betaVersion, "beta", "beta")
        else
          reject(betaVersion, "beta", "stable")
        // TODO(i15561): re-enable once there is a stable Daml 3 protocol version
        // reject(devVersion, "dev", "stable")
      } else {
        accept(maxStableVersion, "stable", "dev")
        accept(betaVersion, "beta", "dev")
        accept(devVersion, "dev", "dev")
      }

    }
  }

  case class EngineModePlugin(
      protected val loggerFactory: NamedLoggerFactory
  ) extends EnvironmentSetupPlugin {
    override def beforeEnvironmentCreated(
        config: CantonConfig
    ): CantonConfig = {
      val noAuth = ConfigTransforms
        .updateParticipantConfig("participant1")(
          _.focus(_.ledgerApi.authServices).replace(Seq(Wildcard))
        )
      val transforms =
        Seq(noAuth) ++ (if (supportDevLanguageVersions)
                          enableAlphaVersionSupport
                        else Nil) ++ setBetaSupport(testedProtocolVersion.isBeta)
      transforms.foldLeft(config)((cfg, transform) => transform(cfg))
    }
  }

}

class StableEngineModeIT extends BaseEngineModeIT(supportDevLanguageVersions = false)

class DevEngineModeIT extends BaseEngineModeIT(supportDevLanguageVersions = true)
