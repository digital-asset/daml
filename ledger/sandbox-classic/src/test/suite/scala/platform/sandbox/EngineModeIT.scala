// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.lf.language.LanguageVersion
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.services.SandboxFixture
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
      sslContext = None,
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

  private[this] def run(darPath: Path, server: SandboxServer) =
    (
      for {
        channel <- GrpcClientResource.owner(server.port).acquire().asFuture
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

    import SandboxConfig.EngineMode
    import EngineMode._

    def buildServer(mode: SandboxConfig.EngineMode) =
      SandboxServer.owner(
        DefaultConfig.copy(
          port = Port.Dynamic,
          engineMode = mode,
          seeding = Some(Seeding.Weak),
        )
      )

    def load(langVersion: LanguageVersion, mode: EngineMode) =
      buildServer(mode).use(
        run(Paths.get(rlocation(s"daml-lf/encoder/test-${langVersion.pretty}.dar")), _)
      )

    def accept(langVersion: LanguageVersion, mode: EngineMode) =
      s"accept LF ${langVersion.pretty} when $mode mode is used" in
        load(langVersion, mode).map {
          inside(_) { case Success(_) =>
            succeed
          }
        }

    def reject(langVersion: LanguageVersion, mode: EngineMode) =
      s"reject LF ${langVersion.pretty} when $mode mode is used" in
        load(langVersion, mode).map {
          inside(_) { case Failure(exception) =>
            exception.getMessage should include("Disallowed language version")
          }
        }

    accept(maxStableVersion, Stable)
    accept(maxStableVersion, EarlyAccess)
    accept(maxStableVersion, Dev)

    if (LanguageVersion.EarlyAccessVersions != LanguageVersion.StableVersions) {
      // a preview version is currently available
      reject(previewVersion, Stable)
      accept(previewVersion, EarlyAccess)
      accept(previewVersion, Dev)
    }

    reject(devVersion, Stable)
    reject(devVersion, EarlyAccess)
    accept(devVersion, Dev)

  }

}
