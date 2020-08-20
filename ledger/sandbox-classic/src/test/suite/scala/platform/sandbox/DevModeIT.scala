// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.services.SandboxFixture
import com.daml.ports.Port
import com.google.protobuf

import scala.util.{Failure, Success}

class DevModeIT
    extends org.scalatest.AsyncWordSpec
    with org.scalatest.Matchers
    with SandboxFixture {

  private[this] implicit val esf: ExecutionSequencerFactory =
    new SingleThreadExecutionSequencerPool("testSequencerPool")

  val List(stableDar, devDar) =
    List("1.8", "1.dev").map { s =>
      Paths.get(rlocation(s"daml-lf/encoder/test-$s.dar"))
    }

  private[this] val applicationId = ApplicationId("DevModeIT")

  private[this] def ledgerClientConfiguration =
    ledger.client.configuration.LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(applicationId),
      ledgerIdRequirement = ledger.client.configuration.LedgerIdRequirement.none,
      commandClient = ledger.client.configuration.CommandClientConfiguration.default,
      sslContext = None,
      token = None
    )

  private[this] def buildServer(devMode: Boolean) =
    SandboxServer.owner(
      SandboxConfig.defaultConfig.copy(
        port = Port.Dynamic,
        devMode = devMode,
      ))

  def buildRequest(pkgId: String, ledgerId: LedgerId) = {
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
              RecordField(value = Some(Value().withParty(party)))
            ),
          ))
      ))
    SubmitAndWaitRequest(
      Some(
        Commands(
          party = party,
          applicationId = applicationId.unwrap,
          ledgerId = ledgerId.unwrap,
          commandId = UUID.randomUUID.toString,
          commands = Seq(cmd)
        )))
  }

  private[this] def test(darPath: Path, server: SandboxServer) =
    (
      for {
        channel <- GrpcClientResource.owner(server.port).acquire().asFuture
        client <- ledger.client.LedgerClient.apply(channel, ledgerClientConfiguration)
        darContent = protobuf.ByteString.copyFrom(Files.readAllBytes(darPath))
        _ <- client.packageManagementClient.uploadDarFile(darContent)
        pkgs <- client.packageManagementClient.listKnownPackages()
        request = buildRequest(pkgs.head.packageId, client.ledgerId)
        resp <- client.commandServiceClient.submitAndWaitForTransactionId(request)
      } yield Success(resp.transactionId)
    ).recover { case x => Failure(x) }

  "SandboxServer" should {

    "accept stable DAML LF when devMode is disable" in {
      buildServer(devMode = false).use(test(stableDar, _)).map(_ shouldBe a[Success[_]])
    }

    "accept stable DAML LF when devMode is enable" in {
      buildServer(devMode = true).use(test(stableDar, _)).map(_ shouldBe a[Success[_]])
    }

    "reject dev DAML LF when devMode is disable" in {
      buildServer(devMode = false).use(test(devDar, _)).map { res =>
        res shouldBe a[Failure[_]]
        res.asInstanceOf[Failure[Nothing]].exception.getMessage should include(
          "Disallowed language version")
      }
    }

    "accept dev DAML LF when devMode is enable" in {
      buildServer(devMode = true).use(test(devDar, _)).map(_ shouldBe a[Success[_]])
    }

  }

}
