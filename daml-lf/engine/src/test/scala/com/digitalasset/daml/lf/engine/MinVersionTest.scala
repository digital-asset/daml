// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.MinVersionTest

import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  OwnedResource,
  SuiteResource,
  SuiteResourceManagementAroundAll,
}
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands.{Command, Commands, CreateCommand}
import com.daml.ledger.api.v1.value._
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ledger.resources.ResourceContext
import com.daml.ledger.runner.common._
import com.daml.ledger.sandbox.{BridgeConfig, BridgeConfigAdaptor, SandboxOnXRunner}
import com.daml.ledger.test.ModelTestDar
import com.daml.lf.VersionRange
import com.daml.lf.archive.DarDecoder
import com.daml.lf.language.LanguageVersion.v1_14
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.ports.Port
import com.google.protobuf.ByteString
import org.scalatest.Suite
import org.scalatest.freespec.AsyncFreeSpec
import scalaz.syntax.tag._

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Path}
import java.util.UUID
import java.util.stream.Collectors

final class MinVersionTest
    extends AsyncFreeSpec
    with SuiteResource[Port]
    with AkkaBeforeAndAfterAll
    with SuiteResourceManagementAroundAll {
  self: Suite =>
  private val darFile = new File(rlocation(ModelTestDar.path))
  private val dar = DarDecoder.assertReadArchiveFromFile(darFile)

  private val tmpDir = Files.createTempDirectory("testMultiParticipantFixture")
  private val portfile = tmpDir.resolve("portfile")

  override protected def afterAll(): Unit = {
    Files.delete(portfile)
    super.afterAll()

  }

  private def readPortfile(f: Path): Port = {
    Port(Integer.parseInt(Files.readAllLines(f).stream.collect(Collectors.joining("\n"))))
  }

  private val ledgerClientConfig = LedgerClientConfiguration(
    applicationId = "minversiontest",
    ledgerIdRequirement = LedgerIdRequirement.none,
    commandClient = CommandClientConfiguration.default,
  )
  // This is an integration test to make sure that the version restrictions for stable packages
  // apply across the whole stack.

  "MinVersionTest" - {
    "can upload an LF 1.14 package and use it in a transaction" in {
      for {
        client <- LedgerClient.insecureSingleHost(
          "localhost",
          suiteResource.value.value,
          ledgerClientConfig,
        )
        darByteString = ByteString.readFrom(new FileInputStream(darFile))
        _ <- client.packageManagementClient.uploadDarFile(darByteString)
        party <- client.partyManagementClient
          .allocateParty(hint = None, displayName = None)
          .map(_.party)
        _ <- client.commandServiceClient.submitAndWaitForTransaction(
          SubmitAndWaitRequest(
            Some(
              Commands(
                ledgerId = client.ledgerId.unwrap,
                applicationId = "minversiontest",
                commandId = UUID.randomUUID.toString,
                party = party,
                commands = Seq(
                  Command().withCreate(
                    CreateCommand(
                      templateId = Some(
                        Identifier(
                          packageId = dar.main._1,
                          moduleName = "Iou",
                          entityName = "Iou",
                        )
                      ),
                      createArguments = Some(
                        Record(
                          None,
                          Seq(
                            RecordField("issuer", Some(Value().withParty(party))),
                            RecordField("owner", Some(Value().withParty(party))),
                            RecordField("currency", Some(Value().withText("EUR"))),
                            RecordField("amount", Some(Value().withNumeric("10.0"))),
                            RecordField("observers", Some(Value().withList(List()))),
                          ),
                        )
                      ),
                    )
                  )
                ),
              )
            )
          )
        )
      } yield succeed
    }
  }
  private val configAdaptor = new BridgeConfigAdaptor()

  override protected lazy val suiteResource: OwnedResource[ResourceContext, Port] = {
    val jdbcUrl = s"jdbc:h2:mem:default;db_close_delay=-1;db_close_on_exit=false"

    val config = Config.Default.copy(
      engine = Config.DefaultEngineConfig
        .copy(allowedLanguageVersions = VersionRange(min = v1_14, max = v1_14)),
      dataSource = Config.Default.participants.map { case (key, _) =>
        key -> ParticipantDataSourceConfig(jdbcUrl)
      },
      participants = Config.Default.participants.map { case (key, value) =>
        key -> value.copy(
          apiServer = value.apiServer.copy(
            portFile = Some(portfile),
            port = Port.Dynamic,
            address = Some("localhost"),
            initialLedgerConfiguration = Some(configAdaptor.initialLedgerConfig(None)),
          )
        )
      },
    )
    val bridgeConfig = BridgeConfig.Default

    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, Port](
      for {
        _ <- SandboxOnXRunner.owner(configAdaptor, config, bridgeConfig)
      } yield readPortfile(portfile)
    )
  }
}
