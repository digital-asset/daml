// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.MinVersionTest

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Path}
import java.util.stream.Collectors

import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands.{CreateCommand, Command, Commands}
import com.daml.ledger.api.v1.value._
import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  OwnedResource,
  SuiteResource,
  SuiteResourceManagementAroundAll,
}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ledger.on.memory.Owner
import com.daml.ledger.participant.state.kvutils.app.{
  ParticipantConfig,
  ParticipantIndexerConfig,
  ParticipantRunMode,
}
import com.daml.ledger.participant.state.kvutils.{app => kvutils}
import com.daml.ledger.resources.ResourceContext
import com.daml.ledger.test.ModelTestDar
import com.daml.lf.archive.DarDecoder
import com.daml.lf.data.Ref
import com.daml.lf.language.LanguageVersion.v1_14
import com.daml.lf.VersionRange
import com.daml.ports.Port
import com.google.protobuf.ByteString
import java.util.UUID
import org.scalatest.Suite
import org.scalatest.freespec.AsyncFreeSpec
import scalaz.syntax.tag._

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
    sslContext = None,
  )
  // This is an integration test to make sure that the version restrictions for stable packages
  // apply across the whole stack.

  "MinVersionTest" - {
    "can upload an LF 1.14 package and use it in a transaction" in {
      for {
        client <- LedgerClient.singleHost(
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

  private val participantId = Ref.ParticipantId.assertFromString("participant1")
  private val participant = ParticipantConfig(
    mode = ParticipantRunMode.Combined,
    participantId = participantId,
    shardName = None,
    address = Some("localhost"),
    port = Port.Dynamic,
    portFile = Some(portfile),
    serverJdbcUrl = ParticipantConfig.defaultIndexJdbcUrl(participantId),
    indexerConfig = ParticipantIndexerConfig(
      allowExistingSchema = false
    ),
  )
  override protected lazy val suiteResource = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, Port](
      for {
        _ <- Owner(
          kvutils.Config
            .createDefault(())
            .copy(
              participants = Seq(participant),
              archiveFiles = Seq(),
              // Bump min version to 1.14 and check that older stable packages are still accepted.
              allowedLanguageVersions = VersionRange(min = v1_14, max = v1_14),
            )
        )
      } yield (readPortfile(portfile))
    )
  }
}
