// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package script
package export

import com.daml.bazeltools.BazelRunfiles
import com.daml.integrationtest.CantonFixture
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.lf.data.{Ref, FrontStack}
import com.daml.lf.engine.script._
import com.daml.lf.engine.script.ledgerinteraction.{GrpcLedgerClient, ScriptTimeMode}
import com.daml.lf.engine.script.test.CompiledDar
import com.daml.lf.speedy.SValue
import com.daml.platform.services.time.TimeProviderType
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import java.nio.file._

final class DamlExportIt
    extends AsyncWordSpec
    with Matchers
    with SuiteResourceManagementAroundAll
    with CantonFixture {

  final override protected lazy val darFiles = List(darPath)
  final override protected lazy val timeProviderType = TimeProviderType.Static

  private[this] lazy val darPath =
    Paths.get(BazelRunfiles.rlocation("daml-script/export/it/test.dar"))
  private[this] lazy val dar = CompiledDar.read(darPath)
  private[this] lazy val ledgerPort = suiteResource.value._1.head

  private[this] def allocateParty(partyPrefix: String) =
    for {
      client <- defaultLedgerClient()
      partyHint = CantonFixture.freshName(partyPrefix)
      partyDetails <- client.partyManagementClient.allocateParty(Some(partyHint), Some(partyHint))
    } yield partyDetails.party

  private[this] def actualOutput(
      dir: Path,
      name: String,
      partiesMapping: Iterable[(String, Ref.Party)],
  ) = {
    val original = Files.readAllLines(dir.resolve(name)).asScala.mkString("\n")
    val partyReplaced = partiesMapping.foldLeft(original) { case (text, (prefix, party)) =>
      text.replace(party.take(party.lastIndexOf("::")), prefix)
    }
    val partiesAndHashesReplaced = partyReplaced.replaceAll("""[\da-f]{64,68}""", "XXXXXXXX")
    partiesAndHashesReplaced
  }

  private[this] def converter[X]: (SValue, X) => Either[Nothing, SValue] = { case (v, _) =>
    Right(v)
  }

  private[this] def test(
      name: String,
      scriptIdentifier: (String, String),
      partyPrefixes: List[String],
      expectedFilesDir: String,
  ) = s"export properly $name" in {
    for {
      client <- defaultLedgerClient()
      parties <- Future.sequence(partyPrefixes.map(allocateParty))
      partiesMapping = partyPrefixes zip parties
      response <- client.transactionClient.getLedgerEnd()
      startOffset <- Future(response.offset.get)
      entry = Runner.run(
        compiledPackages = dar.compiledPackages,
        scriptId = dar.id(scriptIdentifier),
        convertInputValue = Some(converter),
        inputValue = Some(SValue.SList(parties.view.map(SValue.SParty).to(FrontStack))),
        initialClients =
          Participants(Some(new GrpcLedgerClient(client, applicationId)), Map.empty, Map.empty),
        timeMode = ScriptTimeMode.Static,
      )
      (_, future) = entry
      _ <- future
      response <- client.transactionClient.getLedgerEnd()
      endOffset <- Future(response.offset.get)
      outputDir = Files.createTempDirectory(getClass.getSimpleName)
      exportConfig = Config.Empty.copy(
        ledgerHost = "localhost",
        ledgerPort = ledgerPort.value,
        partyConfig = PartyConfig(
          allParties = false,
          parties = parties.map(ApiTypes.Party[String]),
        ),
        exportType = Some(
          Config.EmptyExportScript.copy(
            sdkVersion = SdkVersion.sdkVersion,
            outputPath = outputDir,
          )
        ),
        start = startOffset,
        end = endOffset,
      )
      _ <- Main.run(exportConfig)
      actualExportDaml = actualOutput(outputDir, "Export.daml", partiesMapping)
      actualArgsJson = actualOutput(outputDir, "args.json", partiesMapping)
      actualDamlYaml = actualOutput(outputDir, "daml.yaml", partiesMapping)
      expectedExportDaml =
        Files.readString(Paths.get(BazelRunfiles.rlocation(expectedFilesDir + "Export.daml")))
      expectedArgsJson =
        Files.readString(Paths.get(BazelRunfiles.rlocation(expectedFilesDir + "args.json")))
      expectedDamlYaml =
        Files.readString(Paths.get(BazelRunfiles.rlocation(expectedFilesDir + "daml.yaml")))
    } yield {
      actualExportDaml shouldBe expectedExportDaml
      actualArgsJson shouldBe expectedArgsJson
      actualDamlYaml shouldBe expectedDamlYaml
    }
  }

  "ledger export" should {
    test(
      "values",
      ("Values", "initialize"),
      List("Bank"),
      "daml-script/export/it/src/test/data/values/",
    )

    test(
      "interface",
      ("InterfaceChoices", "initialize"),
      List("Alice", "Bob", "Charlie"),
      "daml-script/export/it/src/test/data/interface/",
    )
  }

}
