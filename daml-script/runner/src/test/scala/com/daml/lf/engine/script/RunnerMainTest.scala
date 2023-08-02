// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import akka.actor.ActorSystem
import com.daml.bazeltools.BazelRunfiles
import com.daml.ledger.api.tls.TlsConfiguration
import org.scalatest.Inspectors
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Path, Paths}

class RunnerMainTest extends AnyFreeSpec with Matchers with Inspectors {

  import RunnerMainTest._

  implicit val system: ActorSystem = ActorSystem()
  implicit val ec = system.dispatcher

  "RunnerMain should not crash" - {
    "with given configurations" in {
      forAll(Seq(configLedgerParticipant, configNodeParticipants, configIdeLedgerParticipant)) {
        clientConfig =>
          clientConfig.toRunnerMainConfig shouldBe Symbol("right")
      }
    }
  }
}

object RunnerMainTest {
  val localHost: String = "localhost"
  val ledgerPort: Int = 8080
  val participantPort: Int = 6865
  val darFilePath: Path =
    BazelRunfiles.rlocation(Paths.get("daml-script/runner/src/test/resources/dummy.dar"))
  val participantConfigPath: Path =
    BazelRunfiles.rlocation(
      Paths.get("daml-script/runner/src/test/resources/participantConfig.json")
    )
  val configLedgerParticipant: RunnerMainConfigIntermediate = RunnerMainConfigIntermediate(
    darPath = darFilePath.toFile,
    mode = Some(RunnerMainConfigIntermediate.CliMode.RunOne("Main:setup")),
    ledgerHost = Some(localHost),
    ledgerPort = Some(ledgerPort),
    participantConfig = None,
    isIdeLedger = false,
    timeMode = Some(RunnerMainConfig.DefaultTimeMode),
    inputFile = None,
    outputFile = None,
    accessTokenFile = None,
    tlsConfig = TlsConfiguration(enabled = false, None, None, None),
    jsonApi = false,
    maxInboundMessageSize = RunnerMainConfig.DefaultMaxInboundMessageSize,
    applicationId = None,
    uploadDar = None,
  )
  val configNodeParticipants: RunnerMainConfigIntermediate = RunnerMainConfigIntermediate(
    darPath = darFilePath.toFile,
    mode = Some(RunnerMainConfigIntermediate.CliMode.RunOne("Main:setup")),
    ledgerHost = None,
    ledgerPort = None,
    participantConfig = Some(participantConfigPath.toFile),
    isIdeLedger = false,
    timeMode = Some(RunnerMainConfig.DefaultTimeMode),
    inputFile = None,
    outputFile = None,
    accessTokenFile = None,
    tlsConfig = TlsConfiguration(enabled = false, None, None, None),
    jsonApi = false,
    maxInboundMessageSize = RunnerMainConfig.DefaultMaxInboundMessageSize,
    applicationId = None,
    uploadDar = None,
  )
  val configIdeLedgerParticipant: RunnerMainConfigIntermediate = RunnerMainConfigIntermediate(
    darPath = darFilePath.toFile,
    mode = Some(RunnerMainConfigIntermediate.CliMode.RunOne("Main:setup")),
    ledgerHost = None,
    ledgerPort = None,
    participantConfig = None,
    isIdeLedger = true,
    timeMode = Some(RunnerMainConfig.DefaultTimeMode),
    inputFile = None,
    outputFile = None,
    accessTokenFile = None,
    tlsConfig = TlsConfiguration(enabled = false, None, None, None),
    jsonApi = false,
    maxInboundMessageSize = RunnerMainConfig.DefaultMaxInboundMessageSize,
    applicationId = None,
    uploadDar = None,
  )
}
