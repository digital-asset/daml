// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import akka.actor.ActorSystem
import com.daml.ledger.api.tls.TlsConfiguration
import org.scalatest.Inspectors
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class RunnerMainTest extends AnyFreeSpec with Matchers with Inspectors {

  import RunnerMainTest._

  implicit val system: ActorSystem = ActorSystem()
  implicit val ec = system.dispatcher

  "RunnerMain should not crash" - {
    "with given configurations" in {
      forAll(Seq(configLedgerParticipant, configNodeParticipants)) { clientConfig =>
        RunnerMain.VerifiedRunnerConfig(clientConfig) shouldBe Symbol("success")
      }
    }
  }
}

object RunnerMainTest {
  val localHost: String = "localhost"
  val ledgerPort: Int = 8080
  val participantPort: Int = 6865
  val configLedgerParticipant: RunnerConfig = RunnerConfig(
    darPath = new File("./daml-script/runner/src/test/resources/dummy.dar"),
    scriptIdentifier = "Main:setup",
    ledgerHost = Some(localHost),
    ledgerPort = Some(ledgerPort),
    participantConfig = None,
    timeMode = ScriptConfig.DefaultTimeMode,
    inputFile = None,
    outputFile = None,
    accessTokenFile = None,
    tlsConfig = TlsConfiguration(enabled = false, None, None, None),
    jsonApi = false,
    maxInboundMessageSize = ScriptConfig.DefaultMaxInboundMessageSize,
    applicationId = None,
  )
  val configNodeParticipants: RunnerConfig = RunnerConfig(
    darPath = new File("./daml-script/runner/src/test/resources/dummy.dar"),
    scriptIdentifier = "Main:setup",
    ledgerHost = None,
    ledgerPort = None,
    participantConfig =
      Some(new File("./daml-script/runner/src/test/resources/participantConfig.json")),
    timeMode = ScriptConfig.DefaultTimeMode,
    inputFile = None,
    outputFile = None,
    accessTokenFile = None,
    tlsConfig = TlsConfiguration(enabled = false, None, None, None),
    jsonApi = false,
    maxInboundMessageSize = ScriptConfig.DefaultMaxInboundMessageSize,
    applicationId = None,
  )
}
