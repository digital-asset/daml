// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import com.daml.platform.config.ParticipantConfig

// Outputs the default config to a file or to the stdout
object DefaultConfigGenApp {

  def main(args: Array[String]): Unit = {
    val text = genText()
    if (args.length >= 1) {
      val outputFile = Paths.get(args(0))
      val _ = Files.write(
        outputFile,
        text.getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE_NEW,
      )
    } else {
      println(text)
    }
  }

  def genText(): String = {
    val config = SandboxOnXConfig()
    val participantConfig = config.ledger.participants(ParticipantConfig.DefaultParticipantId)
    val updatedParticipantConfig = participantConfig.copy(servicesThreadPoolSize = 1337)
    val updatedConfig = config.copy(
      ledger = config.ledger.copy(
        participants = config.ledger.participants
          .updated(ParticipantConfig.DefaultParticipantId, updatedParticipantConfig)
      )
    )
    val text = ConfigRenderer.render(updatedConfig)
    text
  }
}
