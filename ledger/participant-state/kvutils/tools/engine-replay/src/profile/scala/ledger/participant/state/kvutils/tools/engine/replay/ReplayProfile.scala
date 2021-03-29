// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.engine.replay

import java.nio.file.{Path, Paths}

import scopt.{OptionParser, Read}

final case class Config(
    choiceName: String,
    choiceIndex: Option[Int],
    darFile: Path,
    ledgerExport: Path,
    profileDir: Path,
    adapt: Boolean,
)

object Config {
  private implicit val pathRead: Read[Path] = Read.reads(Paths.get(_))

  private val empty: Config = Config(
    choiceName = null,
    choiceIndex = None,
    darFile = null,
    ledgerExport = null,
    profileDir = null,
    adapt = false,
  )

  private val parser: OptionParser[Config] = new scopt.OptionParser[Config]("replay-profile") {
    opt[String]("choice")
      .action((x, c) => c.copy(choiceName = x))
      .text("Choice name in the form Module:Template:Choice")
      .required()
    opt[Int]("choice-index")
      .action((x, c) => c.copy(choiceIndex = Some(x)))
      .text(
        "If the choice got exercised more than once, you can use this to select the nth occurrence of this choice"
      )
    opt[Path]("dar")
      .action((x, c) => c.copy(darFile = x))
      .text("Path to DAR")
      .required()
    opt[Path]("export")
      .text("Path to KVUtils ledger export")
      .action((x, c) => c.copy(ledgerExport = x))
      .required()
    opt[Path]("profile-dir")
      .text("Directory to write profiling results to")
      .action((x, c) => c.copy(profileDir = x))
      .required()
    opt[Unit]("adapt")
      .text("Adapt package ids to the ones found in the DAR")
      .action((_, c) => c.copy(adapt = true))
      .required()
  }

  def parse(args: collection.Seq[String]): Option[Config] =
    parser.parse(args, empty)
}

object ReplayProfile {

  def main(args: Array[String]) = {
    Config.parse(args) match {
      case None => sys.exit(1)
      case Some(config) => run(config)
    }
  }

  def run(config: Config) = {
    val loadedPackages = Replay.loadDar(config.darFile)
    val engine = Replay.compile(loadedPackages, Some(config.profileDir))
    val benchmarks = Replay.loadBenchmarks(config.ledgerExport)
    val originalBenchmark = benchmarks.get(config.choiceName, config.choiceIndex)
    val benchmark = if (config.adapt) {
      Replay.adapt(
        loadedPackages,
        engine.compiledPackages().packageLanguageVersion,
        originalBenchmark,
      )
    } else {
      originalBenchmark
    }
    // Note that we already turn on profiling for this. Profile names
    // are deterministic so the replay run will just overwrite this
    // again. At this point, the engine does not have a way to
    // dynamically turn on or off profiling.
    val validateResult = benchmark.validate(engine)
    validateResult.left.foreach { err =>
      sys.error(s"Error during validation: $err")
    }
    // Run a few times to warm up. Note that each run will overwrite
    // earlier profiles.
    for (_ <- 1 to 10) {
      val replayResult = benchmark.replay(engine)
      replayResult.left.foreach { err =>
        sys.error(s"Error during validation: $err")
      }
    }
  }
}
