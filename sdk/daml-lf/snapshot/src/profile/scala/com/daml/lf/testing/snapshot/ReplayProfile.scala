// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.digitalasset.daml.lf.data.Ref
import scopt.{OptionParser, Read}

import java.nio.file.{Path, Paths}

final case class Config(
    choiceName: String,
    choiceIndex: Option[Int],
    darFile: Option[Path],
    ledgerEntries: Path,
    profileDir: Path,
)

object Config {
  private implicit val pathRead: Read[Path] = Read.reads(Paths.get(_))

  private val empty: Config = Config(
    choiceName = null,
    choiceIndex = None,
    darFile = None,
    ledgerEntries = null,
    profileDir = null,
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
      .action((x, c) => c.copy(darFile = Some(x)))
      .text("Path to DAR")
      .optional()
    opt[Path]("entries")
      .text("Path to submission entries file")
      .action((x, c) => c.copy(ledgerEntries = x))
      .required()
    opt[Path]("profile-dir")
      .text("Directory to write profiling results to")
      .action((x, c) => c.copy(profileDir = x))
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
    val Array(modNameStr, tmplNameStr, name) = config.choiceName.split(":")
    val choice = (
      Ref.QualifiedName(
        Ref.DottedName.assertFromString(modNameStr),
        Ref.DottedName.assertFromString(tmplNameStr),
      ),
      Ref.Name.assertFromString(name),
    )
    val originalBenchmark =
      TransactionSnapshot.loadBenchmark(config.ledgerEntries, choice, 0, Some(config.profileDir))
    val benchmark = config.darFile match {
      case Some(path) =>
        val loadedPackages = TransactionSnapshot.loadDar(path)
        originalBenchmark.adapt(loadedPackages)
      case None =>
        originalBenchmark
    }

    // Note that we already turn on profiling for this. Profile names
    // are deterministic so the replay run will just overwrite this
    // again. At this point, the engine does not have a way to
    // dynamically turn on or off profiling.
    val validateResult = benchmark.validate()
    validateResult.left.foreach { err =>
      sys.error(s"Error during validation: $err")
    }
    // Run a few times to warm up. Note that each run will overwrite
    // earlier profiles.
    for (_ <- 1 to 10) {
      val replayResult = benchmark.replay()
      replayResult.left.foreach { err =>
        sys.error(s"Error during validation: $err")
      }
    }
  }
}
