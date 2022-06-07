// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.runner.common._
import com.daml.ledger.sandbox.SandboxOnXRunner.run
import scopt.OptionParser

import java.time.Duration

object CliSandboxOnXRunner {
  val RunnerName = "sandbox-on-x"

  def bridgeConfigParser(parser: OptionParser[CliConfig[BridgeConfig]]): Unit = {
    parser
      .opt[Int]("bridge-submission-buffer-size")
      .text("Submission buffer size. Defaults to 500.")
      .action((p, c) => c.copy(extra = c.extra.copy(submissionBufferSize = p)))

    parser
      .opt[Unit]("disable-conflict-checking")
      .hidden()
      .text("Disable ledger-side submission conflict checking.")
      .action((_, c) => c.copy(extra = c.extra.copy(conflictCheckingEnabled = false)))

    parser
      .opt[Boolean](name = "implicit-party-allocation")
      .optional()
      .action((x, c) => c.copy(implicitPartyAllocation = x))
      .text(
        s"When referring to a party that doesn't yet exist on the ledger, the participant will implicitly allocate that party."
          + s" You can optionally disable this behavior to bring participant into line with other ledgers."
      )

    parser.checkConfig(c =>
      Either.cond(
        c.maxDeduplicationDuration.forall(_.compareTo(Duration.ofHours(1L)) <= 0),
        (),
        "Maximum supported deduplication duration is one hour",
      )
    )
    ()
  }

  def owner(
      args: collection.Seq[String],
      manipulateConfig: CliConfig[BridgeConfig] => CliConfig[BridgeConfig] = identity,
  ): ResourceOwner[Unit] =
    CliConfig
      .owner(
        RunnerName,
        bridgeConfigParser,
        BridgeConfig.Default,
        args,
      )
      .map(manipulateConfig)
      .flatMap(owner)

  def owner(originalConfig: CliConfig[BridgeConfig]): ResourceOwner[Unit] =
    new ResourceOwner[Unit] {
      override def acquire()(implicit context: ResourceContext): Resource[Unit] = {
        val configAdaptor: BridgeConfigAdaptor = new BridgeConfigAdaptor
        originalConfig.mode match {
          case Mode.DumpIndexMetadata(jdbcUrls) =>
            DumpIndexMetadata(jdbcUrls)
            sys.exit(0)
          case Mode.Run =>
            val config = CliConfigConverter.toConfig(configAdaptor, originalConfig)
            run(
              configAdaptor,
              config,
              originalConfig.extra.copy(maxDeduplicationDuration =
                originalConfig.maxDeduplicationDuration.getOrElse(
                  BridgeConfig.DefaultMaximumDeduplicationDuration
                )
              ),
            ).map(_ => ())
        }
      }
    }
}
