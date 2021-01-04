// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.nio.file.Files

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.app._
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.resources.ProgramResource
import scopt.OptionParser

object MainWithEphemeralDirectory {
  private val DirectoryPattern = "%DIR"

  def main(args: Array[String]): Unit = {
    new ProgramResource(new Runner("SQL Ledger", TestLedgerFactory).owner(args))
      .run(ResourceContext.apply)
  }

  object TestLedgerFactory extends LedgerFactory[ReadWriteService, ExtraConfig] {
    override val defaultExtraConfig: ExtraConfig = SqlLedgerFactory.defaultExtraConfig

    override def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit =
      SqlLedgerFactory.extraConfigParser(parser)

    override def manipulateConfig(config: Config[ExtraConfig]): Config[ExtraConfig] =
      SqlLedgerFactory.manipulateConfig(config)

    override def readWriteServiceOwner(
        config: Config[ExtraConfig],
        participantConfig: ParticipantConfig,
        engine: Engine,
    )(
        implicit materializer: Materializer,
        loggingContext: LoggingContext,
    ): ResourceOwner[ReadWriteService] =
      new Owner(config, participantConfig, engine)

    class Owner(
        config: Config[ExtraConfig],
        participantConfig: ParticipantConfig,
        engine: Engine,
    )(implicit materializer: Materializer, loggingContext: LoggingContext)
        extends ResourceOwner[ReadWriteService] {
      override def acquire()(implicit context: ResourceContext): Resource[ReadWriteService] = {
        val directory = Files.createTempDirectory("ledger-on-sql-ephemeral-")
        val jdbcUrl = config.extra.jdbcUrl.map(_.replace(DirectoryPattern, directory.toString))
        SqlLedgerFactory
          .readWriteServiceOwner(
            config.copy(extra = config.extra.copy(jdbcUrl = jdbcUrl)),
            participantConfig,
            engine,
          )
          .acquire()
      }
    }

  }
}
