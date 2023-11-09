// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.app

import java.time.Clock

import akka.actor.ActorSystem
import com.daml.doobie.logging.Slf4jLogHandler
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService
import com.daml.nonrepudiation.api.NonRepudiationApi
import com.daml.nonrepudiation.postgresql.{Tables, createTransactor}
import com.daml.nonrepudiation.{Metrics, NonRepudiationProxy}
import com.daml.resources.akka.AkkaResourceOwnerFactories
import com.daml.resources.{
  AbstractResourceOwner,
  HasExecutionContext,
  ProgramResource,
  ResourceOwnerFactories,
}
import io.grpc.Server
import io.grpc.netty.{NettyChannelBuilder, NettyServerBuilder}

import scala.concurrent.ExecutionContext

object NonRepudiationApp {

  private[app] val Name = "non-repudiation-app"

  private val resourceFactory = new ResourceOwnerFactories[ExecutionContext]
    with AkkaResourceOwnerFactories[ExecutionContext] {
    override protected implicit val hasExecutionContext: HasExecutionContext[ExecutionContext] =
      HasExecutionContext.`ExecutionContext has itself`
  }

  def main(args: Array[String]): Unit = {

    val configuration: Configuration =
      OptionParser.parse(args, Configuration.Default).getOrElse(sys.exit(1))

    val program = new ProgramResource(appOwner(configuration))

    program.run(identity)

  }

  def appOwner(
      configuration: Configuration
  ): AbstractResourceOwner[ExecutionContext, Server] = {

    val participantChannel =
      NettyChannelBuilder.forAddress(configuration.participantAddress).usePlaintext().build()

    val proxyChannelBuilder =
      NettyServerBuilder.forAddress(configuration.proxyAddress)

    for {
      actorSystem <- resourceFactory.forActorSystem(() => ActorSystem(Name))
      transactor <- createTransactor(
        configuration.databaseJdbcUrl,
        configuration.databaseJdbcUsername,
        configuration.databaseJdbcPassword,
        configuration.databaseMaxPoolSize,
        resourceFactory,
      )
      logHandler = Slf4jLogHandler(getClass)
      db = Tables.initialize(transactor)(logHandler)
      _ <- NonRepudiationApi.owner(
        configuration.apiAddress,
        configuration.apiShutdownTimeout,
        db.certificates,
        db.signedPayloads,
        actorSystem,
      )
      metrics <- Metrics.owner(configuration.metricsReportingPeriod)
      proxy <- NonRepudiationProxy.owner(
        participantChannel,
        proxyChannelBuilder,
        db.certificates,
        db.signedPayloads,
        Clock.systemUTC(),
        metrics,
        CommandService.scalaDescriptor.fullName,
        CommandSubmissionService.scalaDescriptor.fullName,
      )
    } yield proxy
  }

}
