// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.client.configuration.{
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
}
import com.daml.scalautil.ExceptionOps._
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
import io.grpc.netty.NettyChannelBuilder
import scalaz._
import Scalaz._
import com.daml.http.util.Logging.InstanceUUID
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.daml.timer.RetryStrategy

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

object LedgerClientBase {

  final case class Error(message: String)

}

trait LedgerClientBase {

  private val logger = ContextualizedLogger.get(getClass)

  protected def channelBuilder(
      ledgerHost: String,
      ledgerPort: Int,
      clientChannelConfig: LedgerClientChannelConfiguration,
  )(implicit executionContext: ExecutionContext): Future[NettyChannelBuilder]

  private def buildLedgerClient(
      ledgerHost: String,
      ledgerPort: Int,
      clientConfig: LedgerClientConfiguration,
      clientChannelConfig: LedgerClientChannelConfiguration,
  )(implicit
      ec: ExecutionContext,
      aesf: ExecutionSequencerFactory,
  ): Future[DamlLedgerClient] =
    channelBuilder(
      ledgerHost,
      ledgerPort,
      clientChannelConfig,
    ).map(builder => DamlLedgerClient.fromBuilder(builder, clientConfig))

  def fromRetried(
      ledgerHost: String,
      ledgerPort: Int,
      clientConfig: LedgerClientConfiguration,
      clientChannelConfig: LedgerClientChannelConfiguration,
      maxInitialConnectRetryAttempts: Int,
  )(implicit
      ec: ExecutionContext,
      aesf: ExecutionSequencerFactory,
      lc: LoggingContextOf[InstanceUUID],
  ): Future[LedgerClientBase.Error \/ DamlLedgerClient] = {
    logger.info(
      s"Attempting to connect to the ledger $ledgerHost:$ledgerPort ($maxInitialConnectRetryAttempts attempts)"
    )
    RetryStrategy
      .constant(maxInitialConnectRetryAttempts, 1.seconds) { (i, _) =>
        val client = buildLedgerClient(
          ledgerHost,
          ledgerPort,
          clientConfig,
          clientChannelConfig,
        )
        client.onComplete {
          case Success(_) =>
            logger.info(s"""Attempt $i/$maxInitialConnectRetryAttempts succeeded!""")
          case Failure(e) =>
            logger.info(s"""Attempt $i/$maxInitialConnectRetryAttempts failed: ${e.description}""")
        }
        client
      }
      .map(_.right)
      .recover { case NonFatal(e) =>
        \/.left(
          LedgerClientBase.Error(
            s"""Maximum initial connection retry attempts ($maxInitialConnectRetryAttempts) reached,
               | Cannot connect to ledger server: ${e.description}""".stripMargin
          )
        )
      }
  }

  def apply(
      ledgerHost: String,
      ledgerPort: Int,
      clientConfig: LedgerClientConfiguration,
      clientChannelConfig: LedgerClientChannelConfiguration,
  )(implicit
      ec: ExecutionContext,
      aesf: ExecutionSequencerFactory,
  ): Future[LedgerClientBase.Error \/ DamlLedgerClient] =
    buildLedgerClient(
      ledgerHost,
      ledgerPort,
      clientConfig,
      clientChannelConfig,
    )
      .map(_.right)
      .recover { case NonFatal(e) =>
        \/.left(
          LedgerClientBase.Error(s"Cannot connect to the ledger server, error: ${e.description}")
        )
      }

}
