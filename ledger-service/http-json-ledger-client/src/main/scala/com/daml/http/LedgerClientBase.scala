// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.client.configuration.LedgerClientConfiguration
import com.daml.util.ExceptionOps._
import com.daml.ledger.client.{LedgerClient => DamlLedgerClient}
import com.typesafe.scalalogging.StrictLogging
import io.grpc.netty.NettyChannelBuilder
import scalaz._
import Scalaz._
import com.daml.timer.RetryStrategy

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal

object LedgerClientBase {

  final case class Error(message: String)

}

trait LedgerClientBase extends StrictLogging {

  protected def channelBuilder(
      ledgerHost: String,
      ledgerPort: Int,
      nonRepudiationConfig: nonrepudiation.Configuration.Cli,
  )(implicit executionContext: ExecutionContext): Future[NettyChannelBuilder]

  private def buildLedgerClient(
      ledgerHost: String,
      ledgerPort: Int,
      clientConfig: LedgerClientConfiguration,
      nonRepudiationConfig: nonrepudiation.Configuration.Cli,
  )(implicit
      ec: ExecutionContext,
      aesf: ExecutionSequencerFactory,
  ): Future[DamlLedgerClient] =
    channelBuilder(
      ledgerHost,
      ledgerPort,
      nonRepudiationConfig,
    ).flatMap(builder => DamlLedgerClient.fromBuilder(builder, clientConfig))

  def fromRetried(
      ledgerHost: String,
      ledgerPort: Int,
      clientConfig: LedgerClientConfiguration,
      nonRepudiationConfig: nonrepudiation.Configuration.Cli,
      maxInitialConnectRetryAttempts: Int,
  )(implicit
      ec: ExecutionContext,
      aesf: ExecutionSequencerFactory,
  ): Future[LedgerClientBase.Error \/ DamlLedgerClient] =
    RetryStrategy
      .constant(maxInitialConnectRetryAttempts, 1.seconds) { (i, _) =>
        logger.info(s"""Attempting to connect to the ledger $ledgerHost:$ledgerPort
           | (attempt $i/$maxInitialConnectRetryAttempts)""".stripMargin)
        buildLedgerClient(ledgerHost, ledgerPort, clientConfig, nonRepudiationConfig)
      }
      .map(_.right)
      .recover { case NonFatal(e) =>
        \/.left(
          LedgerClientBase.Error(
            s"""Maximum initial connection retry attempts($maxInitialConnectRetryAttempts) reached,
               | Cannot connect to ledger server: ${e.description}""".stripMargin
          )
        )
      }

  def apply(
      ledgerHost: String,
      ledgerPort: Int,
      clientConfig: LedgerClientConfiguration,
      nonRepudiationConfig: nonrepudiation.Configuration.Cli,
  )(implicit
      ec: ExecutionContext,
      aesf: ExecutionSequencerFactory,
  ): Future[LedgerClientBase.Error \/ DamlLedgerClient] =
    buildLedgerClient(ledgerHost, ledgerPort, clientConfig, nonRepudiationConfig)
      .map(_.right)
      .recover { case NonFatal(e) =>
        \/.left(
          LedgerClientBase.Error(s"Cannot connect to the ledger server, error: ${e.description}")
        )
      }

}
