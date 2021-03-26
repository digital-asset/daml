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

import scala.concurrent.{ExecutionContext, Future}
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

  def apply(
      ledgerHost: String,
      ledgerPort: Int,
      clientConfig: LedgerClientConfiguration,
      nonRepudiationConfig: nonrepudiation.Configuration.Cli,
  )(implicit
      ec: ExecutionContext,
      aesf: ExecutionSequencerFactory,
  ): Future[LedgerClientBase.Error \/ DamlLedgerClient] = {
    val client = for {
      builder <- channelBuilder(
        ledgerHost,
        ledgerPort,
        nonRepudiationConfig,
      )
      client <- DamlLedgerClient.fromBuilder(builder, clientConfig)
    } yield client

    client
      .map(_.right)
      .recover { case NonFatal(e) =>
        \/.left(
          LedgerClientBase.Error(s"Cannot connect to the ledger server, error: ${e.description}")
        )
      }
  }

}
