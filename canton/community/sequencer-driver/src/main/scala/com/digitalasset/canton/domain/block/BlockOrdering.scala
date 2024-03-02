// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.time.TimeProvider
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.google.protobuf.ByteString
import io.grpc.BindableService
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.{KillSwitch, Materializer}
import pureconfig.{ConfigReader, ConfigWriter}

import scala.concurrent.{ExecutionContext, Future}

/** Only the differences w.r.t. [[SequencerDriverFactory]] are documented.
  */
trait BlockOrdererFactory {

  type ConfigType

  def version: Int

  def configParser: ConfigReader[ConfigType]

  def configWriter(confidential: Boolean): ConfigWriter[ConfigType]

  def create(
      config: ConfigType,
      domainTopologyManagerId: String,
      timeProvider: TimeProvider,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      materializer: Materializer,
  ): BlockOrderer
}

/** Only the differences w.r.t. [[SequencerDriver]] are documented.
  */
trait BlockOrderer extends AutoCloseable {

  /** Additional services exposed by the [[com.digitalasset.canton.domain.block.BlockOrderer]], e.g., to other peer nodes.
    */
  def grpcServices: Seq[BindableService]

  /** Send a request.
    * Requests are ordered and delivered as [[com.digitalasset.canton.domain.block.BlockOrderer.Block]] to subscribers.
    */
  def sendRequest(tag: String, body: ByteString, signature: Option[TransactionSignature] = None)(
      implicit traceContext: TraceContext
  ): Future[Unit]

  // Read operations

  /** Delivers a stream of blocks starting with `fromHeight`.
    */
  def subscribe(
      fromHeight: Long
  )(implicit traceContext: TraceContext): Source[BlockOrderer.Block, KillSwitch]

  def health(implicit traceContext: TraceContext): Future[BlockOrderer.HealthStatus]
}

object BlockOrderer {

  private type HealthStatus = SequencerDriverHealthStatus // Seems to be enough for now.

  final case class Block(
      blockHeight: Long,
      requests: Seq[Traced[OrderedRequest]],
  )

  final case class OrderedRequest(
      microsecondsSinceEpoch: Long,
      tag: String,
      body: ByteString,
  )
}
