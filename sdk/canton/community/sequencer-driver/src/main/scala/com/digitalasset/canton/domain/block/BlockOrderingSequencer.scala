// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block

import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.time.TimeProvider
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.google.protobuf.ByteString
import io.grpc.ServerServiceDefinition
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.{KillSwitch, Materializer}
import pureconfig.{ConfigReader, ConfigWriter}

import scala.concurrent.{ExecutionContext, Future}

/** Adapts a [[com.digitalasset.canton.domain.block.BlockOrderer]] implementation to the sequencer driver API.
  */
object BlockOrderingSequencer {

  val DefaultFirstBlockHeight: Long = 0

  class Factory[F <: BlockOrdererFactory](
      val blockOrdererFactory: F, // Must be a public field as we leak its type member
      driverName: String,
      useTimeProvider: Boolean,
  ) extends SequencerDriverFactory {

    override type ConfigType = blockOrdererFactory.ConfigType

    override def name: String = driverName

    override def version: Int = blockOrdererFactory.version

    override def configParser: ConfigReader[blockOrdererFactory.ConfigType] =
      blockOrdererFactory.configParser

    override def configWriter(confidential: Boolean): ConfigWriter[blockOrdererFactory.ConfigType] =
      blockOrdererFactory.configWriter(confidential)

    override def create(
        config: blockOrdererFactory.ConfigType,
        nonStandardConfig: Boolean,
        timeProvider: TimeProvider,
        firstBlockHeight: Option[Long],
        loggerFactory: NamedLoggerFactory,
    )(implicit executionContext: ExecutionContext, materializer: Materializer): SequencerDriver =
      new Driver(
        blockOrdererFactory.create(config, timeProvider, loggerFactory),
        firstBlockHeight.getOrElse(DefaultFirstBlockHeight),
        loggerFactory,
      )

    override def usesTimeProvider: Boolean = useTimeProvider
  }

  private class Driver(
      blockOrderer: BlockOrderer,
      firstBlockHeight: Long,
      loggerFactory: NamedLoggerFactory,
  ) extends SequencerDriver {

    private val logger = loggerFactory.getTracedLogger(getClass)

    override def adminServices: Seq[ServerServiceDefinition] =
      // This is a bit of a semantic abuse, as not all exposed services will be administrative (e.g., P2P);
      // perhaps we can rename the field in the sequencer API.
      blockOrderer.grpcServices

    override def registerMember(member: String)(implicit
        traceContext: TraceContext
    ): Future[Unit] =
      blockOrderer.sendRequest(RegisterMemberTag, ByteString.copyFromUtf8(member))

    override def acknowledge(acknowledgement: ByteString)(implicit
        traceContext: TraceContext
    ): Future[Unit] =
      blockOrderer.sendRequest(AcknowledgeTag, acknowledgement)

    override def send(request: ByteString)(implicit traceContext: TraceContext): Future[Unit] =
      blockOrderer.sendRequest(SendTag, request)

    override def subscribe()(implicit
        traceContext: TraceContext
    ): Source[RawLedgerBlock, KillSwitch] =
      blockOrderer
        .subscribe(firstBlockHeight)
        .map(blockOrdererBlockToRawLedgerBlock(logger))

    override def health(implicit
        traceContext: TraceContext
    ): Future[SequencerDriverHealthStatus] =
      blockOrderer.health

    override def close(): Unit = blockOrderer.close()
  }

  def blockOrdererBlockToRawLedgerBlock(
      logger: TracedLogger
  )(block: BlockOrderer.Block): RawLedgerBlock =
    block match {
      case BlockOrderer.Block(blockHeight, requests) =>
        RawLedgerBlock(
          blockHeight,
          requests.map {
            case event @ Traced(BlockOrderer.OrderedRequest(orderingTime, tag, body)) =>
              implicit val traceContext: TraceContext =
                event.traceContext // Preserve the request trace ID in the log
              tag match {
                case RegisterMemberTag =>
                  Traced(RawLedgerBlock.RawBlockEvent.AddMember(body.toStringUtf8))
                case AcknowledgeTag =>
                  Traced(RawLedgerBlock.RawBlockEvent.Acknowledgment(body))
                case SendTag =>
                  Traced(RawLedgerBlock.RawBlockEvent.Send(body, orderingTime))
                case _ =>
                  logger.error(s"Unexpected tag $tag")
                  sys.exit(1)
              }
          },
        )
    }

  private[domain] val AcknowledgeTag = "acknowledge"
  private[domain] val RegisterMemberTag = "registerMember"
  private[domain] val SendTag = "send"
  private[domain] val BatchTag = "batch"
}
