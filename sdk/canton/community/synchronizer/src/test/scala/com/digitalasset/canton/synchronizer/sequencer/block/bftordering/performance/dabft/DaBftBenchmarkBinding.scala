// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.dabft

import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.BftBenchmark.shutdownExecutorService
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.BftBinding.TxConsumer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.{
  BftBenchmarkConfig,
  BftBinding,
  BftBindingFactory,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.standalone.v1.StandaloneBftOrderingServiceGrpc.StandaloneBftOrderingServiceStub
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.standalone.v1.{
  ReadOrderedRequest,
  ReadOrderedResponse,
  SendRequest,
  StandaloneBftOrderingServiceGrpc,
}
import com.digitalasset.canton.util.Mutex
import com.google.protobuf.ByteString
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.stub.StreamObserver
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import java.util.concurrent.*
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.language.existentials
import scala.util.chaining.*
import scala.util.{Failure, Success}

object DaBftBindingFactory extends BftBindingFactory {
  override type T = DaBftBinding

  override def create(config: BftBenchmarkConfig): DaBftBinding = {
    // We could generate enough random payloads in advance, but we may OOM, e.g. with 500k TPS:
    //
    //  1m run + 1m margin = 120s, Max total writes = 500k TPS * 120s = 60_000_000 < 2_147_483_647
    //  20KB * 60mil ~= 20GB * 60
    //
    // Alternatively, we could generate batches as we go but, since DABFT doesn't
    // compress, we just use the same payload every time.
    val payloadOutput = ByteString.newOutput(config.transactionBytes)
    for (_ <- 0 until config.transactionBytes)
      payloadOutput.write('0')
    new DaBftBinding(payloadOutput.toByteString)
  }
}

final class DaBftBinding(payload: ByteString) extends BftBinding {

  private val MaxReadGrpcBytes = 32 * 1024 * 1024

  private val log = ContextualizedLogger.get(getClass)
  implicit private val loggingContext: LoggingContext = LoggingContext.empty

  private val writeChannels =
    new ConcurrentHashMap[
      BftBenchmarkConfig.Node,
      (ManagedChannel, StandaloneBftOrderingServiceStub, Mutex),
    ]

  private val readChannels =
    new ConcurrentHashMap[BftBenchmarkConfig.ReadNode[
      ?
    ], (ManagedChannel, StreamObserver[ReadOrderedResponse])]

  private val scalaFutureExecutor = Executors.newWorkStealingPool()
  private val scalaFutureExecutionContext =
    ExecutionContext.fromExecutor(scalaFutureExecutor)

  private val readExecutor = Executors.newCachedThreadPool()
  private val writeExecutor = Executors.newCachedThreadPool()

  override def write(
      node: BftBenchmarkConfig.WriteNode[?],
      txId: String,
  ): CompletionStage[Unit] = {
    val result = new CompletableFuture[Unit]()
    val (stub, lock) = stubAndLockFor(node)

    writeExecutor.submit(new Runnable {
      override def run(): Unit = {

        val request = SendRequest(txId, payload)

        lock
          .exclusive {
            // The writer is not thread-safe
            stub.send(request)
          }
          .transform {
            case Success(response) =>
              response.rejectionReason.fold {
                log.debug(s"Wrote txId $txId to node $node")
                result.complete(()).discard
              } { reason =>
                val exception =
                  new RuntimeException(s"Request $txId was rejected by node $node: $reason")
                log.error(s"Rejected writing to node $node", exception)
                result.completeExceptionally(exception).discard
              }
              Success(())
            case Failure(exception) =>
              log.error(s"Error writing to node $node", exception)
              result.completeExceptionally(exception).discard
              Success(())
          }(scalaFutureExecutionContext)
          .discard
      }
    })

    result
  }

  override def subscribeOnce(node: BftBenchmarkConfig.ReadNode[?], txConsumer: TxConsumer): Unit = {
    readChannels
      .computeIfAbsent(
        node,
        _ => {
          val channelBuilder =
            node match {
              case BftBenchmarkConfig.InProcessReadWriteNode(_, _, readPort) =>
                InProcessChannelBuilder.forName(readPort)
              case BftBenchmarkConfig.NetworkedReadWriteNode(host, _, readPort) =>
                ManagedChannelBuilder.forTarget(s"$host:$readPort")
            }
          channelBuilder.usePlaintext().discard
          channelBuilder.maxInboundMessageSize(MaxReadGrpcBytes).discard
          val channel = channelBuilder.build()
          val stub = StandaloneBftOrderingServiceGrpc
            .stub(channel)
            .withMaxInboundMessageSize(MaxReadGrpcBytes)
            .withMaxOutboundMessageSize(MaxReadGrpcBytes)

          val reader: StreamObserver[ReadOrderedResponse] =
            new StreamObserver[ReadOrderedResponse] {
              override def onNext(response: ReadOrderedResponse): Unit = {
                log.debug(s"Read batch of ${response.block.size} requests")
                response.block.foreach { o =>
                  readExecutor
                    .submit(new Runnable {
                      override def run(): Unit = {
                        val result = new CompletableFuture[String]()
                        try {
                          val txId = o.tag
                          result.complete(txId).discard
                          log.debug(s"Read back UUID $txId")
                        } catch {
                          case t: Throwable =>
                            log.error("Error while parsing request", t)
                        }
                        txConsumer(result)
                      }
                    })
                    .discard
                }
              }

              override def onError(t: Throwable): Unit = {
                log.error("Error from server", t)
                complete()
              }

              override def onCompleted(): Unit =
                complete()

              def complete(): Unit = {
                log.info(s"Write stream for node $node being completed")
                closeGrpcReadChannel(node)
                readChannels.remove(node).discard
              }
            }
          stub.readOrdered(ReadOrderedRequest(0), reader)
          channel -> reader
        },
      )
    ()
  }

  private def stubAndLockFor(
      writeNode: BftBenchmarkConfig.WriteNode[?]
  ): (StandaloneBftOrderingServiceStub, Mutex) =
    writeChannels
      .computeIfAbsent(
        writeNode,
        _ => {
          val channelBuilder =
            writeNode match {
              case BftBenchmarkConfig.InProcessWriteOnlyNode(_, writePort) =>
                InProcessChannelBuilder.forName(writePort)
              case BftBenchmarkConfig.InProcessReadWriteNode(_, writePort, _) =>
                InProcessChannelBuilder.forName(writePort)
              case BftBenchmarkConfig.NetworkedWriteOnlyNode(host, writePort) =>
                ManagedChannelBuilder.forTarget(s"$host:$writePort")
              case BftBenchmarkConfig.NetworkedReadWriteNode(host, writePort, _) =>
                ManagedChannelBuilder.forTarget(s"$host:$writePort")
            }
          channelBuilder.usePlaintext().discard
          val channel = channelBuilder.build()
          val stub =
            StandaloneBftOrderingServiceGrpc
              .stub(channel)
              .withMaxInboundMessageSize(MaxReadGrpcBytes)
              .withMaxOutboundMessageSize(MaxReadGrpcBytes)
          (channel, stub, Mutex())
        },
      )
      .pipe { case (_, stub, lock) => (stub, lock) }

  override def close(): Unit = {
    writeChannels.values().asScala.map(_._1).foreach(closeGrpcChannel)
    shutdownExecutorService(writeExecutor)

    closeGrpcStreamsAndChannels(readChannels)
    shutdownExecutorService(readExecutor)

    shutdownExecutorService(scalaFutureExecutor)
  }

  private def closeGrpcStreamsAndChannels[N <: BftBenchmarkConfig.Node, R](
      channels: ConcurrentHashMap[N, (ManagedChannel, StreamObserver[R])]
  ): Unit =
    channels.values().forEach { case (channel, observer) =>
      closeGrpcStreamAndChannel(channel, observer)
    }

  private def closeGrpcStreamAndChannel[R, N <: BftBenchmarkConfig.Node](
      channel: ManagedChannel,
      stream: StreamObserver[R],
  ): Unit = {
    stream.onCompleted()
    closeGrpcChannel(channel)
  }

  private def closeGrpcReadChannel[N <: BftBenchmarkConfig.Node](
      node: BftBenchmarkConfig.Node
  ): Unit =
    Option(readChannels.get(node)).map(_._1).foreach(closeGrpcChannel)

  private def closeGrpcChannel[N <: BftBenchmarkConfig.Node](channel: ManagedChannel): Unit =
    channel.shutdown().awaitTermination(20, TimeUnit.SECONDS).discard
}
