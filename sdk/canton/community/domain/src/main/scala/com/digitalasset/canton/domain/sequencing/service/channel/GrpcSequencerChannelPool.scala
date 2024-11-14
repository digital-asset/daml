// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service.channel

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.{SequencerChannelId, SequencerChannelMetadata}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, blocking}
import scala.util.control.NonFatal

/** GrpcSequencerChannelPool tracks active sequencer channels.
  *
  * The dynamically changing channels are kept up to date in response to
  * 1. sequencer-server initiated reasons such as sequencer shutdown or when a member's topology permissions are revoked and
  * 2. reasons arising in the sequencer channel such as when the sequencer channel conversation completes.
  *
  * As channels are initialized in a delayed fashion, the pool tracks initialized and uninitialized channels separately.
  */
private[channel] final class GrpcSequencerChannelPool(
    clock: Clock,
    protocolVersion: ProtocolVersion, // technically the channel protocol is separate from the canton protocol, but use for now at least
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends FlagCloseable
    with NamedLogging {

  // As sequencer channels are mutable, any access or modifications to channels are expected to be synchronized.
  // Initialized channels merely mean that the pool has identified the members that are intending to connect to the channel,
  // but not necessarily that both members have already connected. To be more precise, at least one member has connected
  // to an initialized channel.
  private val initializedChannels = TrieMap[SequencerChannelId, GrpcSequencerChannel]()
  private val uninitializedChannels = mutable.Buffer[UninitializedGrpcSequencerChannel]()

  /** @param responseObserver    GRPC response observer for messages to the client
    * @param authTokenExpiresAtO The member's sequencer token expiration time
    * @param authenticationCheck Callback to use upon channel initialization
    * @return GRPC request observer needed by GrpcSequencerChannelService
    */
  def createUninitializedChannel(
      responseObserver: ServerCallStreamObserver[v30.ConnectToSequencerChannelResponse],
      authTokenExpiresAtO: Option[CantonTimestamp],
      authenticationCheck: Member => Either[String, Unit],
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      traceContext: TraceContext
  ): Either[String, StreamObserver[v30.ConnectToSequencerChannelRequest]] =
    performUnlessClosing(functionFullName) {
      blocking {
        synchronized {
          val channel = new UninitializedGrpcSequencerChannel(
            responseObserver,
            authTokenExpiresAtO,
            authenticationCheck,
            onUninitializedChannelCompleted,
            protocolVersion,
            timeouts,
            loggerFactory,
          ) {

            /** Transitions an uninitialized channel to a newly initialized channel or an existing channel already created by
              * the "opposite" side/member.
              *
              * Also schedules closing of the channel upon member token expiration.
              *
              * @param metadata             Sequencer channel metadata that uniquely identifies the channel and includes the members.
              * @param uninitializedChannel The uninitialized channel to initialize.
              * @param tokenExpiresAt       Token expiration time
              * @param createChannel        Helper to build the initialized channel from the metadata
              * @return the newly created "first" or "second" member message handler (determined by the order in which the members connect)
              */
            override protected def ensureChannelAndConnect(
                metadata: SequencerChannelMetadata,
                uninitializedChannel: UninitializedGrpcSequencerChannel,
                tokenExpiresAt: Option[CantonTimestamp],
                createChannel: SequencerChannelMetadata => GrpcSequencerChannel,
                tc: TraceContext,
            ): Either[String, GrpcSequencerChannelMemberMessageHandler] = {
              implicit val traceContext: TraceContext = tc
              performUnlessClosing(functionFullName) {
                blocking {
                  GrpcSequencerChannelPool.this.synchronized {
                    // Remove uninitialized channel even if adding initialized channel fails
                    // as in the case of an initialization error below the caller closes the GRPC request.
                    uninitializedChannels -= uninitializedChannel
                    for {
                      channelInfo <- initializedChannels.get(metadata.channelId) match {
                        case None =>
                          logger.debug(
                            s"Creating new channel in response to connect request by first member ${metadata.initiatingMember}"
                          )
                          val newChannel = createChannel(metadata)
                          initializedChannels.putIfAbsent(metadata.channelId, newChannel).discard
                          newChannel.onClosed(() => removeChannel(metadata.channelId))
                          Right((newChannel, newChannel.firstMemberHandler))
                        case Some(existingChannel) =>
                          for {
                            _ <- Either.cond(
                              !existingChannel.isFullyConnected,
                              (),
                              s"Sequencer channel ${metadata.channelId} already exists: $metadata",
                            )
                            _ = logger.debug(
                              s"Attaching to existing channel in response to connect request by second member ${metadata.initiatingMember}"
                            )
                            // If channel already exists, we expect the opposite member positions to reflect that
                            // channel.firstMemberToConnect has already connected and channel.secondMemberToConnect
                            // is now connecting and that each request to the channel expects the other member.
                            _ <- Either.cond(
                              existingChannel.secondMember == metadata.initiatingMember,
                              (),
                              s"Sequencer channel ${metadata.channelId} initiating member mismatch: Expected ${existingChannel.secondMember}, but request contains ${metadata.initiatingMember}.",
                            )
                            _ <- Either.cond(
                              existingChannel.firstMember == metadata.receivingMember,
                              (),
                              s"Sequencer channel ${metadata.channelId} connect-to member mismatch: Expected ${existingChannel.firstMember}, but request contains ${metadata.receivingMember}.",
                            )
                            handler <- existingChannel.addSecondMemberToConnectHandler(
                              uninitializedChannel.responseObserver
                            )
                          } yield (existingChannel, handler)
                      }
                      (channel, handler) = channelInfo
                      // schedule expiring the channel once token expires
                      // this could potentially happen immediately if the expiration has already passed
                      _ = tokenExpiresAt.foreach(ts =>
                        clock.scheduleAt(
                          _ => {
                            logger.info(
                              s"Closing channel ${metadata.channelId} because ${channel.firstMember} token has expired at $ts"
                            )
                            closeChannel(metadata.channelId, channel)
                          },
                          ts,
                        )
                      )
                    } yield handler
                  }
                }
              } onShutdown Left("Sequencer channel pool closed")
            }
          }
          uninitializedChannels += channel
          Right(channel.requestObserverAdapter)
        }
      }
    } onShutdown Left("Sequencer channel pool closed")

  private def removeChannel(channelId: SequencerChannelId)(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug(s"Removing channel $channelId")
    blocking {
      synchronized {
        initializedChannels.remove(channelId).discard
      }
    }
  }

  private def closeChannel(
      channelId: SequencerChannelId,
      channel: GrpcSequencerChannel,
      waitForClosed: Boolean = false,
  )(implicit traceContext: TraceContext): Unit =
    try {
      logger.debug(s"Closing channel $channelId")
      channel.close()
      if (waitForClosed) {
        timeouts.network.await(s"closing channel $channelId")(channel.closedF)
      }
    } catch {
      // We don't want to throw if closing fails because it will stop the chain of closing subsequent channels
      case NonFatal(e) => logger.warn(s"Failed to close channel $channelId", e)
    } finally {
      removeChannel(channelId)
    }

  def closeChannels(
      memberO: Option[Member] = None, // None means close all channels i.e. of all members
      waitForClosed: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): Unit =
    blocking {
      synchronized {
        initializedChannels.foreach {
          case (channelId, channel) if memberO.forall(_ == channel.firstMember) =>
            closeChannel(channelId, channel, waitForClosed)
          case _ => ()
        }

        // If we are closing all channels, also close uninitialized channels.
        if (memberO.isEmpty) {
          uninitializedChannels.foreach(_.close())
          uninitializedChannels.clear()
        }
      }
    }

  private def onUninitializedChannelCompleted(channel: UninitializedGrpcSequencerChannel): Unit =
    blocking {
      synchronized {
        (uninitializedChannels -= channel).discard
      }
    }

  override def onClosed(): Unit = blocking {
    synchronized {
      withNewTraceContext { implicit traceContext =>
        logger.debug("Closing all channels in pool")
        // Wait for the channels to actually close in case they are already in the process of closing
        // in which case FlagClosable doesn't wait.
        closeChannels(waitForClosed = true)
      }
    }
  }
}
