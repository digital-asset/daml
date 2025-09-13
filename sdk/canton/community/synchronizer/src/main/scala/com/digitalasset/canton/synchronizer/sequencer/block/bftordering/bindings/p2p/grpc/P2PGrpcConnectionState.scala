// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.P2PConnectionState
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.P2PConnectionState.P2PEndpointIdAssociationResult
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  P2PAddress,
  P2PNetworkRef,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.mutex
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.stub.StreamObserver

import scala.collection.mutable

/** Unified central bookkeeping of all P2P gRPC connections in the BFT orderer.
  */
final class P2PGrpcConnectionState(
    private val thisNode: BftNodeId, // Useful when debugging
    override val loggerFactory: NamedLoggerFactory,
) extends P2PConnectionState
    with NamedLogging
    with PrettyPrinting {

  import P2PGrpcConnectionState.*

  // Node IDs and senders are in a 1:1 relationship, so we use a bidirectional mapping.
  private val bftNodeIdToPeerSender =
    mutable.Map[BftNodeId, StreamObserver[BftOrderingMessage]]()
  private val peerSenderToBftNodeId =
    mutable.Map[StreamObserver[BftOrderingMessage], BftNodeId]()

  // Multiple endpoints may be associated to the same BFT node ID.
  private val p2pEndpointIdToBftNodeId = mutable.Map[P2PEndpoint.Id, BftNodeId]()

  // Node IDs and network refs are kept in a 1:1 relationship, but we never look up by network ref,
  //  so a single map is sufficient.
  private val bftNodeIdToNetworkRef =
    mutable.Map[BftNodeId, P2PNetworkRefEntry]()

  // For outgoing connections, a network ref is initially associated to an endpoint ID
  //  but is later be associated to a BFT node ID when authentication completes, or
  //  if an outgoing connection turns out to be a duplicate of an existing one
  //  for the same BFT node ID.
  private val p2pEndpointIdToNetworkRef =
    mutable.Map[P2PEndpoint.Id, P2PNetworkRefEntry]()

  override protected def pretty: Pretty[P2PGrpcConnectionState] = prettyOfClass(
    param("thisNode", _.thisNode.doubleQuoted),
    param(
      "bftNodeIdToPeerSender",
      _.bftNodeIdToPeerSender.map { case (bftNodeId, sender) =>
        bftNodeId.doubleQuoted -> System.identityHashCode(sender)
      },
    ),
    param(
      "peerSenderToBftNodeId",
      _.peerSenderToBftNodeId.map { case (sender, bftNodeId) =>
        System.identityHashCode(sender) -> bftNodeId.doubleQuoted
      },
    ),
    param(
      "p2pEndpointIdToBftNodeId",
      _.p2pEndpointIdToBftNodeId.map { case (p2pEndpointId, bftNodeId) =>
        p2pEndpointId -> bftNodeId.doubleQuoted
      },
    ),
    param(
      "bftNodeIdToNetworkRef",
      _.bftNodeIdToNetworkRef.map { case (bftNodeId, networkRef) =>
        bftNodeId.doubleQuoted -> networkRef.toString.unquoted
      },
    ),
    param(
      "p2pEndpointIdToNetworkRef",
      _.p2pEndpointIdToNetworkRef.map { case (p2pEndpointId, networkRef) =>
        p2pEndpointId -> networkRef.toString.unquoted
      },
    ),
  )

  override def connections(implicit
      traceContext: TraceContext
  ): Seq[(Option[P2PEndpoint.Id], Option[BftNodeId])] =
    mutex(this) {
      logger.debug(s"P2P connection state (`knownConnections`): $this")
      val endpoints = (
        // All active endpoints
        p2pEndpointIdToNetworkRef.map { case (p2pEndpointId, _) =>
          Some(p2pEndpointId) -> None
        } ++
          // All authenticated endpoints
          p2pEndpointIdToBftNodeId
            .map { case (p2pEndpointId, bftNodeId) =>
              Some(p2pEndpointId) -> Some(bftNodeId)
            }
      ).toMap.toSeq // Convert to a map to deduplicate endpoint IDs, keeping the authenticated ones in case they appear in both
      val result =
        endpoints ++
          // All the BFT node IDs with a network ref (authenticated) but without an endpoint
          bftNodeIdToNetworkRef.keys
            .filterNot(p2pEndpointIdToBftNodeId.values.toSeq.contains)
            .map(bftNodeId => None -> Some(bftNodeId))
      logger.debug(s"Known connections: $result")
      result
    }

  override def isDefined(p2pEndpointId: P2PEndpoint.Id)(implicit
      traceContext: TraceContext
  ): Boolean =
    mutex(this)(connections.flatMap(_._1).contains(p2pEndpointId))

  override def isOutgoing(p2pEndpointId: P2PEndpoint.Id): Boolean =
    mutex(this)(p2pEndpointIdToNetworkRef.get(p2pEndpointId).exists(_.isOutgoingConnection))

  override def authenticatedCount: NonNegativeInt =
    NonNegativeInt.tryCreate(mutex(this)(bftNodeIdToNetworkRef.size))

  override def getBftNodeId(p2pEndpointId: P2PEndpoint.Id): Option[BftNodeId] =
    mutex(this)(p2pEndpointIdToBftNodeId.get(p2pEndpointId))

  override def getNetworkRef(bftNodeId: BftNodeId): Option[P2PNetworkRef[BftOrderingMessage]] =
    bftNodeIdToNetworkRef.get(bftNodeId).map(_.networkRef)

  def getSender(p2pAddressId: P2PAddress.Id): Option[StreamObserver[BftOrderingMessage]] =
    mutex(this) {
      p2pAddressId match {
        case Right(bftNodeId) =>
          bftNodeIdToPeerSender.get(bftNodeId)
        case Left(p2pEndpointId) =>
          p2pEndpointIdToBftNodeId.get(p2pEndpointId).flatMap(bftNodeIdToPeerSender.get)
      }
    }

  //
  // All update operations ensure that:
  //
  // - A P2P endpoint ID is associated with a BFT node ID as soon as the association is known.
  // - Associating a P2P endpoint ID to this BFT node ID returns an error.
  // - Re-associating a P2P endpoint ID to a different BFT node ID returns an error.
  // - All P2P endpoint IDs for a BFT node ID point to the same network reference to which the BFT node ID also points,
  //   replacing and closing duplicates as they are identified.
  // - No new sender is associated with a BFT node ID if one is already associated with it.
  // - No new network ref is associated with a P2P endpoint ID if one is already associated with it.
  // - No new network ref is associated with a BFT node ID if one is already associated with it.
  //

  override def associateP2PEndpointIdToBftNodeId(
      p2pAddress: P2PAddress
  )(implicit traceContext: TraceContext): P2PEndpointIdAssociationResult = {
    val maybeP2PEndpoint = p2pAddress.maybeP2PEndpoint
    val maybeBftNodeId = p2pAddress.maybeBftNodeId
    maybeP2PEndpoint
      .flatMap(e => maybeBftNodeId.map(_ -> e))
      .fold(Right(()): Either[P2PConnectionState.Error, Unit]) { case (bftNodeId, p2pEndpoint) =>
        associateP2PEndpointIdToBftNodeId(p2pEndpoint.id, bftNodeId)
      }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def associateP2PEndpointIdToBftNodeId(
      p2pEndpointId: P2PEndpoint.Id,
      bftNodeId: BftNodeId,
  )(implicit traceContext: TraceContext): P2PEndpointIdAssociationResult =
    mutex(this) {
      var result: P2PEndpointIdAssociationResult = Right(())
      var changesMade = false
      p2pEndpointIdToBftNodeId
        .updateWith(p2pEndpointId) {
          case Some(previousBftNodeId) =>
            if (previousBftNodeId == bftNodeId) {
              logger.debug(s"Endpoint $p2pEndpointId already associated with $bftNodeId, no change")
            } else {
              result = Left(
                P2PConnectionState.Error
                  .P2PEndpointIdAlreadyAssociated(p2pEndpointId, previousBftNodeId, bftNodeId)
              )
              logger.warn(
                "Possible impersonation attempt: " +
                  s"endpoint $p2pEndpointId is already associated with $previousBftNodeId, " +
                  s"not associating it to $bftNodeId; if this is a legitimate change, " +
                  "the previous association must be removed first"
              )
            }
            Some(previousBftNodeId)
          case _ if bftNodeId == thisNode =>
            result = Left(
              P2PConnectionState.Error.CannotAssociateP2PEndpointIdsToSelf(p2pEndpointId, thisNode)
            )
            logger.warn(
              s"Possible impersonation attempt: not associating $p2pEndpointId to this node ($thisNode)"
            )
            None
          case _ =>
            logger.debug(s"Associated $p2pEndpointId -> $bftNodeId, no previous association")
            changesMade = true
            Some(bftNodeId)
        }
        .discard
      if (changesMade)
        consolidateNetworkRefs(bftNodeId)
      result
    }

  // Adds a new sender or completes the endpoint information if already present, returning `false` in that case;
  //  called by the connection manager when a new P2P connection is established
  def addSenderIfMissing(
      bftNodeId: BftNodeId,
      peerSender: StreamObserver[BftOrderingMessage],
  )(implicit traceContext: TraceContext): Boolean =
    mutex(this) {
      logger.debug(s"P2P connection state before `addSenderIfMissing`: $this")
      val result =
        if (!bftNodeIdToPeerSender.contains(bftNodeId)) {
          logger.debug(
            s"Associating $bftNodeId <-> $peerSender "
          )
          biAssociateBftNodeIdWithPeerSender(bftNodeId, peerSender)
          true
        } else {
          logger.debug(
            s"Not Associating $bftNodeId <-> $peerSender because a sender for this node already exists"
          )
          false
        }
      logger.debug(s"P2P connection state after `addSenderIfMissing`: $this")
      result
    }

  // Called by the P2P network output module to ensure connectivity with a peer
  override def addNetworkRefIfMissing(
      p2pAddressId: P2PAddress.Id
  )(
      actionIfPresent: () => Unit
  )(
      createNetworkRef: () => P2PNetworkRef[BftOrderingMessage]
  )(implicit traceContext: TraceContext): Unit =
    mutex(this) {
      logger.debug(s"P2P connection state before `addNetworkRefIfMissing`: $this")
      p2pAddressId match {
        case Left(p2pEndpointId) =>
          // Check if the endpoint ID is indirectly associated with a network ref via BFT node ID
          p2pEndpointIdToBftNodeId
            .get(p2pEndpointId)
            .fold(
              // If no BFT node ID is associated, check if the endpoint ID is directly associated with a network ref
              p2pEndpointIdToNetworkRef
                .get(p2pEndpointId)
                .fold {
                  p2pEndpointIdToNetworkRef
                    .put(
                      p2pEndpointId,
                      P2PNetworkRefEntry(createNetworkRef(), isOutgoingConnection = true),
                    )
                    .discard
                } { _ =>
                  actionIfPresent()
                }
            ) { bftNodeId =>
              // If an endpoint ID is associated with the BFT node ID, recur to the other case
              addNetworkRefIfMissing(Right(bftNodeId))(actionIfPresent)(
                createNetworkRef
              )
            }

        case Right(bftNodeId) =>
          bftNodeIdToNetworkRef
            .get(bftNodeId)
            .fold {
              // Create the network ref and associate it with the BFT node ID and all its endpoint IDs
              val networkRef = createNetworkRef()
              bftNodeIdToNetworkRef
                .put(bftNodeId, P2PNetworkRefEntry(networkRef, isOutgoingConnection = false))
                .discard
            } { _ =>
              // A network ref is already associated to the BFT node ID
              actionIfPresent()
            }
      }
      logger.debug(s"P2P connection state after `addNetworkRefIfMissing`: $this")
    }

  // Used to close a connection in various situations
  def shutdownConnectionAndReturnPeerSender(
      p2pAddressId: P2PAddress.Id,
      clearNetworkRefAssociations: Boolean,
      closeNetworkRef: Boolean,
  )(implicit traceContext: TraceContext): Option[StreamObserver[BftOrderingMessage]] = {
    require(
      clearNetworkRefAssociations || !closeNetworkRef,
      "Cannot close network ref without clearing associations first",
    )
    mutex(this) {
      logger.debug(s"P2P connection state before `shutdownConnectionAndReturnPeerSender`: $this")
      val result = p2pAddressId match {
        case Right(bftNodeId) =>
          // Remove the BFT node ID and its associated sender
          val sender = unassociateAndReturnPeerSender(bftNodeId)
          cleanupNetworkRef(bftNodeId, clearNetworkRefAssociations, closeNetworkRef)
          sender

        case Left(p2pEndpointId) =>
          p2pEndpointIdToBftNodeId
            .get(p2pEndpointId)
            .fold {
              // If no BFT node ID is associated, check if the endpoint ID is directly associated with a network ref
              p2pEndpointIdToNetworkRef
                .get(p2pEndpointId)
                .foreach { case P2PNetworkRefEntry(networkRef, _) =>
                  if (clearNetworkRefAssociations) {
                    p2pEndpointIdToNetworkRef.remove(p2pEndpointId).discard
                    logger.debug(s"Removed $networkRef for $p2pEndpointId")
                  }
                  if (closeNetworkRef) {
                    networkRef.close()
                    logger.debug(s"Closed network ref $networkRef for $p2pEndpointId")
                  }
                }
              Option.empty[StreamObserver[BftOrderingMessage]]
            } { bftNodeId =>
              shutdownConnectionAndReturnPeerSender(
                Right(bftNodeId),
                clearNetworkRefAssociations,
                closeNetworkRef,
              )
            }
      }
      logger.debug(s"P2P connection state after `shutdownConnectionAndReturnPeerSender`: $this")
      result
    }
  }

  def unassociateSenderAndReturnEndpointIds(
      peerSender: StreamObserver[BftOrderingMessage]
  )(implicit traceContext: TraceContext): Seq[P2PEndpoint.Id] =
    mutex(this) {
      logger.debug(s"P2P connection state before `unassociateSenderAndReturnEndpointIds`: $this")
      val result = peerSenderToBftNodeId
        .remove(peerSender)
        .map { bftNodeId =>
          bftNodeIdToPeerSender.remove(bftNodeId).discard
          val endpointIds =
            p2pEndpointIdToBftNodeId.filter(_._2 == bftNodeId).keys.toSeq
          logger.debug(
            s"Removed $bftNodeId <-> $peerSender"
          )
          endpointIds
        }
        .getOrElse {
          logger.debug(
            s"Not removing $peerSender because it does not exist yet (or possibly removed as duplicate)"
          )
          Seq.empty
        }
      logger.debug(s"P2P connection state after `unassociateSenderAndReturnEndpointIds`: $this")
      result
    }

  // Used to simulate a restart
  def clear(): Unit =
    mutex(this) {
      logger.debug(s"P2P connection state before `clear`: $this")(TraceContext.empty)
      bftNodeIdToPeerSender.clear()
      peerSenderToBftNodeId.clear()
      p2pEndpointIdToBftNodeId.clear()
      p2pEndpointIdToNetworkRef.clear()
      bftNodeIdToNetworkRef.clear()
      logger.debug(s"P2P connection state after `clear`: $this")(TraceContext.empty)
    }

  private def biAssociateBftNodeIdWithPeerSender(
      bftNodeId: BftNodeId,
      peerSender: StreamObserver[BftOrderingMessage],
  ): Unit = {
    bftNodeIdToPeerSender.put(bftNodeId, peerSender).discard
    peerSenderToBftNodeId.put(peerSender, bftNodeId).discard
  }

  private def consolidateNetworkRefs(
      bftNodeId: BftNodeId
  )(implicit traceContext: TraceContext): Unit = {
    val p2pEndpointIds =
      p2pEndpointIdToBftNodeId
        .collect {
          case (endpointId, nodeId) if nodeId == bftNodeId => endpointId
        }
    bftNodeIdToNetworkRef
      .get(bftNodeId)
      .orElse(
        p2pEndpointIds
          .flatMap(p2pEndpointIdToNetworkRef.get)
          .headOption
      )
      .foreach { case e @ P2PNetworkRefEntry(existingNetworkRef, isOutgoingConnection) =>
        bftNodeIdToNetworkRef
          .put(bftNodeId, e)
          .foreach { case P2PNetworkRefEntry(previousNetworkRef, _) =>
            closePreviousNetworkRefIfDuplicate(existingNetworkRef, previousNetworkRef, bftNodeId)
          }
        p2pEndpointIds.foreach { p2pEndpoint =>
          p2pEndpointIdToNetworkRef
            .put(p2pEndpoint, P2PNetworkRefEntry(existingNetworkRef, isOutgoingConnection))
            .foreach { case P2PNetworkRefEntry(previousNetworkRef, _) =>
              closePreviousNetworkRefIfDuplicate(
                existingNetworkRef,
                previousNetworkRef,
                p2pEndpoint.toString,
              )
            }
        }
      }
  }

  private def cleanupNetworkRef(
      bftNodeId: BftNodeId,
      clearNetworkRefAssociations: Boolean,
      closeNetworkRef: Boolean,
  )(implicit traceContext: TraceContext): Unit =
    if (clearNetworkRefAssociations) {
      // Remove the BFT node ID from the network ref associations
      bftNodeIdToNetworkRef.remove(bftNodeId).foreach { case P2PNetworkRefEntry(networkRef, _) =>
        p2pEndpointIdToBftNodeId.foreach {
          case (endpointId, nodeId) if nodeId == bftNodeId =>
            p2pEndpointIdToNetworkRef.remove(endpointId).discard
          case _ => ()
        }
        logger.debug(s"Removed $networkRef for $bftNodeId")
        if (closeNetworkRef) {
          networkRef.close()
          logger.debug(s"Closed $networkRef for $bftNodeId")
        }
      }
    }

  private def unassociateAndReturnPeerSender(bftNodeId: BftNodeId)(implicit
      traceContext: TraceContext
  ): Option[StreamObserver[BftOrderingMessage]] =
    // Remove the BFT node ID and its associated sender
    bftNodeIdToPeerSender
      .remove(bftNodeId)
      .map { peerSender =>
        logger.debug(s"Removing $bftNodeId <-> $peerSender")
        peerSenderToBftNodeId.remove(peerSender).discard
        peerSender
      }
      .orElse {
        logger.debug(
          s"Not removing connection state for $bftNodeId " +
            s"because it does not exist yet (or possibly removed as duplicate)"
        )
        None
      }

  private def closePreviousNetworkRefIfDuplicate(
      networkRef: P2PNetworkRef[BftOrderingMessage],
      previousNetworkRef: P2PNetworkRef[BftOrderingMessage],
      connectionId: String,
  )(implicit traceContext: TraceContext): Unit =
    if (previousNetworkRef != networkRef) {
      logger.debug(
        s"Replacing network ref for $connectionId from $previousNetworkRef to $networkRef " +
          "and closing the previous one"
      )
      previousNetworkRef.close()
    } else {
      logger.debug(
        s"Keeping network ref $networkRef for $connectionId, no change"
      )
    }
}

object P2PGrpcConnectionState {

  private final case class P2PNetworkRefEntry(
      networkRef: P2PNetworkRef[BftOrderingMessage],
      isOutgoingConnection: Boolean,
  )
}
