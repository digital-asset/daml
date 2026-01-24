// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.{
  AnnotatedResult,
  abort,
  objId,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.NamedLoggingUtils
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.AtomicUtil
import io.grpc.stub.StreamObserver

import java.util.concurrent.atomic.AtomicReference

/** Unified central bookkeeping of all P2P gRPC connections in the BFT orderer.
  */
final class P2PGrpcConnectionState(
    private val thisNode: BftNodeId, // Useful when debugging
    override val loggerFactory: NamedLoggerFactory,
) extends P2PConnectionState
    with NamedLogging
    with NamedLoggingUtils
    with PrettyPrinting {

  import P2PGrpcConnectionState.*

  private val stateRef = new AtomicReference(State())

  override protected def pretty: Pretty[P2PGrpcConnectionState] =
    prettyOfClass(
      param("thisNode", _.thisNode.doubleQuoted),
      param("state", _.stateRef.get()),
    )

  override def connections(implicit
      traceContext: TraceContext
  ): Seq[(Option[P2PEndpoint.Id], Option[BftNodeId])] = {
    logger.debug(s"P2P connection state (`knownConnections`): $this")
    val state = stateRef.get()
    import state.*
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
    connections.flatMap(_._1).contains(p2pEndpointId)

  override def isOutgoing(p2pEndpointId: P2PEndpoint.Id): Boolean =
    stateRef.get().p2pEndpointIdToNetworkRef.get(p2pEndpointId).exists(_.isOutgoingConnection)

  override def authenticatedCount: NonNegativeInt =
    NonNegativeInt.tryCreate(stateRef.get().bftNodeIdToNetworkRef.size)

  override def getBftNodeId(p2pEndpointId: P2PEndpoint.Id): Option[BftNodeId] =
    stateRef.get().p2pEndpointIdToBftNodeId.get(p2pEndpointId)

  override def getNetworkRef(bftNodeId: BftNodeId): Option[P2PNetworkRef[BftOrderingMessage]] =
    stateRef.get().bftNodeIdToNetworkRef.get(bftNodeId).map(_.networkRef)

  def getSender(p2pAddressId: P2PAddress.Id): Option[StreamObserver[BftOrderingMessage]] = {
    val state = stateRef.get()
    import state.*
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
  )(implicit traceContext: TraceContext): P2PEndpointIdAssociationResult = {
    val (prevState, newState, result, refsToClose) =
      AtomicUtil
        .updateAndGetComputed(stateRef) { state =>
          var result: P2PEndpointIdAssociationResult = Right(())
          var changesMade = false
          var refsToClose: Iterable[(P2PEndpoint.Id, P2PNetworkRef[BftOrderingMessage])] = Seq.empty
          var annotation = ""
          var logger = debug _
          var updatedState =
            state.copy(p2pEndpointIdToBftNodeId =
              state.p2pEndpointIdToBftNodeId
                .updatedWith(p2pEndpointId) {
                  case Some(previousBftNodeId) =>
                    if (previousBftNodeId == bftNodeId) {
                      annotation =
                        s"Endpoint $p2pEndpointId already associated with $bftNodeId, no change"
                    } else {
                      result = Left(
                        P2PConnectionState.Error
                          .P2PEndpointIdAlreadyAssociated(
                            p2pEndpointId,
                            previousBftNodeId,
                            bftNodeId,
                          )
                      )
                      annotation = "Possible impersonation attempt: " +
                        s"endpoint $p2pEndpointId is already associated with $previousBftNodeId, " +
                        s"not associating it to $bftNodeId; if this is a legitimate change, " +
                        "the previous association must be removed first"
                      logger = warn _
                    }
                    Some(previousBftNodeId)
                  case _ if bftNodeId == thisNode =>
                    result = Left(
                      P2PConnectionState.Error
                        .CannotAssociateP2PEndpointIdsToSelf(p2pEndpointId, thisNode)
                    )
                    annotation =
                      s"Possible impersonation attempt: not associating $p2pEndpointId to this node ($thisNode)"
                    logger = warn _
                    None
                  case _ =>
                    annotation = s"Associated $p2pEndpointId -> $bftNodeId, no previous association"
                    changesMade = true
                    Some(bftNodeId)
                }
            )
          if (changesMade) {
            val consolidateResult = consolidateNetworkRefs(updatedState, bftNodeId)
            updatedState = consolidateResult._1
            refsToClose = consolidateResult._2
          }
          updatedState -> AnnotatedResult(
            (state, updatedState, result, refsToClose),
            () => annotation,
            logger,
          )
        }
        .logAndExtract(prefix =
          s"Associating P2P endpoint $p2pEndpointId to BFT node ID $bftNodeId: "
        )

    refsToClose.foreach { case (endpointId, networkRef) =>
      logger.debug(
        s"Closing duplicate network ref ${objId(networkRef)} for endpoint $endpointId after re-association"
      )
      networkRef.close()
    }
    logger.debug(s"P2P connection state before `associateP2PEndpointIdToBftNodeId`: $prevState")
    logger.debug(s"P2P connection state after `associateP2PEndpointIdToBftNodeId`: $newState")
    result
  }

  /** Adds a new sender or completes the endpoint information if already present, returning `false`
    * in that case; called by the connection manager when a new P2P connection is established.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def addSenderIfMissing(
      bftNodeId: BftNodeId,
      peerSender: StreamObserver[BftOrderingMessage],
  )(implicit traceContext: TraceContext): Boolean = {
    val (prevState, newState, result) =
      AtomicUtil
        .updateAndGetComputed(stateRef) { state =>
          var updatedState = state
          var annotation = ""
          val logger = debug _
          val result =
            if (!state.bftNodeIdToPeerSender.contains(bftNodeId)) {
              annotation = s"Associating peer sender $bftNodeId <-> ${objId(peerSender)}"
              updatedState = biAssociateBftNodeIdWithPeerSender(state, bftNodeId, peerSender)
              true
            } else {
              annotation =
                s"Not associating peer sender $bftNodeId <-> ${objId(peerSender)} because one for this node already exists"
              false
            }
          updatedState -> AnnotatedResult((state, updatedState, result), () => annotation, logger)
        }
        .logAndExtract(prefix =
          s"Adding peer sender ${objId(peerSender)} for BFT node ID $bftNodeId: "
        )
    logger.debug(s"P2P connection state before `addSenderIfMissing`: $prevState")
    logger.debug(s"P2P connection state after `addSenderIfMissing`: $newState")
    result
  }

  override def addNetworkRefIfMissing(
      p2pAddressId: P2PAddress.Id
  )(
      actionIfPresent: () => Unit
  )(
      createNetworkRef: () => P2PNetworkRef[BftOrderingMessage]
  )(implicit traceContext: TraceContext): Unit = {
    def internalAddNetworkRefIfMissing(
        state: State,
        p2pAddressId: P2PAddress.Id,
    ): (State, State, Boolean) =
      p2pAddressId match {
        case Left(p2pEndpointId) =>
          // Check if the endpoint ID is indirectly associated with a network ref via BFT node ID
          state.p2pEndpointIdToBftNodeId
            .get(p2pEndpointId)
            .fold(
              // If no BFT node ID is associated, check if the endpoint ID is directly associated with a network ref
              state.p2pEndpointIdToNetworkRef
                .get(p2pEndpointId)
                .fold {
                  // Associate the network ref with the BFT node ID and all its endpoint IDs
                  (
                    state,
                    state.copy(p2pEndpointIdToNetworkRef =
                      state.p2pEndpointIdToNetworkRef
                        .updated(
                          p2pEndpointId,
                          new P2PNetworkRefEntry(createNetworkRef, isOutgoingConnection = true),
                        )
                    ),
                    true,
                  )
                } { _ =>
                  (state, state, false)
                }
            ) { bftNodeId =>
              // If an endpoint ID is associated with the BFT node ID, recur to the other case
              internalAddNetworkRefIfMissing(state, Right(bftNodeId))
            }

        case Right(bftNodeId) =>
          state.bftNodeIdToNetworkRef
            .get(bftNodeId)
            .fold {
              // Associate the network ref with the BFT node ID and all its endpoint IDs
              (
                state,
                state.copy(bftNodeIdToNetworkRef =
                  state.bftNodeIdToNetworkRef
                    .updated(
                      bftNodeId,
                      new P2PNetworkRefEntry(createNetworkRef, isOutgoingConnection = false),
                    )
                ),
                true,
              )
            } { _ =>
              (state, state, false)
            }
      }

    val (prevState, newState, added) =
      AtomicUtil.updateAndGetComputed(stateRef) { state =>
        val (prevState, newState, added) =
          internalAddNetworkRefIfMissing(state, p2pAddressId)
        newState -> (prevState, newState, added)
      }

    if (!added) {
      actionIfPresent()
    } else {
      // Trigger the network ref creation
      p2pAddressId match {
        case Left(p2pEndpointId) =>
          newState.p2pEndpointIdToNetworkRef.get(p2pEndpointId).foreach { networkRefEntry =>
            val networkRef = networkRefEntry.networkRef
            logger.debug(s"Created network ref ${objId(networkRef)} for endpoint $p2pEndpointId")
          }
        case Right(bftNodeId) =>
          newState.bftNodeIdToNetworkRef.get(bftNodeId).foreach { networkRefEntry =>
            val networkRef = networkRefEntry.networkRef
            logger.debug(s"Created network ref ${objId(networkRef)} for BFT node ID $bftNodeId")
          }
      }
    }

    logger.debug(s"P2P connection state before `addNetworkRefIfMissing`: $prevState")
    logger.debug(s"P2P connection state after `addNetworkRefIfMissing`: $newState")
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

    def internalShutdownConnectionAndReturnPeerSender(
        state: State,
        p2pAddressId: P2PAddress.Id,
    ): AnnotatedResult[
      (
          State,
          State,
          Option[StreamObserver[BftOrderingMessage]],
          Option[P2PNetworkRef[BftOrderingMessage]],
      )
    ] =
      p2pAddressId match {
        case Right(bftNodeId) =>
          // Remove the BFT node ID and its associated sender
          val (prevState, newState, peerSenderO) =
            unassociateAndReturnPeerSender(state, bftNodeId)
              .logAndExtract(prefix = s"Unassociating peer sender from $bftNodeId: ")
          val (updatedState, networkRefO) =
            cleanupNetworkRef(newState, bftNodeId, clearNetworkRefAssociations, closeNetworkRef)
              .logAndExtract(prefix = s"Cleaning up network ref for $bftNodeId: ")
          networkRefO.foreach { networkRef =>
            logger.debug(s"Closing ref ${objId(networkRef)} for $bftNodeId (as requested)")
            networkRef.close()
          }
          AnnotatedResult(
            (prevState, updatedState, peerSenderO, None),
            () => s"Shutdown connection for $bftNodeId",
            debug,
          )

        case Left(p2pEndpointId) =>
          state.p2pEndpointIdToBftNodeId
            .get(p2pEndpointId)
            .fold {
              // If no BFT node ID is associated, check if the endpoint ID is directly associated with a network ref
              state.p2pEndpointIdToNetworkRef
                .get(p2pEndpointId)
                .fold {
                  AnnotatedResult(
                    (
                      state,
                      state,
                      Option.empty[StreamObserver[BftOrderingMessage]],
                      Option.empty[P2PNetworkRef[BftOrderingMessage]],
                    ),
                    () => s"No connection nor network ref found for $p2pEndpointId″",
                    debug,
                  )
                } { e =>
                  if (clearNetworkRefAssociations) {
                    val updatedState =
                      state.copy(p2pEndpointIdToNetworkRef =
                        state.p2pEndpointIdToNetworkRef.removed(p2pEndpointId)
                      )
                    AnnotatedResult(
                      (state, updatedState, None, Some(e.networkRef)),
                      () =>
                        s"Network ref ${objId(e.networkRef)} unassociated from $p2pEndpointId (as requested)",
                      debug,
                    )
                  } else {
                    AnnotatedResult(
                      (state, state, None, Some(e.networkRef)),
                      () =>
                        s"Network ref ${objId(e.networkRef)} not unassociated from $p2pEndpointId (as requested)",
                      debug,
                    )
                  }
                }
            } { bftNodeId =>
              // Recur to the other case
              internalShutdownConnectionAndReturnPeerSender(
                state,
                Right(bftNodeId),
              )
            }
      }

    val (prevState, newState, peerSenderO, networkRefO) =
      AtomicUtil.updateAndGetComputed(stateRef) { state =>
        val (prevState, newState, peerSenderO, networkRefO) =
          internalShutdownConnectionAndReturnPeerSender(state, p2pAddressId).logAndExtract(prefix =
            s"Shutting down connection for $p2pAddressId: "
          )
        newState -> (prevState, newState, peerSenderO, networkRefO)
      }

    networkRefO.foreach { networkRef =>
      if (closeNetworkRef) {
        logger.debug(
          s"Closing network ref ${objId(networkRef)} for $p2pAddressId as part of connection shutdown (as requested)"
        )
        networkRef.close()
      }
    }

    logger.debug(s"P2P connection state before `shutdownConnectionAndReturnPeerSender`: $prevState")
    logger.debug(s"P2P connection state after `shutdownConnectionAndReturnPeerSender`: $newState")

    peerSenderO
  }

  private def unassociateAndReturnPeerSender(
      state: State,
      bftNodeId: BftNodeId,
  )(implicit
      traceContext: TraceContext
  ): AnnotatedResult[(State, State, Option[StreamObserver[BftOrderingMessage]])] =
    state.bftNodeIdToPeerSender
      .get(bftNodeId)
      .fold {
        AnnotatedResult(
          (
            state,
            state,
            Option.empty[StreamObserver[BftOrderingMessage]],
          ),
          () =>
            s"Not removing connection state for $bftNodeId " +
              s"because it does not exist yet (or possibly removed as duplicate)",
          debug,
        )
      } { peerSender =>
        val updatedState =
          state.copy(
            bftNodeIdToPeerSender = state.bftNodeIdToPeerSender.removed(bftNodeId),
            peerSenderToBftNodeId = state.peerSenderToBftNodeId.removed(peerSender),
          )
        AnnotatedResult(
          (state, updatedState, Some(peerSender)),
          () => s"Removed  peer sender $bftNodeId <-> ${objId(peerSender)}",
          debug,
        )
      }

  def unassociateSenderAndReturnEndpointIds(
      peerSender: StreamObserver[BftOrderingMessage]
  )(implicit traceContext: TraceContext): Seq[P2PEndpoint.Id] = {
    val (prevState, newState, result) =
      AtomicUtil
        .updateAndGetComputed(stateRef) { state =>
          state.peerSenderToBftNodeId
            .get(peerSender)
            .fold {
              state -> AnnotatedResult(
                (state, state, Seq.empty[P2PEndpoint.Id]),
                () =>
                  s"Not removing peer sender ${objId(peerSender)} because it does not exist yet (or possibly removed as duplicate)",
                debug,
              )
            } { bftNodeId =>
              val updatedState =
                state.copy(
                  peerSenderToBftNodeId = state.peerSenderToBftNodeId.removed(peerSender),
                  bftNodeIdToPeerSender = state.bftNodeIdToPeerSender.removed(bftNodeId),
                )
              updatedState -> AnnotatedResult(
                (
                  state,
                  updatedState,
                  state.p2pEndpointIdToBftNodeId.collect {
                    case (endpointId, nodeId) if nodeId == bftNodeId => endpointId
                  }.toSeq,
                ),
                () => s"Removed peer sender $bftNodeId <-> ${objId(peerSender)}",
                debug,
              )
            }
        }
        .logAndExtract(prefix = s"Unassociating sender ${objId(peerSender)}: ")
    logger.debug(s"P2P connection state before `unassociateSenderAndReturnEndpointIds`: $prevState")
    logger.debug(s"P2P connection state after `unassociateSenderAndReturnEndpointIds`: $newState")
    result
  }

  // Used to simulate a restart
  def clear(): Unit = {
    val prevState = stateRef.getAndUpdate(_ => State())
    logger.debug(s"P2P connection state before `clear`: $prevState")(TraceContext.empty)
  }

  private def biAssociateBftNodeIdWithPeerSender(
      state: State,
      bftNodeId: BftNodeId,
      peerSender: StreamObserver[BftOrderingMessage],
  ): State =
    state.copy(
      bftNodeIdToPeerSender = state.bftNodeIdToPeerSender.updated(bftNodeId, peerSender),
      peerSenderToBftNodeId = state.peerSenderToBftNodeId.updated(peerSender, bftNodeId),
    )

  private def consolidateNetworkRefs(
      state: State,
      bftNodeId: BftNodeId,
  )(implicit
      traceContext: TraceContext
  ): (State, Iterable[(P2PEndpoint.Id, P2PNetworkRef[BftOrderingMessage])]) = {
    import state.*

    val p2pEndpointIds =
      p2pEndpointIdToBftNodeId
        .collect {
          case (endpointId, nodeId) if nodeId == bftNodeId => endpointId
        }
    val maybeExistingNetworkRefAssociatedToNodeIdOrElseEndpoint =
      bftNodeIdToNetworkRef
        .get(bftNodeId)
        .map(_ -> true)
        .orElse(
          p2pEndpointIds.view
            .flatMap(p2pEndpointIdToNetworkRef.get)
            .map(_ -> false)
            .headOption
        )

    maybeExistingNetworkRefAssociatedToNodeIdOrElseEndpoint
      .map {
        // Prioritize the network ref associated to the BFT node ID (potentially an incoming connection)

        case (e, isAssociatedToNodeId) if isAssociatedToNodeId =>
          updateEndpointsNetworkRef(state, p2pEndpointIds, e)

        case (e, _) => // Associated to endpoint
          state.bftNodeIdToNetworkRef
            .get(bftNodeId)
            .foreach(impossibleNetworkRefEntry =>
              abort(
                logger,
                s"Unexpected existing network ref ${objId(impossibleNetworkRefEntry.networkRef)} associated to node ID $bftNodeId",
              )
            )
          val newState =
            copy(bftNodeIdToNetworkRef =
              bftNodeIdToNetworkRef
                .updated(bftNodeId, e)
            )
          updateEndpointsNetworkRef(newState, p2pEndpointIds, e)
      }
      .getOrElse(state -> Seq.empty)
  }

  private def updateEndpointsNetworkRef(
      state: State,
      p2pEndpointIds: Iterable[P2PEndpoint.Id],
      existingNetworkRefEntry: P2PNetworkRefEntry,
  ): (State, Iterable[(P2PEndpoint.Id, P2PNetworkRef[BftOrderingMessage])]) = {
    val updatedState =
      state.copy(p2pEndpointIdToNetworkRef =
        state.p2pEndpointIdToNetworkRef ++
          p2pEndpointIds.map { p2pEndpointId =>
            p2pEndpointId -> existingNetworkRefEntry
          }
      )
    val previousAssociatedRefs =
      p2pEndpointIds.flatMap(p2pEndpointId =>
        state.p2pEndpointIdToNetworkRef.get(p2pEndpointId).map { networkRefEntry =>
          p2pEndpointId -> networkRefEntry.networkRef
        }
      )
    updatedState ->
      (for ((p2pEndpointId, networkRef) <- previousAssociatedRefs) yield {
        Option.when(
          networkRef != existingNetworkRefEntry.networkRef
        )(p2pEndpointId -> networkRef)
      }).flatten
  }

  private def cleanupNetworkRef(
      state: State,
      bftNodeId: BftNodeId,
      clearNetworkRefAssociations: Boolean,
      closeNetworkRef: Boolean,
  )(implicit
      traceContext: TraceContext
  ): AnnotatedResult[(State, Option[P2PNetworkRef[BftOrderingMessage]])] =
    if (clearNetworkRefAssociations) {
      // Remove the BFT node ID from the network ref associations
      state.bftNodeIdToNetworkRef
        .get(bftNodeId)
        .fold {
          AnnotatedResult(
            (state, Option.empty[P2PNetworkRef[BftOrderingMessage]]),
            () => s"No network ref found for $bftNodeId",
            debug,
          )
        } { e =>
          val updatedState =
            state.copy(
              bftNodeIdToNetworkRef = state.bftNodeIdToNetworkRef.removed(bftNodeId),
              p2pEndpointIdToNetworkRef = state.p2pEndpointIdToNetworkRef.filter {
                case (p2pEndpointId, _)
                    if state.p2pEndpointIdToBftNodeId.get(p2pEndpointId).contains(bftNodeId) =>
                  false
                case _ => true
              },
            )
          AnnotatedResult(
            (updatedState, Option.when(closeNetworkRef)(e.networkRef)),
            () =>
              s"Removed network ref ${objId(e.networkRef)} for $bftNodeId and cleaned up its associations",
            debug,
          )
        }
    } else {
      AnnotatedResult(
        (state, None),
        () => s"Not removing network ref for $bftNodeId (as requested)",
        debug,
      )
    }
}

object P2PGrpcConnectionState {

  private final class P2PNetworkRefEntry(
      private val createNetworkRef: () => P2PNetworkRef[BftOrderingMessage],
      val isOutgoingConnection: Boolean,
  ) {
    lazy val networkRef: P2PNetworkRef[BftOrderingMessage] = createNetworkRef()
  }

  private final case class State(
      // Node IDs and senders are in a 1:1 relationship, so we use a bidirectional mapping.
      bftNodeIdToPeerSender: Map[BftNodeId, StreamObserver[BftOrderingMessage]] = Map.empty,
      peerSenderToBftNodeId: Map[StreamObserver[BftOrderingMessage], BftNodeId] = Map.empty,

      // Multiple endpoints may be associated to the same BFT node ID.
      p2pEndpointIdToBftNodeId: Map[P2PEndpoint.Id, BftNodeId] = Map.empty,

      // Node IDs and network refs are kept in a 1:1 relationship, but we never look up by network ref,
      //  so a single map is sufficient.
      bftNodeIdToNetworkRef: Map[BftNodeId, P2PNetworkRefEntry] = Map.empty,

      // For outgoing connections, a network ref is initially associated to an endpoint ID
      //  but is later be associated to a BFT node ID when authentication completes, or
      //  if an outgoing connection turns out to be a duplicate of an existing one
      //  for the same BFT node ID.
      p2pEndpointIdToNetworkRef: Map[P2PEndpoint.Id, P2PNetworkRefEntry] = Map.empty,
  ) extends PrettyPrinting {

    override protected def pretty: Pretty[State] =
      prettyOfClass(
        param(
          "bftNodeIdToPeerSender",
          _.bftNodeIdToPeerSender.map { case (bftNodeId, sender) =>
            bftNodeId.doubleQuoted -> objId(sender)
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
          _.bftNodeIdToNetworkRef.map { case (bftNodeId, networkRefEntry) =>
            bftNodeId.doubleQuoted -> objId(networkRefEntry.networkRef)
          },
        ),
        param(
          "p2pEndpointIdToNetworkRef",
          _.p2pEndpointIdToNetworkRef.map { case (p2pEndpointId, networkRef) =>
            p2pEndpointId -> networkRef.toString.unquoted
          },
        ),
      )
  }
}
