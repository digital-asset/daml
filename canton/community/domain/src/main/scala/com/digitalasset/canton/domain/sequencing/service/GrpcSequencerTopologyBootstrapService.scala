// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.syntax.traverse.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.admin.v0.TopologyBootstrapRequest
import com.digitalasset.canton.domain.admin.v0.TopologyBootstrapServiceGrpc.TopologyBootstrapService
import com.digitalasset.canton.domain.initialization.TopologyManagementInitialization
import com.digitalasset.canton.domain.topology.DomainTopologyManager
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.topology.{DomainId, DomainMember}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.empty.Empty
import io.grpc.{Status, StatusException}

import scala.concurrent.{ExecutionContext, Future}

class GrpcSequencerTopologyBootstrapService(
    id: DomainId,
    protocolVersion: ProtocolVersion,
    syncCrypto: DomainSyncCryptoClient,
    client: SequencerClient,
    isInitialized: () => Future[Boolean],
    val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    timeouts: ProcessingTimeout,
)(implicit executionContext: ExecutionContext)
    extends TopologyBootstrapService
    with NamedLogging {

  override def bootstrap(request: TopologyBootstrapRequest): Future[Empty] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    EitherTUtil.toFuture(for {
      initialized <- EitherT.right[StatusException](isInitialized())
      _ <-
        if (initialized) {
          logger.info(s"Topology has already been initialized, so ignoring bootstrap request")
          EitherT.rightT[Future, StatusException](())
        } else
          for {
            topologySnapshotO <- EitherT
              .fromEither[Future](
                request.initialTopologySnapshot
                  .traverse(StoredTopologyTransactions.fromProtoV0)
              )
              .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.toString).asException())
            topologySnapshot = topologySnapshotO
              .getOrElse(StoredTopologyTransactions.empty)
            _ <- DomainTopologyManager
              .transactionsAreSufficientToInitializeADomain(
                id,
                topologySnapshot.result.map(_.transaction),
                mustHaveActiveMediator = false,
                loggerFactory,
                timeouts,
                futureSupervisor,
              )
              .leftMap(Status.INVALID_ARGUMENT.withDescription(_).asException())
            _ <- EitherT
              .right[StatusException](
                TopologyManagementInitialization
                  .sequenceInitialTopology(
                    id,
                    protocolVersion,
                    client,
                    topologySnapshot.result.map(_.transaction),
                    DomainMember.listAll(id),
                    syncCrypto.headSnapshot,
                    loggerFactory,
                  )
              )
          } yield ()
    } yield Empty())
  }
}
