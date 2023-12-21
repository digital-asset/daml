// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.concurrent.{
  DirectExecutionContext,
  FutureSupervisor,
  HasFutureSupervision,
}
import com.digitalasset.canton.config.{CachingConfigs, DefaultProcessingTimeouts}
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, SyncCryptoApiProvider}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.client.{
  DomainTopologyClient,
  IdentityProvidingServiceClient,
  StoreBasedDomainTopologyClient,
  TopologySnapshot,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

trait TestingIdentityFactoryBase { self: NamedLogging =>

  protected implicit val directExecutionContext: ExecutionContext =
    DirectExecutionContext(noTracingLogger)

  def forOwner(
      owner: Member,
      availableUpToInclusive: CantonTimestamp = CantonTimestamp.MaxValue,
  ): SyncCryptoApiProvider = {
    new SyncCryptoApiProvider(
      owner,
      ips(availableUpToInclusive),
      TestingIdentityFactory.newCrypto(loggerFactory)(owner),
      CachingConfigs.testing,
      DefaultProcessingTimeouts.testing,
      FutureSupervisor.Noop,
      loggerFactory,
    )
  }

  def forOwnerAndDomain(
      owner: Member,
      domain: DomainId = DefaultTestIdentities.domainId,
      availableUpToInclusive: CantonTimestamp = CantonTimestamp.MaxValue,
  ): DomainSyncCryptoClient =
    forOwner(owner, availableUpToInclusive).tryForDomain(domain)

  protected def domains: Set[DomainId]

  private def ips(upToInclusive: CantonTimestamp): IdentityProvidingServiceClient = {
    val ips = new IdentityProvidingServiceClient()
    domains.foreach(dId =>
      ips.add(new DomainTopologyClient() with HasFutureSupervision with NamedLogging {

        override protected def loggerFactory: NamedLoggerFactory =
          self.loggerFactory

        override protected def futureSupervisor: FutureSupervisor = FutureSupervisor.Noop

        override protected val executionContext: ExecutionContext = self.directExecutionContext

        override def await(condition: TopologySnapshot => Future[Boolean], timeout: Duration)(
            implicit traceContext: TraceContext
        ): FutureUnlessShutdown[Boolean] = ???

        override def domainId: DomainId = dId

        override def trySnapshot(timestamp: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): TopologySnapshot = {
          require(timestamp <= upToInclusive, "Topology information not yet available")
          topologySnapshot(domainId, timestampForDomainParameters = timestamp)
        }

        override def currentSnapshotApproximation(implicit
            traceContext: TraceContext
        ): TopologySnapshot =
          topologySnapshot(
            domainId,
            timestampForDomainParameters = CantonTimestamp.Epoch,
          )

        override def snapshotAvailable(timestamp: CantonTimestamp): Boolean =
          timestamp <= upToInclusive

        override def awaitTimestamp(timestamp: CantonTimestamp, waitForEffectiveTime: Boolean)(
            implicit traceContext: TraceContext
        ): Option[Future[Unit]] = Option.when(timestamp > upToInclusive) {
          ErrorUtil.internalErrorAsync(
            new IllegalArgumentException(
              s"Attempt to obtain a topology snapshot at $timestamp that will never be available"
            )
          )
        }

        override def awaitTimestampUS(timestamp: CantonTimestamp, waitForEffectiveTime: Boolean)(
            implicit traceContext: TraceContext
        ): Option[FutureUnlessShutdown[Unit]] =
          awaitTimestamp(timestamp, waitForEffectiveTime).map(FutureUnlessShutdown.outcomeF)

        override def approximateTimestamp: CantonTimestamp =
          currentSnapshotApproximation(TraceContext.empty).timestamp

        override def snapshot(timestamp: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): Future[TopologySnapshot] = awaitSnapshot(timestamp)

        override def awaitSnapshot(timestamp: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): Future[TopologySnapshot] =
          Future.fromTry(Try(trySnapshot(timestamp)))

        override def awaitSnapshotUS(timestamp: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[TopologySnapshot] =
          FutureUnlessShutdown.fromTry(Try(trySnapshot(timestamp)))

        override def close(): Unit = ()

        override def topologyKnownUntilTimestamp: CantonTimestamp = upToInclusive

        override def snapshotUS(timestamp: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[TopologySnapshot] = awaitSnapshotUS(timestamp)
      })
    )
    ips
  }

  def topologySnapshot(
      domainId: DomainId = DefaultTestIdentities.domainId,
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]] =
        StoreBasedDomainTopologyClient.NoPackageDependencies,
      timestampForDomainParameters: CantonTimestamp = CantonTimestamp.Epoch,
  ): TopologySnapshot

}
