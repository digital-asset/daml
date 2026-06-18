// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.elements.dvp

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.javaapi.data.Identifier
import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.MetricName
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.performance.Connectivity
import com.digitalasset.canton.performance.PartyRole.DvpIssuer
import com.digitalasset.canton.performance.acs.ContractStore
import com.digitalasset.canton.performance.elements.{DriverControl, ParticipantDriver}
import com.digitalasset.canton.performance.model.java as M
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.FutureUtil
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters.*

/** @param connectivity
  *   Connectivity to the participant
  * @param partyLf
  *   Trader party (tradie)
  * @param masterLf
  *   Master party
  * @param role
  *   Role of the trader
  * @param prefix
  *   Prefix for the metrics
  * @param control
  *   Parameters for the performance runner
  * @param baseSynchronizerId
  *   Main synchronizer to be used (e.g., assets creation)
  */
class IssuerDriver(
    connectivity: Connectivity,
    partyLf: LfPartyId,
    masterLf: LfPartyId,
    role: DvpIssuer,
    prefix: MetricName,
    metricsFactory: LabeledMetricsFactory,
    loggerFactory: NamedLoggerFactory,
    control: DriverControl,
    baseSynchronizerId: SynchronizerId,
)(implicit
    ec: ExecutionContextExecutor,
    actorSystem: ActorSystem,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends ParticipantDriver(
      connectivity,
      partyLf,
      masterLf,
      role,
      registerGenerator = false,
      prefix,
      metricsFactory,
      loggerFactory,
      control,
      baseSynchronizerId,
    ) {

  protected val submittedRegistryRequest = new AtomicBoolean(false)
  protected val registry = new ContractStore[
    M.dvp.asset.Registry.Contract,
    M.dvp.asset.Registry.ContractId,
    M.dvp.asset.Registry,
    Unit,
  ](
    "registry agreement",
    M.dvp.asset.Registry.COMPANION,
    index = _ => (),
    filter = _ => true, // x => x.data.issuer == party.getValue,
    loggerFactory,
  )

  protected val requests = new ContractStore[
    M.dvp.asset.AssetRequest.Contract,
    M.dvp.asset.AssetRequest.ContractId,
    M.dvp.asset.AssetRequest,
    Unit,
  ](
    "asset requests",
    M.dvp.asset.AssetRequest.COMPANION,
    index = _ => (),
    filter = x => x.data.issuer == party.getValue,
    loggerFactory,
  )

  listeners.appendAll(Seq(requests, registry))

  override def flush(): Boolean =
    registry.one(()) match {
      case Some(registryContract) if (!requests.hasPending) =>
        requests.allAvailable.foreach { request =>
          val cmd = request.id.exerciseAcceptRequest(registryContract.id).commands.asScala.toSeq
          val submissionF =
            submitCommand(
              "accept-request",
              cmd,
              s"approving asset request by ${request.data.requester} on ${request.data.issuer}",
            )
          setPending(requests, request.id, submissionF)
        }
        // issuer doesn't have an active role during dvps
        ensureFlag(M.orchestration.ParticipantFlag.FINISHED)
        false
      case None =>
        // create registry if this is self-registry mode. otherwise, wait until
        // the registry is created externally
        if (role.selfRegistrar) {
          if (!submittedRegistryRequest.getAndSet(true)) {
            logger.info("Creating self-registry for issuer " + partyLf)
            FutureUtil.doNotAwait(
              submitCommand(
                "create-registry",
                new M.dvp.asset.Registry(
                  party.getValue,
                  party.getValue,
                ).create.commands.asScala.toSeq,
                s"creating self registry for $partyLf",
                fixedCommandId = Some("create-registry-" + role.name),
              ).map { res =>
                if (!res) {
                  submittedRegistryRequest.set(false)
                }
              },
              "failed to create self-registry.",
            )
          }
        } else {
          logger.debug("Waiting for registry to be created externally")
          ensureFlag(M.orchestration.ParticipantFlag.INITIALISING)
        }
        false
      case _ => false
    }

  override protected def subscribeToTemplates: Seq[Identifier] =
    Seq(
      M.dvp.asset.Asset.TEMPLATE_ID,
      M.dvp.asset.AssetTransfer.TEMPLATE_ID,
      M.dvp.asset.AssetRequest.TEMPLATE_ID,
      M.dvp.asset.Registry.TEMPLATE_ID,
    )

  override def start(): Future[Either[String, Unit]] = {
    done_.success(()) // we don't need to wait until we are done
    super.start()
  }

}
