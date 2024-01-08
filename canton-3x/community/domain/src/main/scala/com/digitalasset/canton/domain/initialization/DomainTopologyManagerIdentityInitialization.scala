// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.initialization

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.InitConfigBase
import com.digitalasset.canton.crypto.SigningPublicKey
import com.digitalasset.canton.domain.topology.DomainTopologyManager
import com.digitalasset.canton.environment.{CantonNodeBootstrapBase, CantonNodeBootstrapCommon}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.{DynamicDomainParameters, StaticDomainParameters}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

trait DomainTopologyManagerIdentityInitialization[StoredNodeConfig] {

  self: CantonNodeBootstrapBase[_, _, _, _] =>

  protected def authorizeDomainGovernance[E <: CantonError](
      manager: TopologyManager[E],
      key: SigningPublicKey,
      mapping: DomainGovernanceMapping,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    authorizeIfNew(
      manager,
      DomainGovernanceTransaction(mapping, protocolVersion),
      key,
      protocolVersion,
    )

  def initializeTopologyManagerIdentity(
      name: InstanceName,
      initialDynamicDomainParameters: DynamicDomainParameters,
      initConfigBase: InitConfigBase,
      staticDomainParametersFromConfig: StaticDomainParameters,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, (NodeId, DomainTopologyManager, SigningPublicKey)] = {
    // initialize domain with local keys
    val idName =
      initConfigBase.identity.flatMap(_.nodeIdentifier.identifierName).getOrElse(name.unwrap)
    for {
      id <- Identifier.create(idName).toEitherT[FutureUnlessShutdown]
      // first, we create the namespace key for this node
      namespaceKey <- CantonNodeBootstrapCommon
        .getOrCreateSigningKey(crypto.value)(
          s"$name-namespace"
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      // then, we create the topology manager signing key
      topologyManagerSigningKey <- CantonNodeBootstrapCommon
        .getOrCreateSigningKey(crypto.value)(
          s"$name-topology-manager-signing"
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      // using the namespace key and the identifier, we create the uid of this node
      uid = UniqueIdentifier(id, Namespace(namespaceKey.fingerprint))
      nodeId = NodeId(uid)
      // now, we kick off the topology manager services so we can start building topology transactions
      topologyManager <- initializeIdentityManagerAndServices(
        nodeId,
        staticDomainParametersFromConfig,
      )
        .toEitherT[FutureUnlessShutdown]
      // first, we issue the root namespace delegation for our namespace
      _ <- authorizeStateUpdate(
        topologyManager,
        namespaceKey,
        NamespaceDelegation(
          Namespace(namespaceKey.fingerprint),
          namespaceKey,
          isRootDelegation = true,
        ),
        staticDomainParametersFromConfig.protocolVersion,
      )
      // then, we initialise the domain parameters
      _ <- authorizeDomainGovernance(
        topologyManager,
        namespaceKey,
        DomainParametersChange(DomainId(uid), initialDynamicDomainParameters),
        staticDomainParametersFromConfig.protocolVersion,
      )

      domainTopologyManagerId = DomainTopologyManagerId(uid)

      // now, we assign the topology manager key with the domain topology manager
      _ <- authorizeStateUpdate(
        topologyManager,
        namespaceKey,
        OwnerToKeyMapping(domainTopologyManagerId, topologyManagerSigningKey),
        staticDomainParametersFromConfig.protocolVersion,
      )

    } yield (nodeId, topologyManager, namespaceKey)
  }

  protected def initializeIdentityManagerAndServices(
      nodeId: NodeId,
      staticDomainParameters: StaticDomainParameters,
  ): Either[String, DomainTopologyManager]

}
