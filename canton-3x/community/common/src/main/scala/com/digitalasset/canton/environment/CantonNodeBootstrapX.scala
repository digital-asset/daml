// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.{LocalNodeConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.admin.v0.VaultServiceGrpc
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.{GrpcHealthReporter, HealthService}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasCloseContext, Lifecycle}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.{
  GrpcIdentityInitializationServiceX,
  GrpcTopologyAggregationServiceX,
  GrpcTopologyManagerReadServiceX,
  GrpcTopologyManagerWriteServiceX,
}
import com.digitalasset.canton.topology.admin.{v0 as adminV0, v1 as topologyProto}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{InitializationStore, TopologyStoreId, TopologyStoreX}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.SimpleExecutionQueue
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseProtocolVersion}
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.{ExecutionContext, Future}

/** CantonNodeBootstrapX trait insists that nodes have their own topology manager
  * and that they have the ability to auto-initialize their identity on their own.
  */
abstract class CantonNodeBootstrapX[
    T <: CantonNode,
    NodeConfig <: LocalNodeConfig,
    ParameterConfig <: CantonNodeParameters,
    Metrics <: BaseMetrics,
](
    arguments: CantonNodeBootstrapCommonArguments[
      NodeConfig,
      ParameterConfig,
      Metrics,
    ]
)(implicit
    executionContext: ExecutionContextIdlenessExecutorService,
    scheduler: ScheduledExecutorService,
    actorSystem: ActorSystem,
) extends CantonNodeBootstrapCommon[T, NodeConfig, ParameterConfig, Metrics](arguments) {

  protected def customNodeStages(
      storage: Storage,
      crypto: Crypto,
      nodeId: UniqueIdentifier,
      manager: AuthorizedTopologyManagerX,
      healthReporter: GrpcHealthReporter,
      healthService: HealthService,
  ): BootstrapStageOrLeaf[T]

  /** member depends on node type */
  protected def member(uid: UniqueIdentifier): Member

  override def getId: Option[NodeId] =
    startupStage.next.flatMap(_.next).flatMap(_.next.map(x => NodeId(x.nodeId)))

  override def isInitialized: Boolean = startupStage.getNode.isDefined
  override def isActive: Boolean = startupStage.next.forall(_.storage.isActive)

  override def start(): EitherT[Future, String, Unit] =
    startupStage.start().onShutdown(Left("Aborted due to shutdown"))

  override def getNode: Option[T] = startupStage.getNode
  override def crypto: Option[Crypto] = startupStage.next.flatMap(_.next).map(_.crypto)

  /** callback for topology read service
    *
    * this callback must be implemented by all node types, providing access to the domain
    * topology stores which are only available in a later startup stage (domain nodes) or
    * in the node runtime itself (participant sync domain)
    */
  // TODO(#14048) implement me!
  protected def sequencedTopologyStores: Seq[TopologyStoreX[DomainStore]] = Seq()

  protected def sequencedTopologyManagers: Seq[DomainTopologyManagerX] = Seq()

  protected val bootstrapStageCallback = new BootstrapStage.Callback {
    override def loggerFactory: NamedLoggerFactory = CantonNodeBootstrapX.this.loggerFactory
    override def timeouts: ProcessingTimeout = CantonNodeBootstrapX.this.timeouts
    override def abortThisNodeOnStartupFailure(): Unit = {
      // TODO(#14048) bubble this up into env ensuring that the node is properly deregistered from env if we fail during
      //   async startup. (node should be removed from running nodes)
      //   we can't call node.close() here as this thing is executed within a performUnlessClosing, so we'd deadlock
      logger.error("Should be closing node due to startup failure")
    }
    override val queue: SimpleExecutionQueue = initQueue
    override def ec: ExecutionContext = CantonNodeBootstrapX.this.executionContext
  }

  private val startupStage =
    new BootstrapStage[T, SetupCrypto](
      description = "Initialise storage",
      bootstrapStageCallback,
    ) {
      override def attempt()(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, String, Option[SetupCrypto]] = {
        EitherT(
          FutureUnlessShutdown.lift(
            arguments.storageFactory
              .create(
                connectionPoolForParticipant,
                arguments.parameterConfig.logQueryCost,
                arguments.clock,
                Some(scheduler),
                arguments.metrics.storageMetrics,
                arguments.parameterConfig.processingTimeouts,
                loggerFactory,
              )
              .value
          )
        ).map { storage =>
          registerHealthGauge()
          // init health services once
          val healthService = mkNodeHealthService(storage)
          addCloseable(healthService)
          val (healthReporter, grpcHealthServer, livenessHealthService) =
            mkHealthComponents(healthService)
          addCloseable(livenessHealthService)
          grpcHealthServer.foreach(addCloseable)
          addCloseable(storage)
          Some(new SetupCrypto(storage, healthReporter, healthService))
        }
      }
    }

  private class SetupCrypto(
      val storage: Storage,
      val healthReporter: GrpcHealthReporter,
      healthService: HealthService,
  ) extends BootstrapStage[T, SetupNodeId](
        description = "Init crypto module",
        bootstrapStageCallback,
      )
      with HasCloseContext {

    override def attempt()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[SetupNodeId]] = {
      // crypto factory doesn't write to the db during startup, hence,
      // we won't have "isPassive" issues here
      performUnlessClosingEitherU("create-crypto")(
        arguments.cryptoFactory
          .create(
            cryptoConfig,
            storage,
            arguments.cryptoPrivateStoreFactory,
            ReleaseProtocolVersion.latest,
            timeouts,
            loggerFactory,
            tracerProvider,
          )
          .map { crypto =>
            addCloseable(crypto)
            adminServerRegistry.addServiceU(
              VaultServiceGrpc.bindService(
                arguments.grpcVaultServiceFactory
                  .create(
                    crypto,
                    parameterConfig.enablePreviewFeatures,
                    timeouts,
                    loggerFactory,
                  ),
                executionContext,
              )
            )
            Some(new SetupNodeId(storage, crypto, healthReporter, healthService))
          }
      )
    }
  }

  private class SetupNodeId(
      storage: Storage,
      val crypto: Crypto,
      healthReporter: GrpcHealthReporter,
      healthService: HealthService,
  ) extends BootstrapStageWithStorage[T, GenerateOrAwaitNodeTopologyTx, UniqueIdentifier](
        description = "Init node id",
        bootstrapStageCallback,
        storage,
        config.init.autoInit,
      )
      with HasCloseContext
      with GrpcIdentityInitializationServiceX.Callback {

    private val initializationStore = InitializationStore(storage, timeouts, loggerFactory)
    addCloseable(initializationStore)
    private val authorizedStore =
      TopologyStoreX(
        TopologyStoreId.AuthorizedStore,
        storage,
        timeouts,
        loggerFactory,
      )
    addCloseable(authorizedStore)

    private val topologyManager: AuthorizedTopologyManagerX =
      new AuthorizedTopologyManagerX(
        clock,
        crypto,
        authorizedStore,
        config.topologyX.enableTopologyTransactionValidation,
        timeouts,
        futureSupervisor,
        loggerFactory,
      )
    addCloseable(topologyManager)
    adminServerRegistry
      .addServiceU(
        topologyProto.TopologyManagerReadServiceXGrpc
          .bindService(
            new GrpcTopologyManagerReadServiceX(
              sequencedTopologyStores :+ authorizedStore,
              crypto,
              loggerFactory,
            ),
            executionContext,
          )
      )
    adminServerRegistry
      .addServiceU(
        topologyProto.TopologyManagerWriteServiceXGrpc
          .bindService(
            new GrpcTopologyManagerWriteServiceX(
              sequencedTopologyManagers :+ topologyManager,
              authorizedStore,
              getId,
              crypto,
              parameterConfig.initialProtocolVersion,
              clock,
              loggerFactory,
            ),
            executionContext,
          )
      )
    adminServerRegistry
      .addServiceU(
        topologyProto.IdentityInitializationServiceXGrpc
          .bindService(
            new GrpcIdentityInitializationServiceX(
              clock,
              this,
              crypto.cryptoPublicStore,
            ),
            executionContext,
          )
      )
    import cats.syntax.functorFilter.*
    adminServerRegistry
      .addServiceU(
        adminV0.TopologyAggregationServiceGrpc.bindService(
          new GrpcTopologyAggregationServiceX(
            sequencedTopologyStores.mapFilter(TopologyStoreId.selectX[TopologyStoreId.DomainStore]),
            ips,
            loggerFactory,
          ),
          executionContext,
        )
      )

    override protected def stageCompleted(implicit
        traceContext: TraceContext
    ): Future[Option[UniqueIdentifier]] = initializationStore.id.map(_.map(_.identity))

    override protected def buildNextStage(uid: UniqueIdentifier): GenerateOrAwaitNodeTopologyTx =
      new GenerateOrAwaitNodeTopologyTx(
        uid,
        topologyManager,
        authorizedStore,
        storage,
        crypto,
        healthReporter,
        healthService,
      )

    override protected def autoCompleteStage()
        : EitherT[FutureUnlessShutdown, String, Option[UniqueIdentifier]] = {
      for {
        // create namespace key
        namespaceKey <- CantonNodeBootstrapCommon.getOrCreateSigningKey(crypto)(s"$name-namespace")
        // create id
        identifierName = arguments.config.init.identity
          .flatMap(_.nodeIdentifier.identifierName)
          .getOrElse(name.unwrap)
        identifier <- EitherT
          .fromEither[Future](Identifier.create(identifierName))
          .leftMap(err => s"Failed to convert name to identifier: $err")
        uid = UniqueIdentifier(
          identifier,
          Namespace(namespaceKey.fingerprint),
        )
        _ <- EitherT.right[String](initializationStore.setId(NodeId(uid)))
      } yield Option(uid)
    }.mapK(FutureUnlessShutdown.outcomeK)

    override def initializeWithProvidedId(uid: UniqueIdentifier): EitherT[Future, String, Unit] =
      completeWithExternal(
        EitherT.right(initializationStore.setId(NodeId(uid)).map(_ => uid))
      ).onShutdown(Left("Node has been shutdown"))

    override def getId: Option[UniqueIdentifier] = next.map(_.nodeId)
    override def isInitialized: Boolean = getId.isDefined
  }

  private class GenerateOrAwaitNodeTopologyTx(
      val nodeId: UniqueIdentifier,
      manager: AuthorizedTopologyManagerX,
      authorizedStore: TopologyStoreX[TopologyStoreId.AuthorizedStore],
      storage: Storage,
      crypto: Crypto,
      healthReporter: GrpcHealthReporter,
      healthService: HealthService,
  ) extends BootstrapStageWithStorage[T, BootstrapStageOrLeaf[T], Unit](
        description = "generate-or-await-node-topology-tx",
        bootstrapStageCallback,
        storage,
        config.init.autoInit,
      ) {

    override protected def stageCompleted(implicit
        traceContext: TraceContext
    ): Future[Option[Unit]] = {
      val myMember = member(nodeId)
      authorizedStore
        .findPositiveTransactions(
          CantonTimestamp.MaxValue,
          asOfInclusive = false,
          isProposal = false,
          types = Seq(OwnerToKeyMappingX.code),
          filterUid = Some(Seq(nodeId)),
          filterNamespace = None,
        )
        .map { res =>
          Option.when(
            res.result
              .filterNot(_.transaction.isProposal)
              .map(_.transaction.transaction.mapping)
              .exists {
                case OwnerToKeyMappingX(`myMember`, None, keys) =>
                  // stage is clear if we have a general signing key and possibly also an encryption key
                  // this tx can not exist without appropriate certificates, so don't need to check for them
                  keys.exists(_.isSigning) && (myMember.code != ParticipantId.Code || keys
                    .exists(x => !x.isSigning))
                case _ => false
              }
          )(())
        }
    }

    override protected def buildNextStage(result: Unit): BootstrapStageOrLeaf[T] =
      customNodeStages(
        storage,
        crypto,
        nodeId,
        manager,
        healthReporter,
        healthService,
      )

    override protected def autoCompleteStage()
        : EitherT[FutureUnlessShutdown, String, Option[Unit]] = {
      val protocolVersion = parameterConfig.initialProtocolVersion
      for {
        namespaceKeyO <- crypto.cryptoPublicStore
          .signingKey(nodeId.namespace.fingerprint)
          .leftMap(_.toString)
          .mapK(FutureUnlessShutdown.outcomeK)
        namespaceKey <- EitherT.fromEither[FutureUnlessShutdown](
          namespaceKeyO.toRight(
            s"Performing auto-init but can't find key ${nodeId.namespace.fingerprint} from previous step"
          )
        )
        // init topology manager
        nsd <- EitherT.fromEither[FutureUnlessShutdown](
          NamespaceDelegationX.create(
            Namespace(namespaceKey.fingerprint),
            namespaceKey,
            isRootDelegation = true,
          )
        )
        _ <- authorizeStateUpdate(namespaceKey, nsd, protocolVersion)
        // all nodes need a signing key
        signingKey <- CantonNodeBootstrapCommon
          .getOrCreateSigningKey(crypto)(s"$name-signing")
          .mapK(FutureUnlessShutdown.outcomeK)
        // key owner id depends on the type of node
        ownerId = member(nodeId)
        // participants need also an encryption key
        keys <-
          if (ownerId.code == ParticipantId.Code) {
            for {
              encryptionKey <- CantonNodeBootstrapCommon
                .getOrCreateEncryptionKey(crypto)(
                  s"$name-encryption"
                )
                .mapK(FutureUnlessShutdown.outcomeK)
            } yield NonEmpty.mk(Seq, signingKey, encryptionKey)
          } else {
            EitherT.rightT[FutureUnlessShutdown, String](NonEmpty.mk(Seq, signingKey))
          }
        // register the keys
        _ <- authorizeStateUpdate(
          namespaceKey,
          OwnerToKeyMappingX(ownerId, None, keys),
          protocolVersion,
        )
      } yield Some(())
    }

    private def authorizeStateUpdate(
        key: SigningPublicKey,
        mapping: TopologyMappingX,
        protocolVersion: ProtocolVersion,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Unit] = {
      manager
        .proposeAndAuthorize(
          TopologyChangeOpX.Replace,
          mapping,
          serial = None,
          Seq(key.fingerprint),
          protocolVersion,
          expectFullAuthorization = true,
        )
        // TODO(#14048) error handling
        .leftMap(_.toString)
        .map(_ => ())
    }

  }

  override protected def onClosed(): Unit = {
    Lifecycle.close(clock, initQueue, adminServerRegistry, adminServer, startupStage)(
      logger
    )
    super.onClosed()
  }

}
