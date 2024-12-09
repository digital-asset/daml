// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.daml.lf.data.Ref.PackageId
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{Crypto, Fingerprint}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown.outcomeF
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.topology.ParticipantTopologyManager.PostInitCallbacks
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError.IdentityManagerParentError
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.TopologyManagerError.ParticipantErrorGroup
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{DomainTopologyClient, TopologySnapshot}
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, blocking}

trait ParticipantTopologyManagerOps {
  def allocateParty(
      validatedSubmissionId: String255,
      partyId: PartyId,
      participantId: ParticipantId,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit]

}

trait ParticipantTopologyManagerObserver {
  def addedNewTransactions(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]
}

/** Participant side topology manager
  *
  * @param clock the participants clock
  * @param store the topology store to be used for the authorized store
  * @param crypto the set of crypto methods
  */
class ParticipantTopologyManager(
    clock: Clock,
    override val store: TopologyStore[TopologyStoreId.AuthorizedStore],
    crypto: Crypto,
    packageDependencyResolver: PackageDependencyResolver,
    override protected val timeouts: ProcessingTimeout,
    protocolVersion: ProtocolVersion,
    loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends TopologyManager[ParticipantTopologyManagerError](
      clock,
      crypto,
      store,
      timeouts,
      protocolVersion,
      loggerFactory,
      futureSupervisor,
    )(ec)
    with ParticipantTopologyManagerOps {

  private val observers = mutable.ListBuffer[ParticipantTopologyManagerObserver]()
  def addObserver(observer: ParticipantTopologyManagerObserver): Unit = blocking(synchronized {
    val _ = observers += observer
  })

  @VisibleForTesting
  private[topology] def clearObservers(): Unit = observers.clear()

  private val postInitCallbacks = new AtomicReference[Option[PostInitCallbacks]](None)
  def setPostInitCallbacks(callbacks: PostInitCallbacks): Unit =
    postInitCallbacks.set(Some(callbacks))

  private val participantIdO = new AtomicReference[Option[ParticipantId]](None)
  def setParticipantId(participantId: ParticipantId) = participantIdO.set(Some(participantId))

  private def packageAndSyncService(implicit
      traceContext: TraceContext
  ): EitherT[Future, ParticipantTopologyManagerError, PostInitCallbacks] =
    EitherT.fromEither[Future](
      postInitCallbacks
        .get()
        .toRight(
          ParticipantTopologyManagerError.UninitializedParticipant
            .Reject("Can not vet packages on an uninitialised participant")
        )
    )

  override protected def wrapError(error: TopologyManagerError)(implicit
      traceContext: TraceContext
  ): ParticipantTopologyManagerError =
    IdentityManagerParentError(error)

  override protected def notifyObservers(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): Future[Unit] =
    blocking(synchronized(observers.toList))
      .parTraverse(_.addedNewTransactions(timestamp, transactions))
      .map(_ => ())
      .onShutdown(())

  private def checkPartyHasActiveContracts(
      callbacks: PostInitCallbacks,
      transaction: PartyToParticipant,
      force: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, ParticipantTopologyManagerError, Unit] =
    transaction match {
      case PartyToParticipant(_, partyId: PartyId, _, _) =>
        if (force) {
          logger.info(
            show"Using force to disable party $partyId in participant ${transaction.participant}"
          )
          EitherT.rightT[Future, ParticipantTopologyManagerError](())
        } else {
          for {
            hasActiveContracts <- EitherT.liftF(
              callbacks
                .partyHasActiveContracts(partyId)
            )
            res <- EitherT
              .cond[Future](
                !hasActiveContracts,
                (),
                ParticipantTopologyManagerError.DisablePartyWithActiveContractsRequiresForce.Reject(
                  partyId
                ),
              ): EitherT[Future, ParticipantTopologyManagerError, Unit]
          } yield res
        }
      // anything else, pass through
      case _ => EitherT.rightT(())
    }

  private def checkOwnerToKeyMappingRefersToExistingKeys(
      participantId: ParticipantId,
      mapping: OwnerToKeyMapping,
      force: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, ParticipantTopologyManagerError, Unit] =
    mapping match {
      // if tx is for this node, check that we do have this key
      case OwnerToKeyMapping(`participantId`, key) =>
        crypto.cryptoPrivateStore
          .existsPrivateKey(key.fingerprint, key.purpose)
          .leftMap(err => wrapError(TopologyManagerError.InternalError.CryptoPrivateError(err)))
          .subflatMap { exists =>
            if (exists) {
              Right(())
            } else {
              Left(
                ParticipantTopologyManagerError.DangerousKeyUseCommandRequiresForce
                  .NoSuchKey(key.fingerprint)
              )
            }
          }

      // if tx for another node, we require force
      case OwnerToKeyMapping(pid: ParticipantId, _) if !force =>
        EitherT.leftT(
          ParticipantTopologyManagerError.DangerousKeyUseCommandRequiresForce.AlienParticipant(pid)
        )
      // anything else, pass through
      case _ => EitherT.rightT(())
    }

  private def checkPackageTopologyTransactions(
      participantId: ParticipantId,
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      force: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] =
    for {
      _ <- (transaction.transaction match {
        case TopologyStateUpdate(
              TopologyChangeOp.Add,
              TopologyStateUpdateElement(_, mapping: TopologyPackagesStateUpdateMapping),
            ) if !force =>
          checkAddPackagesTopologyTransaction(participantId, mapping)
        case TopologyStateUpdate(
              op,
              TopologyStateUpdateElement(_, vp: TopologyPackagesStateUpdateMapping),
            ) =>
          if (force) {
            logger.info(show"Using force to authorize $op of $vp")
            EitherT.rightT[FutureUnlessShutdown, ParticipantTopologyManagerError](())
          } else {
            EitherT.leftT(
              ParticipantTopologyManagerError.DangerousVettingCommandsRequireForce.Reject()
            )
          }
        case _ =>
          EitherT.rightT[FutureUnlessShutdown, ParticipantTopologyManagerError](())
      }): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit]
    } yield ()

  private def checkAddPackagesTopologyTransaction(
      participantId: ParticipantId,
      mapping: TopologyPackagesStateUpdateMapping,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] = {
    def dependenciesOf(packageIds: Seq[PackageId]) =
      packageDependencyResolver
        .packageDependencies(packageIds.toList)
        .leftMap(ParticipantTopologyManagerError.CannotVetDueToMissingPackages.Missing(_))

    mapping match {
      case VettedPackages(pid, packageIds) if pid == participantId =>
        for {
          dependencies <- dependenciesOf(packageIds)
          notVettedDependencies <- EitherT.right(outcomeF(packagesNotVetted(pid, dependencies)))
          _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
            notVettedDependencies.isEmpty,
            ParticipantTopologyManagerError.DependenciesNotVetted
              .Reject(notVettedDependencies): ParticipantTopologyManagerError,
          )
        } yield ()
      case CheckOnlyPackages(pid, packageIds) if pid == participantId =>
        for {
          dependencies <- dependenciesOf(packageIds)
          // Dependencies of check-only packages should be either check-only or vetted
          notCheckOnlyNorVettedF = packagesNotMarkedAsCheckOnly(pid, dependencies)
            .flatMap {
              case set if set.isEmpty => Future.successful(Set.empty)
              case notCheckOnly =>
                packagesNotVetted(pid, dependencies).map(_ intersect notCheckOnly)
            }: Future[Set[PackageId]]
          notCheckOnlyNorVetted <- EitherT.right(outcomeF(notCheckOnlyNorVettedF))
          _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
            notCheckOnlyNorVetted.isEmpty,
            ParticipantTopologyManagerError.DependenciesNotCheckOnlyNorVetted
              .Reject(notCheckOnlyNorVetted): ParticipantTopologyManagerError,
          )
        } yield ()
      case _ => EitherT.rightT(())
    }
  }

  private def runWithParticipantId(
      run: ParticipantId => EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] =
    runIfInitialized(participantIdO, "Participant id is not set yet", run)

  private def runWithCallbacksAndParticipantId(
      run: (
          ParticipantId,
          PostInitCallbacks,
      ) => EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] =
    runWithParticipantId(participantId =>
      runIfInitialized(
        postInitCallbacks,
        "Post init callbacks are not set yet",
        (callbacks: PostInitCallbacks) => run(participantId, callbacks),
      )
    )

  private def runIfInitialized[A](
      itemO: AtomicReference[Option[A]],
      msg: String,
      run: A => EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] =
    itemO
      .get()
      .fold(
        EitherT.leftT[FutureUnlessShutdown, Unit](
          ParticipantTopologyManagerError.UninitializedParticipant
            .Reject(msg): ParticipantTopologyManagerError
        )
      )(run(_))

  override protected def checkNewTransaction(
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      force: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] =
    transaction.transaction.element.mapping match {
      case x: PartyToParticipant if transaction.operation == TopologyChangeOp.Remove =>
        runWithCallbacksAndParticipantId((_, callbacks) =>
          checkPartyHasActiveContracts(callbacks, x, force).mapK(FutureUnlessShutdown.outcomeK)
        )
      case x: OwnerToKeyMapping if transaction.operation == TopologyChangeOp.Add =>
        runWithParticipantId(
          checkOwnerToKeyMappingRefersToExistingKeys(_, x, force).mapK(
            FutureUnlessShutdown.outcomeK
          )
        )
      case _: TopologyPackagesStateUpdateMapping =>
        runWithParticipantId(checkPackageTopologyTransactions(_, transaction, force))
      case _ => EitherT.rightT(())
    }

  def issueParticipantDomainStateCert(
      participantId: ParticipantId,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {

    def alreadyTrusted: EitherT[Future, ParticipantTopologyManagerError, Boolean] =
      EitherT.right(
        store
          .findPositiveTransactions(
            asOf = CantonTimestamp.MaxValue,
            asOfInclusive = true,
            includeSecondary = false,
            types = Seq(DomainTopologyTransactionType.ParticipantState),
            filterUid = Some(Seq(participantId.uid)),
            filterNamespace = None,
          )
          .map(_.adds.result.iterator.map(_.transaction.transaction.element.mapping).exists {
            case ParticipantState(side, `domainId`, `participantId`, permission, _)
                if side != RequestSide.From && permission.isActive =>
              true
            case _ => false
          })
      )

    def trustDomain: EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] = {
      val transaction = ParticipantState(
        RequestSide.To,
        domainId,
        participantId,
        ParticipantPermission.Submission,
        TrustLevel.Ordinary,
      )

      authorize(
        TopologyStateUpdate.createAdd(transaction, protocolVersion),
        signingKey = None,
        protocolVersion = protocolVersion,
        force = false,
        replaceExisting = true,
      ).map(_ => ())
    }

    // check if cert already exists
    val ret = for {
      have <- performUnlessClosingEitherU(functionFullName)(alreadyTrusted)
      _ <-
        if (have) EitherT.rightT[FutureUnlessShutdown, ParticipantTopologyManagerError](())
        else trustDomain
    } yield ()
    ret.leftMap(_.cause)
  }

  /** Return the packages in `packages` that are not vetted (i.e., check-only or unknown)
    */
  def packagesNotVetted(pid: ParticipantId, packages: Set[PackageId])(implicit
      traceContext: TraceContext
  ): Future[Set[PackageId]] =
    packagesNotMarkedAs(pid, packages, VettedPackages.dbType) {
      case VettedPackages(_, packageIds) => packageIds
    }

  /** Return the packages in `packages` that are not check-only (i.e., vetted or unknown)
    */
  def packagesNotMarkedAsCheckOnly(pid: ParticipantId, packages: Set[PackageId])(implicit
      traceContext: TraceContext
  ): Future[Set[PackageId]] =
    packagesNotMarkedAs(pid, packages, CheckOnlyPackages.dbType) {
      case CheckOnlyPackages(_, packageIds) => packageIds
    }

  private def packagesNotMarkedAs(
      pid: ParticipantId,
      packages: Set[PackageId],
      domainTopologyTransactionType: DomainTopologyTransactionType,
  )(
      packageIdsExtractor: PartialFunction[TopologyStateUpdateMapping, Seq[PackageId]]
  )(implicit traceContext: TraceContext): Future[Set[PackageId]] =
    this.store
      .findPositiveTransactions(
        CantonTimestamp.MaxValue,
        asOfInclusive = true,
        includeSecondary = false,
        types = Seq(domainTopologyTransactionType),
        filterUid = Some(Seq(pid.uid)),
        filterNamespace = None,
      )
      .map { current =>
        current.adds.toTopologyState
          .foldLeft(packages) {
            case (acc, TopologyStateUpdateElement(_, mapping)) =>
              packageIdsExtractor.lift(mapping).map(acc -- _).getOrElse(acc)
            case (acc, _) => acc
          }
      }

  /** @return true if all packages in the provided package set and their dependencies are available
    *         as vetted within a predefined timeout window.
    */
  def waitForPackagesBeingVetted(
      packageSet: Set[LfPackageId],
      pid: ParticipantId,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Boolean] =
    waitForTopologyState(
      _.findUnvettedPackagesOrDependencies(pid, packageSet)
        .map(_.isEmpty) // false if some packages are not vetted
        .getOrElse(false) // false if a package is unknown
        .onShutdown(false)
    )

  /** @return true if all packages in the provided package set and their dependencies are available
    *         as check-only within a predefined timeout window.
    */
  def waitForPackagesMarkedAsCheckOnly(
      packageSet: Set[LfPackageId],
      pid: ParticipantId,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Boolean] =
    waitForTopologyState(
      _.findPackagesOrDependenciesNotDeclaredAsCheckOnly(pid, packageSet)
        .map(_.isEmpty) // false if some packages are not check-only
        .getOrElse(false) // false if a package is unknown
        .onShutdown(false)
    )

  /** @return Returns true if the topology state on all domain topology clients is
    *         satisfies the [[check]] following the timeout.network.duration.
    */
  private def waitForTopologyState(
      check: TopologySnapshot => Future[Boolean]
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Boolean] =
    packageAndSyncService
      .mapK(FutureUnlessShutdown.outcomeK)
      .flatMap(callbacks =>
        EitherT.right[ParticipantTopologyManagerError](
          callbacks
            .clients()
            .parTraverse(_.await(check, timeouts.network.duration))
            .map(_.forall(identity))
        )
      )

  override def allocateParty(
      validatedSubmissionId: String255,
      partyId: PartyId,
      participantId: ParticipantId,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] = {
    val update = TopologyStateUpdate(
      TopologyChangeOp.Add,
      TopologyStateUpdateElement(
        TopologyElementId.adopt(validatedSubmissionId),
        PartyToParticipant(
          RequestSide.Both,
          partyId,
          participantId,
          ParticipantPermission.Submission,
        ),
      ),
      protocolVersion,
    )
    authorize(
      update,
      None,
      protocolVersion,
      force = false,
    ).map(_ => ())
  }

}

object ParticipantTopologyManager {
  // the sync service
  // depends on the topology manager and vice versa. therefore, we do have to inject these callbacks after init
  trait PostInitCallbacks {
    def clients(): Seq[DomainTopologyClient]
    def partyHasActiveContracts(partyId: PartyId)(implicit
        traceContext: TraceContext
    ): Future[Boolean]
  }
}

sealed trait ParticipantTopologyManagerError extends CantonError
object ParticipantTopologyManagerError extends ParticipantErrorGroup {

  final case class IdentityManagerParentError(parent: TopologyManagerError)(implicit
      val loggingContext: ErrorLoggingContext
  ) extends ParticipantTopologyManagerError
      with ParentCantonError[TopologyManagerError] {
    override def logOnCreation: Boolean = false
  }

  @Explanation(
    """This error indicates that a package vetting command failed due to packages not existing locally.
      |This can be due to either the packages not being present or their dependencies being missing.
      |When vetting a package, the package must exist on the participant, as otherwise the participant
      |will not be able to process a transaction relying on a particular package."""
  )
  @Resolution(
    "Ensure that the package exists locally before issuing such a transaction."
  )
  object CannotVetDueToMissingPackages
      extends ErrorCode(
        id = "CANNOT_VET_DUE_TO_MISSING_PACKAGES",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Missing(packages: PackageId)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Package vetting failed due to packages not existing on the local node"
        )
        with ParticipantTopologyManagerError
  }

  @Explanation(
    """This error indicates that a dangerous package vetting command was rejected.
      |This is the case if a vetting command, if not run correctly, could potentially lead to a ledger fork.
      |The vetting authorization checks the participant for the presence of the given set of
      |packages (including their dependencies) and allows only to vet for the given participant id.
      |In rare cases where a more centralised topology manager is used, this behaviour can be overridden
      |with force. However, if a package is vetted but not present on the participant, the participant will
      |refuse to process any transaction of the given domain until the problematic package has been uploaded."""
  )
  @Resolution("Set force=true if you really know what you are doing.")
  object DangerousVettingCommandsRequireForce
      extends ErrorCode(
        id = "DANGEROUS_VETTING_COMMANDS_REQUIRE_FORCE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject()(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Package vetting failed due to packages not existing on the local node"
        )
        with ParticipantTopologyManagerError
  }

  @Explanation(
    """This error indicates a vetting request failed due to dependencies not being vetted.
      |On every vetting request, the set supplied packages is analysed for dependencies. The
      |system requires that not only the main packages are vetted explicitly but also all dependencies.
      |This is necessary as not all participants are required to have the same packages installed and therefore
      |not every participant can resolve the dependencies implicitly."""
  )
  @Resolution("Vet the dependencies first and then repeat your attempt.")
  object DependenciesNotVetted
      extends ErrorCode(
        id = "DEPENDENCIES_NOT_VETTED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(unvetted: Set[PackageId])(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Package vetting failed due to dependencies not being vetted"
        )
        with ParticipantTopologyManagerError
  }

  @Explanation(
    """This error indicates a packages topology request marking a set of packages as check-only failed due to dependencies not being marked as vetted or check-only.
      |The system requires that not only the main packages are marked checked-only explicitly but also all dependencies
      |must be declared as either vetted or check-only.
      |This is necessary as not all participants are required to have the same packages installed and therefore
      |not every participant can resolve the dependencies implicitly."""
  )
  @Resolution("Vet the dependencies first and then repeat your attempt.")
  object DependenciesNotCheckOnlyNorVetted
      extends ErrorCode(
        id = "DEPENDENCIES_NOT_CHECK_ONLY_OR_VETTED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(notVettedNorCheckOnly: Set[PackageId])(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "Marking packages as check-only failed due to dependencies not being vetted or check-only"
        )
        with ParticipantTopologyManagerError
  }

  @Explanation(
    """This error indicates that a request involving topology management was attempted on a participant that is not yet initialised.
      |During initialisation, only namespace and identifier delegations can be managed."""
  )
  @Resolution("Initialise the participant and retry.")
  object UninitializedParticipant
      extends ErrorCode(
        id = "UNINITIALIZED_PARTICIPANT",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(_cause: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = _cause
        )
        with ParticipantTopologyManagerError
  }

  @Explanation(
    """This error indicates that a dangerous owner to key mapping authorization was rejected.
      |This is the case if a command is run that could break a participant.
      |If the command was run to assign a key for the given participant, then the command
      |was rejected because the key is not in the participants private store.
      |If the command is run on a participant to issue transactions for another participant,
      |then such commands must be run with force, as they are very dangerous and could easily break
      |the participant.
      |As an example, if we assign an encryption key to a participant that the participant does not
      |have, then the participant will be unable to process an incoming transaction. Therefore we must
      |be very careful to not create such situations.
      | """
  )
  @Resolution("Set force=true if you really know what you are doing.")
  object DangerousKeyUseCommandRequiresForce
      extends ErrorCode(
        id = "DANGEROUS_KEY_USE_COMMAND_REQUIRES_FORCE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class AlienParticipant(participant: ParticipantId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Issuing owner to key mappings for alien participants requires force=yes"
        )
        with ParticipantTopologyManagerError
    final case class NoSuchKey(fingerprint: Fingerprint)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"Can not assign unknown key $fingerprint to this participant"
        )
        with ParticipantTopologyManagerError
  }

  @Explanation(
    """This error indicates that a dangerous PartyToParticipant mapping deletion was rejected.
      |If the command is run and there are active contracts where the party is a stakeholder these contracts
      |will become inoperable and will never get pruned, leaking storage.
      | """
  )
  @Resolution("Set force=true if you really know what you are doing.")
  object DisablePartyWithActiveContractsRequiresForce
      extends ErrorCode(
        id = "DISABLE_PARTY_WITH_ACTIVE_CONTRACTS_REQUIRES_FORCE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(partyId: PartyId)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            show"Disable party $partyId failed because there are active contracts where the party is a stakeholder"
        )
        with ParticipantTopologyManagerError
  }

  @Explanation(
    """This error indicates that a package topology state update has timed out."""
  )
  @Resolution("Retry the operation.")
  object PackageTopologyStateUpdateTimeout
      extends ErrorCode(
        id = "PACKAGE_TOPOLOGY_STATE_UPDATE_TIMEOUT",
        ErrorCategory.DeadlineExceededRequestStateUnknown,
      ) {
    final case class Reject(override val cause: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause)
        with ParticipantTopologyManagerError
  }

}
