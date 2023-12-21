// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology

import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.topology.DomainTopologyManagerError.TopologyManagerParentError
import com.digitalasset.canton.error.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{
  StoreBasedDomainTopologyClient,
  StoreBasedTopologySnapshot,
}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.{AuthorizedStore, DomainStore}
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreId,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

/** Simple callback trait to inform system about changes on the topology manager
  */
trait DomainIdentityStateObserver {
  // receive update on domain topology transaction change AFTER the local change was added to the state
  def addedSignedTopologyTransaction(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): Unit = ()

  // receive an update on the participant identity change BEFORE the change is added to the state
  def willChangeTheParticipantState(
      participantId: ParticipantId,
      attributes: ParticipantAttributes,
  ): Unit = ()

}

object DomainTopologyManager {

  def transactionsAreSufficientToInitializeADomain(
      id: DomainId,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
      mustHaveActiveMediator: Boolean,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[Future, String, Unit] = {
    val store =
      new InMemoryTopologyStore(AuthorizedStore, loggerFactory, timeouts, futureSupervisor)
    val ts = CantonTimestamp.Epoch
    EitherT
      .right(
        store
          .append(
            SequencedTime(ts),
            EffectiveTime(ts),
            transactions.map(x => ValidatedTopologyTransaction(x, None)),
          )
      )
      .flatMap { _ =>
        isInitializedAt(id, store, ts.immediateSuccessor, mustHaveActiveMediator, loggerFactory)
      }
  }

  def isInitializedAt[T <: TopologyStoreId](
      id: DomainId,
      store: TopologyStore[T],
      timestamp: CantonTimestamp,
      mustHaveActiveMediator: Boolean,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[Future, String, Unit] = {
    val useStateStore = store.storeId match {
      case AuthorizedStore => false
      case DomainStore(_, _) => true
    }
    val dbSnapshot = new StoreBasedTopologySnapshot(
      timestamp,
      store,
      initKeys =
        Map(), // we need to do this because of this map here, as the target client will mix-in the map into the response
      useStateTxs = useStateStore,
      packageDependencies = StoreBasedDomainTopologyClient.NoPackageDependencies,
      loggerFactory,
    )
    def hasSigningKey(owner: Member): EitherT[Future, String, SigningPublicKey] = EitherT(
      dbSnapshot
        .signingKey(owner)
        .map(_.toRight(s"$owner signing key is missing"))
    )
    // check that we have at least one mediator and one sequencer
    for {
      // first, check that we have domain parameters
      _ <- EitherT(dbSnapshot.findDynamicDomainParameters())
      // then, check that we have at least one mediator
      mediators <- EitherT.right(dbSnapshot.mediatorGroups().map(_.flatMap(_.active)))
      _ <- EitherT.cond[Future](
        !mustHaveActiveMediator || mediators.nonEmpty,
        (),
        "No mediator domain state authorized yet.",
      )
      // check that topology manager has a signing key
      _ <- hasSigningKey(DomainTopologyManagerId(id.uid))
      // check that all mediators have a signing key
      _ <- mediators.toList.parTraverse(hasSigningKey)
      // check that sequencer has a signing key
      _ <- hasSigningKey(SequencerId(id.uid))
    } yield ()
  }

}

/** Domain manager implementation
  *
  * The domain manager is the topology manager of a domain. The read side of the domain manager is the identity
  * providing service.
  *
  * The domain manager is a special local manager but otherwise follows the same logic as a local manager.
  *
  * The domain manager exposes three main functions for manipulation:
  * - authorize - take an Identity Transaction, sign it with the given private key and add it to the state
  * - add       - take a signed Identity Transaction and add it to the given state
  * - set       - update the participant state
  *
  * In order to bootstrap a domain, we need to add appropriate signing keys for the domain identities (topology manager,
  * sequencer, mediator).
  *
  * In order to add a participant, we need to add appropriate signing and encryption keys. Once they are there, we can
  * set the participant state to enabled.
  */
class DomainTopologyManager(
    val id: DomainTopologyManagerId,
    clock: Clock,
    override val store: TopologyStore[TopologyStoreId.AuthorizedStore],
    override val crypto: Crypto,
    override protected val timeouts: ProcessingTimeout,
    val protocolVersion: ProtocolVersion,
    loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends TopologyManager[DomainTopologyManagerError](
      clock,
      crypto,
      store,
      timeouts,
      protocolVersion,
      loggerFactory,
      futureSupervisor,
    )(ec)
    with RequestProcessingStrategy.ManagerHooks {

  private val observers = new AtomicReference[List[DomainIdentityStateObserver]](List.empty)
  def addObserver(observer: DomainIdentityStateObserver): Unit = {
    observers.updateAndGet(_ :+ observer).discard
  }
  def removeObserver(observer: DomainIdentityStateObserver): Unit = {
    observers.updateAndGet(_.filterNot(_ == observer)).discard
  }
  private def sendToObservers(action: DomainIdentityStateObserver => Unit): Unit = {
    observers.get().foreach(action)
  }

  /** Authorizes a new topology transaction by signing it and adding it to the topology state
    *
    * @param transaction the transaction to be signed and added
    * @param signingKey  the key which should be used to sign
    * @param force       force dangerous operations, such as removing the last signing key of a participant
    * @param replaceExisting if true and the transaction op is add, then we'll replace existing active mappings before adding the new
    * @return            the domain state (initialized or not initialized) or an error code of why the addition failed
    */
  def authorize[Op <: TopologyChangeOp](
      transaction: TopologyTransaction[Op],
      signingKey: Option[Fingerprint],
      force: Boolean,
      replaceExisting: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainTopologyManagerError, SignedTopologyTransaction[Op]] =
    authorize(transaction, signingKey, protocolVersion, force, replaceExisting)

  override protected def notifyObservers(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): Future[Unit] =
    Future.successful(sendToObservers(_.addedSignedTopologyTransaction(timestamp, transactions)))

  /** Return a set of initial keys we can use before the sequenced store has seen any topology transaction */
  def getKeysForBootstrapping()(implicit
      traceContext: TraceContext
  ): Future[Map[Member, Seq[PublicKey]]] =
    store.findInitialState(id)

  private def checkTransactionIsNotForAlienDomainEntities(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): EitherT[Future, DomainTopologyManagerError, Unit] =
    transaction.transaction.element.mapping match {
      case OwnerToKeyMapping(ParticipantId(_), _) | OwnerToKeyMapping(MediatorId(_), _) =>
        EitherT.rightT(())
      case OwnerToKeyMapping(owner, _) if (owner.uid != this.id.uid) =>
        EitherT.leftT(DomainTopologyManagerError.AlienDomainEntities.Failure(owner.uid))
      case _ => EitherT.rightT(())
    }

  protected def checkNewTransaction(
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      force: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainTopologyManagerError, Unit] =
    for {
      _ <- checkCorrectProtocolVersion(transaction)
      _ <- checkTransactionIsNotForAlienDomainEntities(transaction)
      _ <- checkNotAddingToWrongDomain(transaction)
      _ <- checkNotEnablingParticipantWithoutKeys(transaction)
    } yield ()

  private def checkNotAddingToWrongDomain(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): EitherT[Future, DomainTopologyManagerError, Unit] = {
    val domainId = id.domainId
    transaction.restrictedToDomain match {
      case None | Some(`domainId`) => EitherT.pure(())
      case Some(otherDomain) =>
        EitherT.leftT(DomainTopologyManagerError.WrongDomain.Failure(otherDomain))
    }
  }

  // Representative for the class of protocol versions of SignedTopologyTransaction
  private val signedTopologyTransactionRepresentativeProtocolVersion =
    SignedTopologyTransaction.protocolVersionRepresentativeFor(protocolVersion)

  private def checkCorrectProtocolVersion(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): EitherT[Future, DomainTopologyManagerError, Unit] = {
    val resultE = for {
      _ <- Either.cond(
        transaction.representativeProtocolVersion == signedTopologyTransactionRepresentativeProtocolVersion,
        (),
        DomainTopologyManagerError.WrongProtocolVersion.Failure(
          domainProtocolVersion = protocolVersion,
          transactionProtocolVersion = transaction.representativeProtocolVersion.representative,
        ),
      )

      domainId = id.domainId
      _ <- transaction.restrictedToDomain match {
        case None | Some(`domainId`) => Right(())
        case Some(otherDomain) =>
          Left(DomainTopologyManagerError.WrongDomain.Failure(otherDomain))
      }
    } yield ()
    EitherT.fromEither[Future](resultE)
  }

  private def checkNotEnablingParticipantWithoutKeys(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): EitherT[Future, DomainTopologyManagerError, Unit] =
    if (transaction.operation == TopologyChangeOp.Remove) EitherT.pure(())
    else
      transaction.transaction.element.mapping match {
        // in v5, the domain topology message validator expects to get the From before the To
        // and no Both. so we need to ensure that at no point, a Both is added to the domain manager
        case ParticipantState(RequestSide.Both, _, participant, _, _) =>
          EitherT.leftT(
            DomainTopologyManagerError.InvalidOrFaultyOnboardingRequest
              .Failure(
                participant,
                reason = "RequestSide.Both is not supported for participant states",
              )
          )
        case ParticipantState(side, _, participant, permission, _) =>
          checkParticipantHasKeys(side, participant, permission)
        case _ => EitherT.pure(())
      }

  private def checkParticipantHasKeys(
      side: RequestSide,
      participantId: ParticipantId,
      permission: ParticipantPermission,
  )(implicit traceContext: TraceContext): EitherT[Future, DomainTopologyManagerError, Unit] = {
    if (permission == ParticipantPermission.Disabled) EitherT.rightT(())
    else {
      for {
        txs <- EitherT.right(
          store.findPositiveTransactions(
            CantonTimestamp.MaxValue,
            asOfInclusive = false,
            includeSecondary = false,
            types = Seq(
              DomainTopologyTransactionType.OwnerToKeyMapping,
              DomainTopologyTransactionType.ParticipantState,
            ),
            filterUid = Some(
              Seq(participantId.uid)
            ), // unique path is using participant id for both types of topo transactions
            filterNamespace = None,
          )
        )
        keysAndSides = txs.adds.toTopologyState
          .map(_.mapping)
          .foldLeft((KeyCollection(Seq(), Seq()), false)) {
            case ((keys, sides), OwnerToKeyMapping(`participantId`, key)) =>
              (keys.addTo(key), sides)
            case (
                  (keys, _),
                  ParticipantState(otherSide, domain, `participantId`, permission, _),
                ) if permission.isActive && id.domainId == domain && side != otherSide =>
              (keys, true)
            case (acc, _) => acc
          }
        (keys, hasOtherSide) = keysAndSides
        _ <- EitherT.cond[Future](
          keys.hasBothKeys() || (!hasOtherSide && side != RequestSide.Both),
          (),
          DomainTopologyManagerError.ParticipantNotInitialized
            .Failure(participantId, keys): DomainTopologyManagerError,
        )
      } yield ()
    }
  }

  override protected def preNotifyObservers(
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]]
  )(implicit traceContext: TraceContext): Unit = {
    transactions.iterator
      .filter(_.transaction.op == TopologyChangeOp.Add)
      .map(_.transaction.element.mapping)
      .foreach {
        case ParticipantState(side, _, participant, permission, trustLevel)
            if side != RequestSide.To =>
          val attributes = ParticipantAttributes(permission, trustLevel)
          sendToObservers(_.willChangeTheParticipantState(participant, attributes))
          logger.info(s"Setting participant $participant state to $attributes")
        case _ => ()
      }
  }

  override protected def wrapError(error: TopologyManagerError)(implicit
      traceContext: TraceContext
  ): DomainTopologyManagerError =
    TopologyManagerParentError(error)

  /** Return true if domain identity is sufficiently initialized such that it can be used */
  def isInitialized(
      mustHaveActiveMediator: Boolean,
      logReason: Boolean = true,
  )(implicit traceContext: TraceContext): Future[Boolean] =
    isInitializedET(mustHaveActiveMediator).value
      .map {
        case Left(reason) =>
          if (logReason)
            logger.debug(s"Domain is not yet initialised: ${reason}")
          false
        case Right(_) => true
      }

  def isInitializedET(
      mustHaveActiveMediator: Boolean
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
    DomainTopologyManager
      .isInitializedAt(
        id.domainId,
        store,
        CantonTimestamp.MaxValue,
        mustHaveActiveMediator,
        loggerFactory,
      )

  override def issueParticipantStateForDomain(participantId: ParticipantId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainTopologyManagerError, Unit] = {
    authorize(
      TopologyStateUpdate.createAdd(
        ParticipantState(
          RequestSide.From,
          id.domainId,
          participantId,
          ParticipantPermission.Submission,
          TrustLevel.Ordinary,
        ),
        protocolVersion,
      ),
      signingKey = None,
      force = false,
      replaceExisting = true,
    ).map(_ => ())
  }

  override def addFromRequest(transaction: SignedTopologyTransaction[TopologyChangeOp])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainTopologyManagerError, Unit] =
    add(transaction, force = true, replaceExisting = false, allowDuplicateMappings = true)
}

sealed trait DomainTopologyManagerError extends CantonError with Product with Serializable
object DomainTopologyManagerError extends TopologyManagerError.DomainErrorGroup() {

  final case class TopologyManagerParentError(parent: TopologyManagerError)(implicit
      val loggingContext: ErrorLoggingContext
  ) extends DomainTopologyManagerError
      with ParentCantonError[TopologyManagerError]

  @Explanation(
    """This error indicates an external issue with the member addition hook."""
  )
  @Resolution("""Consult the error details.""")
  object FailedToAddMember
      extends ErrorCode(
        id = "FAILED_TO_ADD_MEMBER",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Failure(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "The add member hook failed"
        )
        with DomainTopologyManagerError
  }

  @Explanation(
    """This error is returned if a domain topology manager attempts to activate a
      participant without having all necessary data, such as keys or domain trust certificates."""
  )
  @Resolution("""Register the necessary keys or trust certificates and try again.""")
  object ParticipantNotInitialized
      extends ErrorCode(
        id = "PARTICIPANT_NOT_INITIALIZED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(participantId: ParticipantId, currentKeys: KeyCollection)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "The participant can not be enabled without registering the necessary keys first"
        )
        with DomainTopologyManagerError
    final case class Reject(participantId: ParticipantId, reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(reason)
        with DomainTopologyManagerError
  }

  object InvalidOrFaultyOnboardingRequest
      extends AlarmErrorCode(id = "MALICOUS_OR_FAULTY_ONBOARDING_REQUEST") {

    final case class Failure(participantId: ParticipantId, reason: String)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends Alarm(
          cause =
            "The participant submitted invalid or insufficient topology transactions during onboarding"
        )
        with DomainTopologyManagerError
  }

  @Explanation(
    """This error is returned if a transaction attempts to add keys for alien domain manager or sequencer entities to this domain topology manager."""
  )
  @Resolution(
    """Use a participant topology manager if you want to manage foreign domain keys"""
  )
  object AlienDomainEntities
      extends ErrorCode(
        id = "ALIEN_DOMAIN_ENTITIES",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(alienUid: UniqueIdentifier)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "Keys of alien domain entities can not be managed through a domain topology manager"
        )
        with DomainTopologyManagerError
  }

  @Explanation(
    """This error is returned if a transaction has a protocol version different than the one spoken on the domain."""
  )
  @Resolution(
    """Recreate the transaction with a correct protocol version."""
  )
  object WrongProtocolVersion
      extends ErrorCode(
        id = "WRONG_PROTOCOL_VERSION",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(
        domainProtocolVersion: ProtocolVersion,
        transactionProtocolVersion: ProtocolVersion,
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            "Mismatch between protocol version of the transaction and the one spoken on the domain."
        )
        with DomainTopologyManagerError
  }

  @Explanation(
    """This error is returned if a transaction restricted to a domain should be added to another domain."""
  )
  @Resolution(
    """Recreate the content of the transaction with a correct domain identifier."""
  )
  object WrongDomain
      extends ErrorCode(id = "WRONG_DOMAIN", ErrorCategory.InvalidGivenCurrentSystemStateOther) {
    final case class Failure(wrongDomain: DomainId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Domain restricted transaction can not be added to different domain"
        )
        with DomainTopologyManagerError
  }

}
