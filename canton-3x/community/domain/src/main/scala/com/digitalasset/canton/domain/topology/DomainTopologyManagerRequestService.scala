// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology

import cats.data.EitherT
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.config.TopologyConfig
import com.digitalasset.canton.domain.topology.DomainTopologyManagerError.ParticipantNotInitialized
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages
import com.digitalasset.canton.protocol.messages.RegisterTopologyTransactionResponseResult
import com.digitalasset.canton.topology.TopologyManagerError.TopologyManagerAlarm
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{DomainTopologyClient, StoreBasedTopologySnapshot}
import com.digitalasset.canton.topology.processing.SnapshotAuthorizationValidator
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

private[domain] trait RequestProcessingStrategy {

  def decide(
      requestedBy: Member,
      participant: ParticipantId,
      transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[List[messages.RegisterTopologyTransactionResponseResult.State]]

}

private[domain] object RequestProcessingStrategy {

  import RegisterTopologyTransactionResponseResult.State.*

  trait ManagerHooks {
    def addFromRequest(
        transaction: SignedTopologyTransaction[TopologyChangeOp]
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, DomainTopologyManagerError, Unit]

    def issueParticipantStateForDomain(participantId: ParticipantId)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, DomainTopologyManagerError, Unit]
  }

  class Impl(
      config: TopologyConfig,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      authorizedStore: TopologyStore[AuthorizedStore],
      targetDomainClient: DomainTopologyClient,
      hooks: ManagerHooks,
      timeouts: ProcessingTimeout,
      val loggerFactory: NamedLoggerFactory,
      futureSupervisor: FutureSupervisor,
  )(implicit ec: ExecutionContext)
      extends RequestProcessingStrategy
      with NamedLogging {

    private val allowedOnboardingSet =
      TopologyStore.initialParticipantDispatchingSet + DomainTopologyTransactionType.NamespaceDelegation + DomainTopologyTransactionType.IdentifierDelegation
    private val allowedSet =
      allowedOnboardingSet + DomainTopologyTransactionType.PackageUse + DomainTopologyTransactionType.PartyToParticipant

    protected def awaitParticipantIsActive(participantId: ParticipantId)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Boolean] =
      targetDomainClient.await(_.isParticipantActive(participantId), timeouts.network.unwrap)

    private val authorizedSnapshot = makeSnapshot(authorizedStore)

    private def makeSnapshot(store: TopologyStore[AuthorizedStore]): StoreBasedTopologySnapshot =
      StoreBasedTopologySnapshot.headstateOfAuthorizedStore(store, loggerFactory)

    private def toResult[T](
        eitherT: EitherT[FutureUnlessShutdown, DomainTopologyManagerError, T]
    ): FutureUnlessShutdown[RegisterTopologyTransactionResponseResult.State] =
      eitherT
        .leftMap {
          case DomainTopologyManagerError.TopologyManagerParentError(
                TopologyManagerError.DuplicateTransaction.Failure(_, _)
              ) | DomainTopologyManagerError.TopologyManagerParentError(
                TopologyManagerError.MappingAlreadyExists.Failure(_, _)
              ) =>
            Duplicate
          case DomainTopologyManagerError.TopologyManagerParentError(err)
              if err.code == TopologyManagerError.NoCorrespondingActiveTxToRevoke =>
            Obsolete
          case err => Failed
        }
        .map(_ => Accepted)
        .merge

    private def addTransactions(
        transactions: List[SignedTopologyTransaction[TopologyChangeOp]]
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[List[RegisterTopologyTransactionResponseResult.State]] = {
      def permitted(transaction: SignedTopologyTransaction[TopologyChangeOp]): Boolean = {
        if (!allowedSet.contains(transaction.transaction.element.mapping.dbType)) {
          TopologyManagerAlarm
            .Warn(
              s"Rejecting requested change by participant of not-permitted type: ${transaction.transaction} "
            )
            .report()
          false
        } else {
          transaction.transaction.element.mapping match {
            case OwnerToKeyMapping(owner, _)
                if owner.uid == domainId.uid &&
                  owner.code != ParticipantId.Code =>
              TopologyManagerAlarm
                .Warn(
                  s"Rejecting requested change by participant of domain key: ${transaction.transaction} "
                )
                .report()
              false
            case _ => true
          }
        }
      }
      def process(transaction: SignedTopologyTransaction[TopologyChangeOp]) = {
        if (!permitted(transaction)) {
          FutureUnlessShutdown.pure(Failed)
        } else {
          FutureUnlessShutdown.outcomeF(authorizedStore.exists(transaction)).flatMap {
            case false => toResult(hooks.addFromRequest(transaction))
            case true => FutureUnlessShutdown.pure(Duplicate)
          }
        }
      }

      for {
        res <- MonadUtil.sequentialTraverse(transactions)(process)
      } yield res.toList
    }

    def processExistingParticipantRequest(
        participant: ParticipantId,
        transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[List[RegisterTopologyTransactionResponseResult.State]] = {
      // TODO(i4933) enforce limits with respect to parties, packages, keys, certificates etc.
      for {
        isActive <- FutureUnlessShutdown.outcomeF(
          authorizedSnapshot.isParticipantActive(participant)
        )
        res <-
          if (isActive) addTransactions(transactions)
          else {
            logger.warn(
              s"Failed to process participant ${participant} request as participant is not active"
            )
            FutureUnlessShutdown.pure(rejectAll(transactions))
          }
      } yield res
    }

    private def rejectAll(
        transactions: List[SignedTopologyTransaction[TopologyChangeOp]]
    ): List[RegisterTopologyTransactionResponseResult.State] =
      transactions.map(_ => RegisterTopologyTransactionResponseResult.State.Rejected)

    private def failAll(
        transactions: List[SignedTopologyTransaction[TopologyChangeOp]]
    ): List[RegisterTopologyTransactionResponseResult.State] =
      transactions.map(_ => RegisterTopologyTransactionResponseResult.State.Failed)

    private def validateOnlyOnboardingTransactions(
        participantId: ParticipantId,
        transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
    )(implicit
        traceContext: TraceContext
    ): EitherT[Future, DomainTopologyManagerError, Unit] = {
      EitherT
        .fromEither[Future](transactions.traverse { tx =>
          for {
            _ <- Either.cond(
              allowedOnboardingSet.contains(tx.transaction.element.mapping.dbType),
              (),
              s"Transaction type of ${tx.transaction.element.mapping} must not be sent as part of an on-boarding request",
            )
            _ <- Either.cond(
              tx.transaction.op == TopologyChangeOp.Add,
              (),
              s"Invalid operation of ${tx.transaction.element.mapping}: ${tx.operation}",
            )
            _ <- Either.cond(
              tx.transaction.element.mapping.requiredAuth.uids.forall(_ == participantId.uid),
              (),
              s"Invalid transaction uids in ${tx.transaction.element.mapping} not corresponding to the participant uid ${participantId.uid}",
            )
            _ <- Either.cond(
              tx.transaction.element.mapping.requiredAuth.namespaces._1
                .forall(_ == participantId.uid.namespace),
              (),
              s"Invalid transaction namespaces in ${tx.transaction.element.mapping} not corresponding to participant namespace ${participantId.uid.namespace}",
            )
          } yield ()
        })
        .leftMap[DomainTopologyManagerError](err =>
          DomainTopologyManagerError.InvalidOrFaultyOnboardingRequest
            .Failure(participantId = participantId, reason = err)
        )
        .map(_ => ())
    }

    private def validateAuthorizationOfOnboardingTransactions(
        participantId: ParticipantId,
        transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, DomainTopologyManagerError, TopologyStore[AuthorizedStore]] =
      SnapshotAuthorizationValidator
        .validateTransactions(timeouts, futureSupervisor, loggerFactory)(transactions)
        .leftMap { err =>
          DomainTopologyManagerError.InvalidOrFaultyOnboardingRequest.Failure(
            participantId,
            err,
          )
        }

    def processOnboardingRequest(
        participant: ParticipantId,
        transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[List[RegisterTopologyTransactionResponseResult.State]] = {

      type S = SignedTopologyTransaction[TopologyChangeOp]

      def extract(items: Seq[S])(matcher: TopologyMapping => Boolean) =
        items.partition(elem =>
          elem.transaction.op == TopologyChangeOp.Add &&
            matcher(elem.transaction.element.mapping)
        )

      val (participantStates, reduced1) = extract(transactions) {
        case ParticipantState(side, domain, pid, permission, _) =>
          domain == domainId && participant == pid && (side == RequestSide.To || side == RequestSide.Both) && permission.isActive
        case _ => false
      }

      def reject(msg: String) =
        DomainTopologyManagerError.ParticipantNotInitialized.Reject(participant, msg)

      def maliciousOrFaulty(msg: String) =
        DomainTopologyManagerError.InvalidOrFaultyOnboardingRequest.Failure(participant, msg)

      // if not open, check if domain trust certificate exists
      // check that we have:
      //   - owner to key mappings
      //   - domain trust certificate
      //   - if necessary, legal certificate
      //   - plus all delegations
      // if domain is open, then issue domain trust certificate,
      val ret = for {
        // check that all transactions are authorized (note, they could still be out of order)
        store <- validateAuthorizationOfOnboardingTransactions(participant, transactions)
        snapshot = makeSnapshot(store)
        // check that we only got the transactions we expected
        _ <- validateOnlyOnboardingTransactions(participant, transactions).mapK(
          FutureUnlessShutdown.outcomeK
        )
        // check that we have a domain trust certificate of the participant
        _ <- EitherT.cond[FutureUnlessShutdown](
          participantStates.nonEmpty,
          (),
          maliciousOrFaulty(s"Participant ${participant} has not sent a domain trust certificate"),
        )
        domainTrustsParticipant <- participantIsAllowed(participant).mapK(
          FutureUnlessShutdown.outcomeK
        )
        _ <- EitherT.cond[FutureUnlessShutdown](
          domainTrustsParticipant || config.open,
          (),
          reject(s"Participant ${participant} is not on the allow-list of this closed network"),
        )
        keys <- EitherT.right(snapshot.allKeys(participant)).mapK(FutureUnlessShutdown.outcomeK)
        _ <- EitherT.cond[FutureUnlessShutdown](
          keys.hasBothKeys(),
          (),
          DomainTopologyManagerError.ParticipantNotInitialized.Failure(participant, keys),
        )
        res <- EitherT.right(addTransactions(reduced1.toList))
        // Activate the participant
        _ <-
          if (!domainTrustsParticipant)
            hooks.issueParticipantStateForDomain(participant)
          else EitherT.rightT[FutureUnlessShutdown, DomainTopologyManagerError](())
        // Finally, add the participant state (keys need to be pushed before the participant is active)
        rest <- EitherT
          .right(addTransactions(participantStates.toList))
        // wait until the participant has become active on the domain. we do that such that this
        // request doesn't terminate before the participant can proceed with subscribing.
        // on shutdown, we don't wait, as the participant will subsequently fail to subscribe anyway
        active <- EitherT
          .right[DomainTopologyManagerError](
            targetDomainClient.await(
              _.isParticipantActive(participant),
              timeouts.network.unwrap,
            )
          )
      } yield {
        if (active) {
          logger.info(s"Successfully onboarded $participant")
        } else {
          logger.error(
            s"Auto-activation of participant $participant initiated, but was not observed within ${timeouts.network}"
          )
        }
        res ++ rest
      }
      ret.fold(
        {
          case ParticipantNotInitialized.Reject(_, _) => rejectAll(transactions)
          case _ => failAll(transactions)
        },
        x => x,
      )
    }

    private def participantIsAllowed(
        pid: ParticipantId
    )(implicit traceContext: TraceContext): EitherT[Future, DomainTopologyManagerError, Boolean] = {
      EitherT.right(
        authorizedStore
          .findPositiveTransactions(
            asOf =
              CantonTimestamp.MaxValue, // use max timestamp to select the "head snapshot" of the authorized store
            asOfInclusive = false,
            includeSecondary = false,
            types = Seq(DomainTopologyTransactionType.ParticipantState),
            filterUid = Some(Seq(pid.uid)),
            filterNamespace = None,
          )
          .map { res =>
            res.toIdentityState.exists {
              case TopologyStateUpdateElement(_, x: ParticipantState) => x.side == RequestSide.From
              case _ => false
            }
          }
      )
    }

    override def decide(
        requestedBy: Member,
        participant: ParticipantId,
        transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[List[RegisterTopologyTransactionResponseResult.State]] = {
      requestedBy match {
        case UnauthenticatedMemberId(uid) =>
          // check that we have:
          //   - owner to key mappings
          //   - domain trust certificate
          //   - if necessary, legal certificate
          //   - plus all delegations
          // if domain is open, then issue domain trust certificate,
          // if not open, check if domain trust certificate exists
          processOnboardingRequest(participant, transactions)
        case requester: ParticipantId if requester == participant =>
          processExistingParticipantRequest(participant, transactions)
        case member: AuthenticatedMember =>
          logger.warn(s"RequestedBy ${requestedBy} does not match participant ${participant}")
          FutureUnlessShutdown.pure(rejectAll(transactions))
      }
    }
  }

}

private[domain] class DomainTopologyManagerRequestService(
    strategy: RequestProcessingStrategy,
    crypto: CryptoPureApi,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with DomainTopologyManagerRequestService.Handler {

  import RegisterTopologyTransactionResponseResult.State.*

  override def newRequest(
      requestedBy: Member,
      participant: ParticipantId,
      res: List[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[List[RegisterTopologyTransactionResponseResult]] = {
    for {
      // run pre-checks first
      preChecked <- FutureUnlessShutdown.outcomeF(res.parTraverse(preCheck))
      preCheckedTx = res.zip(preChecked)
      valid = preCheckedTx.collect { case (tx, Accepted) => tx }
      // pass the tx that passed the pre-check to the strategy
      outcome <- strategy.decide(requestedBy, participant, valid)
    } yield {
      val (rest, result) =
        preChecked.foldLeft((outcome, List.empty[RegisterTopologyTransactionResponseResult])) {
          case ((result :: rest, acc), Accepted) =>
            (
              rest,
              acc :+ RegisterTopologyTransactionResponseResult(result),
            )
          case ((rest, acc), failed) =>
            (
              rest,
              acc :+ RegisterTopologyTransactionResponseResult(failed),
            )
        }
      ErrorUtil.requireArgument(rest.isEmpty, "rest should be empty!")
      ErrorUtil.requireArgument(
        result.lengthCompare(res) == 0,
        "result should have same length as tx",
      )
      val output = res
        .zip(result)
        .map { case (tx, response) =>
          show"${response.state.toString} -> ${tx.transaction.op} ${tx.transaction.element.mapping}"
        }
        .mkString("\n  ")
      logger.info(
        show"Register topology request by ${requestedBy} for $participant yielded\n  $output"
      )
      result
    }
  }

  private def preCheck(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  ): Future[RegisterTopologyTransactionResponseResult.State] = {
    (for {
      // check validity of signature
      _ <- EitherT
        .fromEither[Future](transaction.verifySignature(crypto))
        .leftMap(_ => Failed)
    } yield Accepted).merge
  }

}

private[domain] object DomainTopologyManagerRequestService {

  trait Handler {
    def newRequest(
        requestedBy: Member,
        participant: ParticipantId,
        res: List[SignedTopologyTransaction[TopologyChangeOp]],
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[List[RegisterTopologyTransactionResponseResult]]
  }

  def create(
      config: TopologyConfig,
      manager: DomainTopologyManager,
      domainClient: DomainTopologyClient,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      futureSupervisor: FutureSupervisor,
  )(implicit ec: ExecutionContext): DomainTopologyManagerRequestService = {

    new DomainTopologyManagerRequestService(
      new RequestProcessingStrategy.Impl(
        config,
        manager.id.domainId,
        manager.protocolVersion,
        manager.store,
        domainClient,
        manager,
        timeouts,
        loggerFactory,
        futureSupervisor,
      ),
      manager.crypto.pureCrypto,
      loggerFactory,
    )
  }
}
