// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.daml.lf.value.Value
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.FullTransactionViewTree
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  NamedLoggerFactory,
  NamedLogging,
  NamedLoggingContext,
}
import com.digitalasset.canton.participant.protocol.validation.InternalConsistencyChecker.*
import com.digitalasset.canton.protocol.RollbackContext.RollbackScope
import com.digitalasset.canton.protocol.{
  LfContractId,
  LfGlobalKey,
  RollbackContext,
  WithRollbackScope,
}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DiscardOps, LfPartyId}

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

class InternalConsistencyChecker(
    uniqueContractKeys: Boolean,
    protocolVersion: ProtocolVersion,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  /** Checks if there is no internal consistency issue between views, e.g., it would return an error if
    * there are two different views (within the same rollback scope) that archive the same contract.
    *
    * The method does not check for consistency issues inside of a single view. This is checked by Daml engine as part
    * of [[ModelConformanceChecker]].
    */
  def check(
      rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]],
      hostedParty: LfPartyId => Boolean,
  )(implicit
      traceContext: TraceContext
  ): Either[ErrorWithInternalConsistencyCheck, Unit] = {
    for {
      _ <- checkRollbackScopes(rootViewTrees)
      _ <- checkContractState(rootViewTrees)
      _ <- checkKeyState(rootViewTrees, hostedParty)
    } yield ()
  }

  private def checkRollbackScopes(
      rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]]
  ): Result[Unit] =
    checkRollbackScopeOrder(
      rootViewTrees.map(_.viewParticipantData.rollbackContext)
    ).left.map { error =>
      ErrorWithInternalConsistencyCheck(IncorrectRollbackScopeOrder(error))
    }

  private def checkContractState(
      rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]]
  )(implicit traceContext: TraceContext): Result[Unit] = {

    MonadUtil
      .foldLeftM[Result, ContractState, FullTransactionViewTree](
        ContractState.empty,
        rootViewTrees,
      )((previous, rootViewTree) => {

        val state = adjustRollbackScope[ContractState, Set[LfContractId]](
          previous,
          rootViewTree.viewParticipantData.tryUnwrap.rollbackContext.rollbackScope,
        )

        val created = rootViewTree.view.createdContracts.keySet
        val input = rootViewTree.view.inputContracts.keySet
        val consumed = rootViewTree.view.consumed.keySet

        val referenced = created ++ input

        for {
          _ <- checkNotUsedBeforeCreation(state.referenced, created)
          _ <- checkNotUsedAfterArchive(state.consumed, referenced)
        } yield state.update(referenced = referenced, consumed = consumed)

      })
      .map(_.discard)
  }

  private def checkNotUsedBeforeCreation(
      previouslyReferenced: Set[LfContractId],
      newlyCreated: Set[LfContractId],
  ): Result[Unit] = {
    NonEmpty.from(newlyCreated.intersect(previouslyReferenced)) match {
      case Some(ne) => Left(ErrorWithInternalConsistencyCheck(UsedBeforeCreation(ne)))
      case None => Right(())
    }
  }

  private def checkNotUsedAfterArchive(
      previouslyConsumed: Set[LfContractId],
      newlyReferenced: Set[LfContractId],
  ): Result[Unit] = {
    NonEmpty.from(previouslyConsumed.intersect(newlyReferenced)) match {
      case Some(ne) => Left(ErrorWithInternalConsistencyCheck(UsedAfterArchive(ne)))
      case None => Right(())
    }
  }

  private def checkKeyState(
      rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]],
      hostedParty: LfPartyId => Boolean,
  )(implicit traceContext: TraceContext): Result[Unit] = {

    if (uniqueContractKeys) {

      // Validating only hosted keys (i.e. keys where this participant hosts a maintainer),
      // because for non-hosted keys, we do not necessarily see all updates and may therefore
      // raise a false alarm.
      val hostedKeys =
        rootViewTrees.map(_.view.globalKeyInputs).foldLeft(Set.empty[LfGlobalKey]) { (keys, gki) =>
          keys ++ (gki -- keys).collect {
            case (key, a) if a.maintainers.exists(hostedParty) => key
          }
        }

      def hostedUpdatedKeys(rootViewTree: FullTransactionViewTree): KeyMapping =
        rootViewTree.view.updatedKeyValues.view
          .filterKeys(hostedKeys)
          .toMap

      MonadUtil
        .foldLeftM[Result, KeyState, FullTransactionViewTree](KeyState.empty, rootViewTrees)(
          (previous, rootViewTree) => {

            val state = adjustRollbackScope[KeyState, KeyMapping](
              previous,
              rootViewTree.viewParticipantData.rollbackContext.rollbackScope,
            )

            val viewGlobal = rootViewTree.view.globalKeyInputs.view
              .filterKeys(hostedKeys)
              .mapValues(_.resolution)
              .toMap

            for {
              _ <- checkConsistentKeyUse(state.inconsistentKeys(viewGlobal))
            } yield state.update(viewGlobal, hostedUpdatedKeys(rootViewTree))

          }
        )
        .map(_.discard)
    } else {
      Right(())
    }
  }

  /** @param inconsistent - the set of inconsistent keys or the empty set if no inconsistencies have been found,
    *                     see [[KeyState.inconsistentKeys]] to see how inconsistency is detected.
    * @return - returns a failed result if there are inconsistent keys
    */
  private def checkConsistentKeyUse(inconsistent: Set[LfGlobalKey]): Result[Unit] = {
    NonEmpty.from(inconsistent) match {
      case Some(ne) => Left(ErrorWithInternalConsistencyCheck(InconsistentKeyUse(ne)))
      case None => Right(())
    }
  }

}

object InternalConsistencyChecker {

  type Result[R] = Either[ErrorWithInternalConsistencyCheck, R]

  sealed trait Error extends PrettyPrinting

  /** This trait manages pushing the active state onto a stack when a new rollback context
    * is entered and restoring the rollback back active state when a rollback scope is
    * exited.
    *
    * It is assumed that not all rollback scopes will be presented to [[adjustRollbackScope]]
    * in order but there may be hierarchical jumps in rollback scopt between calls.
    */
  private sealed trait PushPopRollbackScope[M <: PushPopRollbackScope[M, T], T] {

    def self: M

    def rollbackScope: RollbackScope

    def stack: List[WithRollbackScope[T]]

    /** State that will be recorded / restored at the beginning / end of a rollback scope. */
    def activeRollbackState: T

    def copyWith(
        rollbackScope: RollbackScope,
        activeState: T,
        stack: List[WithRollbackScope[T]],
    ): M

    private[validation] def pushRollbackScope(newScope: RollbackScope): M =
      copyWith(
        newScope,
        activeRollbackState,
        WithRollbackScope(rollbackScope, activeRollbackState) :: stack,
      )

    private[validation] def popRollbackScope(): M = stack match {
      case WithRollbackScope(stackScope, stackActive) :: other =>
        copyWith(rollbackScope = stackScope, activeState = stackActive, stack = other)
      case _ =>
        throw new IllegalStateException(
          s"Unable to pop scope of empty stack ${getClass.getSimpleName}"
        )
    }
  }

  private final def adjustRollbackScope[M <: PushPopRollbackScope[M, T], T](
      starting: M,
      targetScope: RollbackScope,
  ): M = {
    @tailrec def loop(current: M): M = {
      RollbackScope.popsAndPushes(current.rollbackScope, targetScope) match {
        case (0, 0) => current
        case (0, _) => current.pushRollbackScope(targetScope)
        case _ => loop(current.popRollbackScope())
      }
    }

    loop(starting)
  }

  /** @param referenced - Contract ids used or created by previous views, including rolled back usages and creations.
    * @param rollbackScope - The current rollback scope
    * @param consumed - Contract ids consumed in a previous view
    * @param stack - The stack of rollback scopes that are currently open
    */
  private final case class ContractState(
      referenced: Set[LfContractId],
      rollbackScope: RollbackScope,
      consumed: Set[LfContractId],
      stack: List[WithRollbackScope[Set[LfContractId]]],
  ) extends PushPopRollbackScope[ContractState, Set[LfContractId]] {

    override def self: ContractState = this

    override def activeRollbackState: Set[LfContractId] = consumed

    override def copyWith(
        rollbackScope: RollbackScope,
        activeState: Set[LfContractId],
        stack: List[WithRollbackScope[Set[LfContractId]]],
    ): ContractState = copy(rollbackScope = rollbackScope, consumed = activeState, stack = stack)

    def update(referenced: Set[LfContractId], consumed: Set[LfContractId]): ContractState = {
      copy(referenced = this.referenced ++ referenced, consumed = this.consumed ++ consumed)
    }

  }

  private object ContractState {
    val empty: ContractState = ContractState(Set.empty, RollbackScope.empty, Set.empty, Nil)
  }

  private type KeyResolution = Option[Value.ContractId]
  private type KeyMapping = Map[LfGlobalKey, KeyResolution]

  /** @param preResolutions - The the key resolutions that must be in place before processing the transaction in order to successfully process all previous views.
    * @param rollbackScope - The current rollback scope
    * @param activeResolutions - The key resolutions that are in place after processing all previous views that have been changed by some previous view. This excludes changes to key resolutions performed by rolled back views.
    * @param stack - The stack of rollback scopes that are currently open
    */
  private final case class KeyState(
      preResolutions: KeyMapping,
      rollbackScope: RollbackScope,
      activeResolutions: KeyMapping,
      stack: List[WithRollbackScope[KeyMapping]],
  ) extends PushPopRollbackScope[KeyState, KeyMapping] {

    override def self: KeyState = this

    override def activeRollbackState: KeyMapping = activeResolutions

    override def copyWith(
        rollbackScope: RollbackScope,
        activeState: KeyMapping,
        stack: List[WithRollbackScope[KeyMapping]],
    ): KeyState =
      copy(rollbackScope = rollbackScope, activeResolutions = activeState, stack = stack)

    def inconsistentKeys(
        viewKeyMappings: Map[LfGlobalKey, KeyResolution]
    ): Set[LfGlobalKey] = {
      (for {
        (key, previousResolution) <- preResolutions ++ activeRollbackState
        currentResolution <- viewKeyMappings.get(key)
        if previousResolution != currentResolution
      } yield key).toSet
    }

    def update(viewGlobal: KeyMapping, updates: KeyMapping): KeyState = {
      copy(
        preResolutions = preResolutions ++ (viewGlobal -- preResolutions.keySet),
        activeResolutions = activeResolutions ++ updates,
      )
    }

  }

  private object KeyState {
    val empty: KeyState = KeyState(Map.empty, RollbackScope.empty, Map.empty, Nil)
  }

  final case class ErrorWithInternalConsistencyCheck(error: Error) extends PrettyPrinting {
    override def pretty: Pretty[ErrorWithInternalConsistencyCheck] =
      prettyOfClass(
        unnamedParam(_.error)
      )
  }

  final case class IncorrectRollbackScopeOrder(error: String) extends Error {
    override def pretty: Pretty[IncorrectRollbackScopeOrder] = prettyOfClass(
      param("cause", _ => error.unquoted)
    )
  }

  final case class UsedBeforeCreation(contractIds: NonEmpty[Set[LfContractId]]) extends Error {
    override def pretty: Pretty[UsedBeforeCreation] = prettyOfClass(
      param("cause", _ => "Contract id used before creation".unquoted),
      param("contractIds", _.contractIds),
    )
  }

  final case class UsedAfterArchive(keys: NonEmpty[Set[LfContractId]]) extends Error {
    override def pretty: Pretty[UsedAfterArchive] = prettyOfClass(
      param("cause", _ => "Contract id used after archive".unquoted),
      param("contractIds", _.keys),
    )
  }

  final case class InconsistentKeyUse(keys: NonEmpty[Set[LfGlobalKey]]) extends Error {
    override def pretty: Pretty[InconsistentKeyUse] = prettyOfClass(
      param("cause", _ => "Inconsistent global key assumptions".unquoted),
      param("contractIds", _.keys),
    )
  }

  def alertingPartyLookup(
      hostedParties: Map[LfPartyId, Boolean]
  )(implicit loggingContext: ErrorLoggingContext): LfPartyId => Boolean = { party =>
    hostedParties.getOrElse(
      party, {
        loggingContext.error(
          s"Prefetch of global key parties is wrong and missed to load data for party $party"
        )
        false
      },
    )
  }

  def hostedGlobalKeyParties(
      rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]],
      participantId: ParticipantId,
      topologySnapshot: TopologySnapshot,
  )(implicit
      loggingContext: NamedLoggingContext,
      ec: ExecutionContext,
  ): Future[Map[LfPartyId, Boolean]] = {
    val parties =
      rootViewTrees.forgetNE.flatMap(_.view.globalKeyInputs.values.flatMap(_.maintainers)).toSet
    ExtractUsedAndCreated.fetchHostedParties(
      parties,
      participantId,
      topologySnapshot,
    )
  }

  private[validation] def checkRollbackScopeOrder(
      presented: Seq[RollbackContext]
  ): Either[String, Unit] = {
    Either.cond(
      presented == presented.sorted,
      (),
      s"Detected out of order rollback scopes in: $presented",
    )
  }

}
