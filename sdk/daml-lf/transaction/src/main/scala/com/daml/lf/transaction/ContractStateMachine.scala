// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.transaction

import com.daml.lf.transaction.Transaction.{KeyCreate, KeyInput, NegativeKeyLookup}
import com.daml.lf.transaction.TransactionErrors.{
  CreateError,
  DuplicateContractId,
  DuplicateContractKey,
  InconsistentContractKey,
  KeyInputError,
}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId

/** Implements a state machine for contracts and their keys while interpreting a Daml-LF command
  * or iterating over a [[com.daml.lf.transaction.HasTxNodes]] in execution order.
  * The contract state machine keeps track of the updates to the [[ContractStateMachine.ActiveLedgerState]]
  * since the beginning of the interpretation or iteration.
  * Given a [[ContractStateMachine.State]] `s`, a client can compute the next state for a given action node `n`,
  * by calling `s.handleNode(..., n, ...)`.
  * For a rollback node `nr`, a client must call `beginRollback` before processing the first child of `nr` and
  * `endRollback` after processing the last child of `nr`.
  *
  * @tparam Nid Type parameter for [[com.daml.lf.transaction.NodeId]]s during interpretation.
  *             Use [[scala.Unit]] for iteration.
  *
  * @see com.daml.lf.transaction.HasTxNodes.contractKeyInputs for an iteration in mode
  *      [[com.daml.lf.transaction.ContractKeyUniquenessMode.Strict]] and
  * @see ContractStateMachineSpec.visitSubtree for iteration in all modes
  */
object ContractStateMachine {

  /** @param locallyCreated
    *   Tracks all contracts created by a node processed so far (including nodes under a rollback).
    *
    * @param globalKeyInputs
    *   Contains the key mappings required by Daml Engine to get to the current state
    *   (including [[Transaction.KeyCreate]] for create nodes).
    *   That is, `globalKeyInputs` contains the answers to all [[engine.ResultNeedKey]] requests that Daml Engine would
    *   emit while it is building the nodes passed to this contract state machine as input.
    *
    *   The map `globalKeyInputs` grows monotonically. Its entries are never overwritten and not reset after a
    *   rollback scope is left.
    *   Formally, if a contract state machine transitions from state `s1` to state `s2`,
    *   then `s1.globalKeyInputs` is a subset of `s2.globalKeyInputs`.
    *
    *   The map `globalKeyInputs` can be used to resolve keys during re-interpretation.
    *
    *   In mode [[com.daml.lf.transaction.ContractKeyUniquenessMode.Strict]],
    *   `globalKeyInputs` stores the contract key states required at the beginning of the transaction.
    *   The first node involving a key determines the state of the key in `globalKeyInputs`.
    *
    *   In mode [[com.daml.lf.transaction.ContractKeyUniquenessMode.Off]],
    *   `globalKeyInputs(k)` is defined by the first node `n` referring to the key `k`:
    *   - If `n` is a fetch-by-key or exercise-by-key, then `globalKeyInputs(k)` is [[Transaction.KeyActive]].
    *   - If `n` is lookup-by-key, then `globalKeyInputs(k)` is [[Transaction.KeyActive]] (positive lookup)
    *     or [[Transaction.NegativeKeyLookup]] (negative lookup).
    *   - If `n` is a create, then `globalKeyInputs(k)` is [[Transaction.KeyCreate]].
    *   Note: a plain fetch or exercise (with `byKey == false`) does not impact `globalKeyInputs`.
    *
    *  @param activeState
    *    Summarizes the active state of the partial transaction that was visited so far.
    *    When a rollback scope is left, this is restored to the state at the beginning of the rollback,
    *    which is cached in `rollbackStack`.
    *
    * @param rollbackStack
    *   The stack of active states at the beginning of the currently active try blocks (interpretation) or
    *   Rollback nodes (iteration).
    *
    * Invariants:
    * - [[globalKeyInputs]]'s keyset is a superset of [[activeState]].[[ActiveLedgerState.keys]]'s keyset
    *   and of all the [[ActiveLedgerState.keys]]'s keysets in [[rollbackStack]].
    *   (the superset can be strict in case of by-key nodes inside an already completed Rollback scope)
    * - In mode [[ContractKeyUniquenessMode.Strict]],
    *   the keys belonging to the contracts in [[activeState]].[[ActiveLedgerState.consumedBy]]'s
    *   keyset are in [[activeState]].[[ActiveLedgerState.keys]],
    *   and similarly for all [[ActiveLedgerState]]s in [[rollbackStack]].
    */
  case class State[Nid] private[lf] (
      locallyCreated: Set[ContractId],
      inputContractIds: Set[ContractId],
      globalKeyInputs: Map[GlobalKey, KeyInput],
      activeState: ContractStateMachine.ActiveLedgerState[Nid],
      rollbackStack: List[ContractStateMachine.ActiveLedgerState[Nid]],
      mode: ContractKeyUniquenessMode,
  ) {

    /** The return value indicates if the given contract is either consumed, inactive, or otherwise
      * - Some(Left(nid)) -- consumed by a specified node-id
      * - Some(Right(())) -- inactive, because the (local) contract creation has been rolled-back
      * - None -- neither consumed nor inactive
      */
    def consumedByOrInactive(cid: Value.ContractId): Option[Either[Nid, Unit]] = {
      activeState.consumedBy.get(cid) match {
        case Some(nid) => Some(Left(nid)) // consumed
        case None =>
          if (locallyCreated(cid) && !activeState.locallyCreatedThisTimeline.contains(cid)) {
            Some(Right(())) // inactive
          } else {
            None // neither
          }
      }
    }

    /** Lookup the given key k. Returns
      * - Some(KeyActive(cid)) if k maps to cid and cid is active.
      * - Some(KeyInactive) if there is no active contract with the given key.
      * - None if we know no mapping for that key.
      */
    def lookupActiveKey(key: GlobalKey): Option[ContractStateMachine.KeyMapping] =
      activeState.getLocalActiveKey(key).orElse(lookupActiveGlobalKeyInput(key))

    def lookupActiveGlobalKeyInput(key: GlobalKey): Option[ContractStateMachine.KeyMapping] =
      globalKeyInputs.get(key).map {
        case Transaction.KeyActive(cid) if !activeState.consumedBy.contains(cid) =>
          ContractStateMachine.KeyActive(cid)
        case _ => ContractStateMachine.KeyInactive
      }

    /** Visit a create node */
    def handleCreate(node: Node.Create): Either[KeyInputError, State[Nid]] =
      visitCreate(node.coid, node.gkeyOpt).left.map(KeyInputError.from)

    private[lf] def visitCreate(
        contractId: ContractId,
        mbKey: Option[GlobalKey],
    ): Either[CreateError, State[Nid]] = {
      if (locallyCreated.union(inputContractIds).contains(contractId)) {
        Left(CreateError.inject(DuplicateContractId(contractId)))
      } else {
        val me =
          this.copy(
            locallyCreated = locallyCreated + contractId,
            activeState = this.activeState
              .copy(locallyCreatedThisTimeline =
                this.activeState.locallyCreatedThisTimeline + contractId
              ),
          )
        // if we have a contract key being added, include it in the list of
        // active keys
        mbKey match {
          case None => Right(me)
          case Some(gk) =>
            val conflict = lookupActiveKey(gk).exists(_ != KeyInactive)
            val newKeyInputs =
              if (globalKeyInputs.contains(gk)) globalKeyInputs
              else globalKeyInputs.updated(gk, KeyCreate)
            Either.cond(
              !conflict || mode == ContractKeyUniquenessMode.Off,
              me.copy(
                activeState = me.activeState.createKey(gk, contractId),
                globalKeyInputs = newKeyInputs,
              ),
              CreateError.inject(DuplicateContractKey(gk)),
            )
        }
      }
    }

    def handleExercise(nid: Nid, exe: Node.Exercise): Either[KeyInputError, State[Nid]] =
      visitExercise(
        nid,
        exe.targetCoid,
        exe.gkeyOpt,
        exe.byKey,
        exe.consuming,
      ).left.map(KeyInputError.inject)

    /** Omits the key lookup that are done in [[com.daml.lf.speedy.Compiler.compileChoiceByKey]] for by-bey nodes,
      * which translates to a [[resolveKey]] below.
      * Use [[handleExercise]] when visiting an exercise node during iteration.
      */
    private[lf] def visitExercise(
        nodeId: Nid,
        targetId: ContractId,
        mbKey: Option[GlobalKey],
        byKey: Boolean,
        consuming: Boolean,
    ): Either[InconsistentContractKey, State[Nid]] = {
      val state = witnessContractId(targetId)
      for {
        state <-
          if (byKey || mode == ContractKeyUniquenessMode.Strict)
            state.assertKeyMapping(targetId, mbKey)
          else
            Right(state)
      } yield {
        if (consuming) {
          val consumedState = state.activeState.consume(targetId, nodeId)
          state.copy(activeState = consumedState)
        } else state
      }
    }

    /** Must be used to handle lookups iff in [[com.daml.lf.transaction.ContractKeyUniquenessMode.Strict]] mode
      */
    def handleLookup(lookup: Node.LookupByKey): Either[KeyInputError, State[Nid]] = {
      // If the key has not yet been resolved, we use the resolution from the lookup node,
      // but this only makes sense if `activeState.keys` is updated by every node and not only by by-key nodes.
      if (mode != ContractKeyUniquenessMode.Strict)
        throw new UnsupportedOperationException(
          "handleLookup can only be used if all key nodes are considered"
        )
      visitLookup(lookup.gkey, lookup.result, lookup.result).left.map(KeyInputError.inject)
    }

    /** Must be used to handle lookups iff in [[com.daml.lf.transaction.ContractKeyUniquenessMode.Off]] mode
      * The second argument takes the contract key resolution to be given to the Daml interpreter instead of
      * [[com.daml.lf.transaction.Node.LookupByKey.result]].
      * This is because the iteration might currently be within a rollback scope
      * that has already archived a contract with the key without a by-key operation
      * (and for this reason the archival is not tracked in [[ContractStateMachine.ActiveLedgerState.keys]]),
      * i.e., the lookup resolves to [[scala.None$]] but the correct key input is [[scala.Some$]] for some contract ID
      * and this may matter after the rollback scope is left.
      * Daml Engine will ask the ledger in that case to resolve the lookup and then perform an activeness check
      * against the result potentially turning it into a negative lookup.
      */
    def handleLookupWith(
        lookup: Node.LookupByKey,
        keyInput: Option[ContractId],
    ): Either[KeyInputError, State[Nid]] = {
      if (mode != ContractKeyUniquenessMode.Off)
        throw new UnsupportedOperationException(
          "handleLookupWith can only be used if only by-key nodes are considered"
        )
      visitLookup(lookup.gkey, keyInput, lookup.result).left.map(KeyInputError.inject)
    }

    private[lf] def visitLookup(
        gk: GlobalKey,
        keyInput: Option[ContractId],
        keyResolution: Option[ContractId],
    ): Either[InconsistentContractKey, State[Nid]] = {
      val state = keyInput match {
        case Some(contractId) => witnessContractId(contractId)
        case None => this
      }
      val (keyMapping, next) = state.resolveKey(gk) match {
        case Right(result) => result
        case Left(handle) => handle(keyInput)
      }
      Either.cond(
        keyMapping == keyResolution,
        next,
        InconsistentContractKey(gk),
      )
    }

    private[lf] def resolveKey(
        gkey: GlobalKey
    ): Either[Option[ContractId] => (KeyMapping, State[Nid]), (KeyMapping, State[Nid])] = {
      lookupActiveKey(gkey) match {
        case Some(keyMapping) => Right(keyMapping -> this)
        case None =>
          // if we cannot find it here, send help, and make sure to update keys after
          // that.
          def handleResult(result: Option[ContractId]): (KeyMapping, State[Nid]) = {
            // Update key inputs. Create nodes never call this method,
            // so NegativeKeyLookup is the right choice for the global key input.
            val keyInput = result match {
              case None => NegativeKeyLookup
              case Some(cid) => Transaction.KeyActive(cid)
            }
            val newKeyInputs = globalKeyInputs.updated(gkey, keyInput)
            val state = this.copy(globalKeyInputs = newKeyInputs)
            result match {
              case Some(cid) if !activeState.consumedBy.contains(cid) =>
                KeyActive(cid) -> state
              case _ =>
                KeyInactive -> state
            }
          }
          Left(handleResult)
      }
    }

    def handleFetch(node: Node.Fetch): Either[KeyInputError, State[Nid]] =
      visitFetch(node.coid, node.gkeyOpt, node.byKey).left.map(KeyInputError.inject)

    private[lf] def visitFetch(
        contractId: ContractId,
        mbKey: Option[GlobalKey],
        byKey: Boolean,
    ): Either[InconsistentContractKey, State[Nid]] = {
      val state = witnessContractId(contractId)
      if (byKey || mode == ContractKeyUniquenessMode.Strict)
        state.assertKeyMapping(contractId, mbKey)
      else
        Right(state)
    }

    private[lf] def witnessContractId(contractId: ContractId): State[Nid] =
      if (locallyCreated.contains(contractId)) this
      else this.copy(inputContractIds = inputContractIds + contractId)

    private[lf] def assertKeyMapping(
        cid: Value.ContractId,
        mbKey: Option[GlobalKey],
    ): Either[InconsistentContractKey, State[Nid]] =
      mbKey match {
        case None => Right(this)
        case Some(gk) =>
          val (keyMapping, next) = resolveKey(gk) match {
            case Right(result) => result
            case Left(handle) => handle(Some(cid))
          }
          // Since keys is defined only where keyInputs is defined, we don't need to update keyInputs.
          Either.cond(keyMapping == KeyActive(cid), next, InconsistentContractKey(gk))
      }

    /** Utility method that takes a node and computes the corresponding next state.
      * The method does not handle any children of `node`; it is up to the caller to do that.
      * @param keyInput will only be used in mode [[ContractKeyUniquenessMode.Off]] and if the node is a lookupByKey
      */
    def handleNode(
        id: Nid,
        node: Node.Action,
        keyInput: => Option[ContractId],
    ): Either[KeyInputError, State[Nid]] = node match {
      case create: Node.Create => handleCreate(create)
      case fetch: Node.Fetch => handleFetch(fetch)
      case lookup: Node.LookupByKey =>
        mode match {
          case ContractKeyUniquenessMode.Strict => handleLookup(lookup)
          case ContractKeyUniquenessMode.Off => handleLookupWith(lookup, keyInput)
        }

      case exercise: Node.Exercise => handleExercise(id, exercise)
    }

    /** To be called when interpretation enters a try block or iteration enters a Rollback node
      * Must be matched by [[endRollback]] or [[dropRollback]].
      */
    def beginRollback(): State[Nid] =
      this.copy(rollbackStack = activeState :: rollbackStack)

    /** To be called when interpretation does insert a Rollback node or iteration leaves a Rollback node.
      * Must be matched by a [[beginRollback]].
      */
    def endRollback(): State[Nid] = rollbackStack match {
      case Nil => throw new IllegalStateException("Not inside a rollback scope")
      case headState :: tailStack => this.copy(activeState = headState, rollbackStack = tailStack)
    }

    /** To be called if interpretation notices that a try block did not lead to a Rollback node
      * Must be matched by a [[beginRollback]].
      */
    def dropRollback(): State[Nid] = rollbackStack match {
      case Nil => throw new IllegalStateException("Not inside a rollback scope")
      case _ :: tailStack => this.copy(rollbackStack = tailStack)
    }

    private[lf] def withinRollbackScope: Boolean = rollbackStack.nonEmpty

    /** Let `this` state be the result of iterating over a transaction `tx` until just before a node `n`.
      * Let `substate` be the state obtained after fully iterating over the subtree rooted at `n`
      * starting from [[State.empty]].
      * Then, `advance(resolver, substate)` equals the state resulting from iterating over `tx` until
      * processing all descendants of `n`.
      *
      * The call to `advance(resolver, substate)` fails if and only if
      * the iteration over the subtree rooted at `n` starting from `this` fails, but the error may be different.
      *
      * @param resolver
      *   In mode [[ContractKeyUniquenessMode.Strict]], this parameter has no effect.
      *   In mode [[ContractKeyUniquenessMode.Off]], `resolver` must be the resolver used while iterating over `tx`
      *   until just before `n`.
      *   While iterating over the subtree rooted at `n`, [[projectKeyResolver]](`resolver`) must be used as resolver.
      *
      * @param substate
      *   The state obtained after fully iterating over the subtree `n` starting from [[State.empty]].
      *   Consumed contracts ([[activeState.consumedBy]]) in `this` and `substate` must be disjoint.
      *
      * @see com.daml.lf.transaction.HasTxNodes.contractKeyInputs for an iteration in mode
      *      [[com.daml.lf.transaction.ContractKeyUniquenessMode.Strict]] and
      * @see ContractStateMachineSpec.visitSubtree for iteration in all modes
      */
    def advance(resolver: KeyResolver, substate: State[Nid]): Either[KeyInputError, State[Nid]] = {
      require(
        !substate.withinRollbackScope,
        "Cannot lift a state over a substate with unfinished rollback scopes",
      )

      // We want consistent key lookups within an action in any contract key mode.
      def consistentGlobalKeyInputs: Either[KeyInputError, Unit] = {
        substate.locallyCreated.find(locallyCreated.union(inputContractIds).contains) match {
          case Some(contractId) =>
            Left[KeyInputError, Unit](KeyInputError.inject(DuplicateContractId(contractId)))
          case None =>
            substate.globalKeyInputs
              .find {
                case (key, KeyCreate) =>
                  lookupActiveKey(key).exists(
                    _ != KeyInactive
                  ) && mode == ContractKeyUniquenessMode.Strict
                case (key, NegativeKeyLookup) => lookupActiveKey(key).exists(_ != KeyInactive)
                case (key, Transaction.KeyActive(cid)) =>
                  lookupActiveKey(key).exists(_ != KeyActive(cid))
                case _ => false
              } match {
              case Some((key, KeyCreate)) =>
                Left[KeyInputError, Unit](KeyInputError.inject(DuplicateContractKey(key)))
              case Some((key, NegativeKeyLookup)) =>
                Left[KeyInputError, Unit](KeyInputError.inject(InconsistentContractKey(key)))
              case Some((key, Transaction.KeyActive(_))) =>
                Left[KeyInputError, Unit](KeyInputError.inject(InconsistentContractKey(key)))
              case _ => Right[KeyInputError, Unit](())
            }
        }
      }

      for {
        _ <- consistentGlobalKeyInputs
      } yield {
        val next = this.activeState.advance(substate.activeState)
        val globalKeyInputs =
          if (mode == ContractKeyUniquenessMode.Strict)
            // In strict mode, `key`'s state is the same at `this` as at the beginning
            // if `key` is not in `this.globalKeyInputs`.
            // So just extend `this.globalKeyInputs` with the new stuff.
            substate.globalKeyInputs ++ this.globalKeyInputs
          else
            substate.globalKeyInputs.foldLeft(this.globalKeyInputs) { case (acc, (key, keyInput)) =>
              if (acc.contains(key)) acc
              else {
                val resolution = keyInput match {
                  case KeyCreate =>
                    // A create brought the contract key in scope without querying a resolver.
                    // So the global key input for `key` does not depend on the resolver.
                    KeyCreate
                  case NegativeKeyLookup =>
                    // A lookup-by-key brought the key in scope. Use the resolver's resolution instead
                    // as the projected resolver's resolution might have been mapped to None.
                    resolver(key) match {
                      case None => keyInput
                      case Some(cid) => Transaction.KeyActive(cid)
                    }
                  case active: Transaction.KeyActive =>
                    active
                }
                acc.updated(key, resolution)
              }
            }

        this.copy(
          locallyCreated = this.locallyCreated.union(substate.locallyCreated),
          inputContractIds =
            this.inputContractIds.union(substate.inputContractIds.diff(this.locallyCreated)),
          globalKeyInputs = globalKeyInputs,
          activeState = next,
        )
      }
    }

    /** @see advance */
    def projectKeyResolver(resolver: KeyResolver): KeyResolver = {
      val consumed = activeState.consumedBy.keySet
      resolver.map { case (key, keyMapping) =>
        val newKeyInput = activeState.getLocalActiveKey(key) match {
          case None => keyMapping.filterNot(consumed.contains)
          case Some(localMapping) => localMapping
        }
        key -> newKeyInput
      }
    }
  }

  object State {
    def empty[Nid](mode: ContractKeyUniquenessMode): State[Nid] = new State(
      Set.empty,
      Set.empty,
      Map.empty,
      ContractStateMachine.ActiveLedgerState.empty,
      List.empty,
      mode,
    )
  }

  def initial[Nid](mode: ContractKeyUniquenessMode): State[Nid] = State.empty(mode)

  /** Represents the answers for [[com.daml.lf.engine.ResultNeedKey]] requests
    * that may arise during Daml interpretation.
    */
  type KeyResolver = Map[GlobalKey, KeyMapping]

  type KeyMapping = Option[Value.ContractId]

  /** There is no active contract with the given key. */
  val KeyInactive = None

  /** The contract with the given cid is active and has the given key. */
  val KeyActive = Some

  /** Summarizes the updates to the current ledger state by nodes up to now.
    *
    * @param locallyCreatedThisTimeline
    *   Tracks contracts created by a node processed so far that have not been rolled back.
    *   This is a subset of [[ContractStateMachine.State.locallyCreated]].
    *
    * @param consumedBy [[com.daml.lf.value.Value.ContractId]]s of all contracts
    *                   that have been consumed by nodes up to now.
    * @param localKeys
    *   A store of the latest local contract that has been created with the given key in this timeline.
    *   Later creates overwrite earlier ones. Note that this does not track whether the contract
    *   was consumed or not. That information is stored in consumedBy.
    *   It also _only_ includes local contracts not global contracts.
    */
  final case class ActiveLedgerState[Nid](
      locallyCreatedThisTimeline: Set[ContractId],
      consumedBy: Map[ContractId, Nid],
      private[lf] val localKeys: Map[GlobalKey, Value.ContractId],
  ) {
    def consume(contractId: ContractId, nodeId: Nid): ActiveLedgerState[Nid] =
      this.copy(consumedBy = consumedBy.updated(contractId, nodeId))

    def createKey(key: GlobalKey, cid: Value.ContractId): ActiveLedgerState[Nid] =
      this.copy(localKeys = localKeys.updated(key, cid))

    /** Equivalence relative to locallyCreatedThisTimeline, consumedBy & localActiveKeys.
      */
    def isEquivalent(other: ActiveLedgerState[Nid]): Boolean =
      this.locallyCreatedThisTimeline == other.locallyCreatedThisTimeline &&
        this.consumedBy == other.consumedBy &&
        this.localActiveKeys == other.localActiveKeys

    /** See docs of [[ContractStateMachine.advance]]
      */
    private[lf] def advance(
        substate: ActiveLedgerState[Nid]
    ): ActiveLedgerState[Nid] =
      ActiveLedgerState(
        locallyCreatedThisTimeline = this.locallyCreatedThisTimeline
          .union(substate.locallyCreatedThisTimeline),
        consumedBy = this.consumedBy ++ substate.consumedBy,
        localKeys = this.localKeys.concat(substate.localKeys),
      )

    /** localKeys filter by whether contracts have been consumed already.
      */
    def localActiveKeys: Map[GlobalKey, KeyMapping] =
      localKeys.view
        .mapValues((v: ContractId) => if (consumedBy.contains(v)) KeyInactive else KeyActive(v))
        .toMap

    /** Lookup in localActiveKeys.
      */
    def getLocalActiveKey(key: GlobalKey): Option[KeyMapping] =
      localKeys.get(key) match {
        case None => None
        case Some(cid) =>
          Some(if (consumedBy.contains(cid)) KeyInactive else KeyActive(cid))
      }
  }

  object ActiveLedgerState {
    def empty[Nid]: ActiveLedgerState[Nid] = ActiveLedgerState(Set.empty, Map.empty, Map.empty)
  }

}
