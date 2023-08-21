// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package lf.verified
package translation

import stainless.lang._
import stainless.annotation._
import stainless.collection._
import utils.{
  Either,
  Map,
  Set,
  Value,
  GlobalKey,
  Transaction,
  Unreachable,
  Node,
  ContractKeyUniquenessMode,
  filterNot,
}
import utils.Value.ContractId
import utils.Transaction.{
  KeyCreate,
  KeyInputError,
  NegativeKeyLookup,
  InconsistentContractKey,
  DuplicateContractKey,
  KeyInput,
}

object ContractStateMachine {
  @dropVCs
  case class State[Nid] private[lf] (
      locallyCreated: Set[ContractId],
      globalKeyInputs: Map[GlobalKey, KeyInput],
      activeState: ContractStateMachine.ActiveLedgerState[Nid],
      rollbackStack: List[ContractStateMachine.ActiveLedgerState[Nid]],
      mode: ContractKeyUniquenessMode,
  ) {
    def consumedByOrInactive(cid: Value.ContractId): Option[Either[Nid, Unit]] = {
      activeState.consumedBy.get(cid) match {
        case Some(nid) => Some(Left(nid)) // consumed
        case None() =>
          if (locallyCreated(cid) && !activeState.locallyCreatedThisTimeline.contains(cid)) {
            Some(Right(())) // inactive
          } else {
            None() // neither
          }
      }
    }
    def lookupActiveKey(key: GlobalKey): Option[ContractStateMachine.KeyMapping] =
      activeState.getLocalActiveKey(key).orElse(lookupActiveGlobalKeyInput(key))
    def lookupActiveGlobalKeyInput(key: GlobalKey): Option[ContractStateMachine.KeyMapping] =
      globalKeyInputs.get(key).map {
        case Transaction.KeyActive(cid) if !activeState.consumedBy.contains(cid) =>
          Some[ContractId](cid)
        case _ => None[ContractId]()
      }
    def handleCreate(node: Node.Create): Either[KeyInputError, State[Nid]] =
      visitCreate(node.coid, node.gkeyOpt).left.map(Right(_))
    private[lf] def visitCreate(
        contractId: ContractId,
        mbKey: Option[GlobalKey],
    ): Either[DuplicateContractKey, State[Nid]] = {
      val me =
        this.copy(
          locallyCreated = locallyCreated + contractId,
          activeState = this.activeState
            .copy(locallyCreatedThisTimeline =
              this.activeState.locallyCreatedThisTimeline + contractId
            ),
        )
      mbKey match {
        case None() => Right(me)
        case Some(gk) =>
          val conflict = lookupActiveKey(gk).exists(_ != None[ContractId]())
          val newKeyInputs =
            if (globalKeyInputs.contains(gk)) globalKeyInputs
            else globalKeyInputs.updated(gk, KeyCreate)
          Either.cond(
            !conflict || mode == ContractKeyUniquenessMode.Off,
            me.copy(
              activeState = me.activeState.createKey(gk, contractId),
              globalKeyInputs = newKeyInputs,
            ),
            DuplicateContractKey(gk),
          )
      }
    }
    def handleExercise(nid: Nid, exe: Node.Exercise): Either[KeyInputError, State[Nid]] =
      visitExercise(
        nid,
        exe.targetCoid,
        exe.gkeyOpt,
        exe.byKey,
        exe.consuming,
      ).left
        .map(Left(_))
    private[lf] def visitExercise(
        nodeId: Nid,
        targetId: ContractId,
        mbKey: Option[GlobalKey],
        byKey: Boolean,
        consuming: Boolean,
    ): Either[InconsistentContractKey, State[Nid]] = {
      for {
        state <-
          if (byKey || mode == ContractKeyUniquenessMode.Strict)
            assertKeyMapping(targetId, mbKey)
          else
            Right(this)
      } yield {
        if (consuming) {
          val consumedState = state.activeState.consume(targetId, nodeId)
          state.copy(activeState = consumedState)
        } else state
      }
    }
    def handleLookup(lookup: Node.LookupByKey): Either[KeyInputError, State[Nid]] = {
      if (mode != ContractKeyUniquenessMode.Strict)
        Unreachable()
      visitLookup(lookup.gkey, lookup.result, lookup.result).left.map(Left(_))
    }
    def handleLookupWith(
        lookup: Node.LookupByKey,
        keyInput: Option[ContractId],
    ): Either[KeyInputError, State[Nid]] = {
      if (mode != ContractKeyUniquenessMode.Off)
        Unreachable()
      visitLookup(lookup.gkey, keyInput, lookup.result).left.map(Left(_))
    }
    private[lf] def visitLookup(
        gk: GlobalKey,
        keyInput: Option[ContractId],
        keyResolution: Option[ContractId],
    ): Either[InconsistentContractKey, State[Nid]] = {
      val (keyMapping, next) = resolveKey(gk) match {
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
        case None() =>
          def handleResult(result: Option[ContractId]): (KeyMapping, State[Nid]) = {
            val keyInput = result match {
              case None() => NegativeKeyLookup
              case Some(cid) => Transaction.KeyActive(cid)
            }
            val newKeyInputs = globalKeyInputs.updated(gkey, keyInput)
            val state = this.copy(globalKeyInputs = newKeyInputs)
            result match {
              case Some(cid) if !activeState.consumedBy.contains(cid) =>
                Some[ContractId](cid) -> state
              case _ =>
                None[ContractId]() -> state
            }
          }
          Left(handleResult)
      }
    }
    def handleFetch(node: Node.Fetch): Either[KeyInputError, State[Nid]] =
      visitFetch(node.coid, node.gkeyOpt, node.byKey).left.map(Left(_))
    private[lf] def visitFetch(
        contractId: ContractId,
        mbKey: Option[GlobalKey],
        byKey: Boolean,
    ): Either[InconsistentContractKey, State[Nid]] =
      if (byKey || mode == ContractKeyUniquenessMode.Strict)
        assertKeyMapping(contractId, mbKey)
      else
        Right(this)
    private[lf] def assertKeyMapping(
        cid: Value.ContractId,
        mbKey: Option[GlobalKey],
    ): Either[InconsistentContractKey, State[Nid]] =
      mbKey match {
        case None() => Right(this)
        case Some(gk) =>
          val (keyMapping, next) = resolveKey(gk) match {
            case Right(result) => result
            case Left(handle) => handle(Some(cid))
          }
          Either.cond(keyMapping == Some[ContractId](cid), next, InconsistentContractKey(gk))
      }
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
    def beginRollback(): State[Nid] =
      this.copy(rollbackStack = Cons(activeState, rollbackStack))
    def endRollback(): State[Nid] = rollbackStack match {
      case Nil() => Unreachable()
      case Cons(headState, tailStack) =>
        this.copy(activeState = headState, rollbackStack = tailStack)
    }
    def dropRollback(): State[Nid] = rollbackStack match {
      case Nil() => Unreachable()
      case Cons(_, tailStack) => this.copy(rollbackStack = tailStack)
    }
    private[lf] def withinRollbackScope: Boolean = rollbackStack.nonEmpty
    def advance(resolver: KeyResolver, substate: State[Nid]): Either[KeyInputError, State[Nid]] = {
      require(
        !substate.withinRollbackScope,
        "Cannot lift a state over a substate with unfinished rollback scopes",
      )
      def consistentGlobalKeyInputs: Either[KeyInputError, Unit] =
        substate.globalKeyInputs
          .find {
            case (key, KeyCreate) =>
              lookupActiveKey(key).exists(
                _ != None[ContractId]()
              ) && mode == ContractKeyUniquenessMode.Strict
            case (key, NegativeKeyLookup) => lookupActiveKey(key).exists(_ != None[ContractId]())
            case (key, Transaction.KeyActive(cid)) =>
              lookupActiveKey(key).exists(_ != Some[ContractId](cid))
            case _ => false
          } match {
          case Some((key, KeyCreate)) => Left[KeyInputError, Unit](Right(DuplicateContractKey(key)))
          case Some((key, NegativeKeyLookup)) =>
            Left[KeyInputError, Unit](Left(InconsistentContractKey(key)))
          case Some((key, Transaction.KeyActive(_))) =>
            Left[KeyInputError, Unit](Left(InconsistentContractKey(key)))
          case _ => Right[KeyInputError, Unit](())
        }
      for {
        _ <- consistentGlobalKeyInputs
      } yield {
        val next = this.activeState.advance(substate.activeState)
        val globalKeyInputs =
          if (mode == ContractKeyUniquenessMode.Strict)
            substate.globalKeyInputs ++ this.globalKeyInputs
          else
            substate.globalKeyInputs.foldLeft(this.globalKeyInputs) { case (acc, (key, keyInput)) =>
              if (acc.contains(key)) acc
              else {
                val resolution = keyInput match {
                  case KeyCreate =>
                    KeyCreate
                  case NegativeKeyLookup =>
                    resolver(key) match {
                      case None() => keyInput
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
          globalKeyInputs = globalKeyInputs,
          activeState = next,
        )
      }
    }
    def projectKeyResolver(resolver: KeyResolver): KeyResolver = {
      val consumed = activeState.consumedBy.keySet
      resolver.map { case (key, keyMapping) =>
        val newKeyInput = activeState.getLocalActiveKey(key) match {
          case None() => keyMapping.filterNot(consumed.contains)
          case Some(localMapping) => localMapping
        }
        key -> newKeyInput
      }
    }
  }
  object State {
    def empty[Nid](mode: ContractKeyUniquenessMode): State[Nid] = new State(
      Set.empty,
      Map.empty,
      ContractStateMachine.ActiveLedgerState.empty,
      List.empty,
      mode,
    )
  }
  def initial[Nid](mode: ContractKeyUniquenessMode): State[Nid] = State.empty(mode)
  type KeyResolver = Map[GlobalKey, KeyMapping]
  type KeyMapping = Option[Value.ContractId]
  final case class ActiveLedgerState[Nid](
      locallyCreatedThisTimeline: Set[ContractId],
      consumedBy: Map[ContractId, Nid],
      private[lf] val localKeys: Map[GlobalKey, Value.ContractId],
  ) {
    def consume(contractId: ContractId, nodeId: Nid): ActiveLedgerState[Nid] =
      this.copy(consumedBy = consumedBy.updated(contractId, nodeId))
    def createKey(key: GlobalKey, cid: Value.ContractId): ActiveLedgerState[Nid] =
      this.copy(localKeys = localKeys.updated(key, cid))
    def isEquivalent(other: ActiveLedgerState[Nid]): Boolean =
      this.locallyCreatedThisTimeline == other.locallyCreatedThisTimeline &&
        this.consumedBy == other.consumedBy &&
        this.localActiveKeys == other.localActiveKeys
    private[lf] def advance(
        substate: ActiveLedgerState[Nid]
    ): ActiveLedgerState[Nid] =
      ActiveLedgerState(
        locallyCreatedThisTimeline = this.locallyCreatedThisTimeline
          .union(substate.locallyCreatedThisTimeline),
        consumedBy = this.consumedBy ++ substate.consumedBy,
        localKeys = this.localKeys.concat(substate.localKeys),
      )
    def localActiveKeys: Map[GlobalKey, KeyMapping] =
      localKeys.view
        .mapValues((v: ContractId) =>
          if (consumedBy.contains(v)) None[ContractId]()
          else Some[ContractId](v)
        )
        .toMap
    def getLocalActiveKey(key: GlobalKey): Option[KeyMapping] =
      localKeys.get(key) match {
        case None() => None()
        case Some(cid) =>
          Some(if (consumedBy.contains(cid)) None[ContractId]() else Some[ContractId](cid))
      }
  }
  object ActiveLedgerState {
    def empty[Nid]: ActiveLedgerState[Nid] = ActiveLedgerState(Set.empty, Map.empty, Map.empty)
  }
}

