// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.ledgerinteraction

import akka.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.PartyDetails
import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.scenario.ScenarioLedger
import com.daml.lf.scenario.ScenarioLedger.RichTransaction
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.Speedy.{Machine, OffLedger, OnLedger}
import com.daml.lf.speedy.{PartialTransaction, SValue, ScenarioRunner, TraceLog}
import com.daml.lf.transaction.Node.{
  NodeRollback,
  NodeCreate,
  NodeExercises,
  NodeFetch,
  NodeLookupByKey,
}
import com.daml.lf.transaction.{GlobalKey, NodeId}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.lf._
import com.daml.scalautil.Statement.discard
import com.daml.script.converter.ConverterException
import io.grpc.StatusRuntimeException
import scalaz.OneAnd
import scalaz.OneAnd._
import scalaz.std.set._
import scalaz.syntax.foldable._

import scala.collection.compat.immutable.LazyList
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

// Client for the script service.
class IdeLedgerClient(val compiledPackages: CompiledPackages) extends ScriptLedgerClient {
  class ArrayBufferTraceLog extends TraceLog {
    val buffer = ArrayBuffer[(String, Option[Location])]()
    override def add(message: String, optLocation: Option[Location]): Unit = {
      discard { buffer.append((message, optLocation)) }
    }
    override def iterator: Iterator[(String, Option[Location])] = {
      buffer.iterator
    }
    def clear: Unit = buffer.clear()
  }

  val traceLog = new ArrayBufferTraceLog()

  private[this] val preprocessor = new engine.preprocessing.CommandPreprocessor(compiledPackages)

  private val txSeeding =
    speedy.InitialSeeding.TransactionSeed(crypto.Hash.hashPrivateKey(s"script-service"))

  // Machine for scenario expressions.
  val machine = Machine(
    compiledPackages,
    submissionTime = Time.Timestamp.Epoch,
    initialSeeding = txSeeding,
    expr = null,
    globalCids = Set.empty,
    committers = Set.empty,
    traceLog = traceLog,
  )
  val onLedger = machine.ledgerMode match {
    case OffLedger => throw SRequiresOnLedger("ScenarioRunner")
    case onLedger: OnLedger => onLedger
  }
  val scenarioRunner = ScenarioRunner(machine)
  private var allocatedParties: Map[String, PartyDetails] = Map()

  override def query(parties: OneAnd[Set, Ref.Party], templateId: Identifier)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Seq[ScriptLedgerClient.ActiveContract]] = {
    val acs = scenarioRunner.ledger.query(
      view = ScenarioLedger.ParticipantView(Set(), Set(parties.toList: _*)),
      effectiveAt = scenarioRunner.ledger.currentTime,
    )
    val filtered = acs.collect {
      case ScenarioLedger.LookupOk(cid, Value.ContractInst(tpl, arg, _), stakeholders)
          if tpl == templateId && parties.any(stakeholders.contains(_)) =>
        (cid, arg)
    }
    Future.successful(filtered.map { case (cid, c) =>
      ScriptLedgerClient.ActiveContract(templateId, cid, c.value)
    })
  }

  override def queryContractId(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
      cid: ContractId,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Option[ScriptLedgerClient.ActiveContract]] = {
    scenarioRunner.ledger.lookupGlobalContract(
      view = ScenarioLedger.ParticipantView(Set(), Set(parties.toList: _*)),
      effectiveAt = scenarioRunner.ledger.currentTime,
      cid,
    ) match {
      case ScenarioLedger.LookupOk(_, Value.ContractInst(_, arg, _), stakeholders)
          if parties.any(stakeholders.contains(_)) =>
        Future.successful(Some(ScriptLedgerClient.ActiveContract(templateId, cid, arg.value)))
      case _ =>
        // Note that contrary to `fetch` in a scenario, we do not
        // abort on any of the error cases. This makes sense if you
        // consider this a wrapper around the ACS endpoint where
        // we cannot differentiate between visibility errors
        // and the contract not being active.
        Future.successful(None)
    }
  }

  override def queryContractKey(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
      key: SValue,
      translateKey: (Identifier, Value[ContractId]) => Either[String, SValue],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Option[ScriptLedgerClient.ActiveContract]] = {
    GlobalKey
      .build(templateId, key.toValue)
      .fold(err => Future.failed(new ConverterException(err)), Future.successful(_))
      .flatMap { gkey =>
        scenarioRunner.ledger.ledgerData.activeKeys.get(gkey) match {
          case None => Future.successful(None)
          case Some(cid) => queryContractId(parties, templateId, cid)
        }
      }
  }

  // unsafe version of submit that does not clear the commit.
  private def unsafeSubmit(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit ec: ExecutionContext): Future[
    Either[StatusRuntimeException, RichTransaction]
  ] =
    Future {
      // Clear state at the beginning like in SBSBeginCommit for scenarios.
      machine.returnValue = null
      onLedger.commitLocation = optLocation
      onLedger.globalDiscriminators = Set.empty
      onLedger.cachedContracts = Map.empty
      val speedyCommands = preprocessor.unsafePreprocessCommands(commands.to(ImmArray))._1
      val translated = compiledPackages.compiler.unsafeCompile(speedyCommands)
      machine.setExpressionToEvaluate(SEApp(translated, Array(SEValue.Token)))
      onLedger.committers = actAs.toSet
      var result: RichTransaction = null
      while (result == null) {
        machine.run() match {
          case SResultNeedContract(coid, tid @ _, committers @ _, cbMissing, cbPresent) =>
            scenarioRunner.lookupContract(coid, actAs.toSet, readAs, cbMissing, cbPresent).toTry.get
          case SResultNeedKey(keyWithMaintainers, committers @ _, cb) =>
            scenarioRunner
              .lookupKey(keyWithMaintainers.globalKey, actAs.toSet, readAs, cb)
              .toTry
              .get
          case SResultNeedLocalKeyVisible(stakeholders, committers @ _, cb) =>
            val visible = SVisibleByKey.fromSubmitters(actAs.toSet, readAs)(stakeholders)
            cb(visible)
          case SResultFinalValue(SUnit) =>
            onLedger.ptx.finish match {
              case PartialTransaction.CompleteTransaction(tx) =>
                ScenarioLedger.commitTransaction(
                  actAs = actAs.toSet,
                  readAs = readAs,
                  effectiveAt = scenarioRunner.ledger.currentTime,
                  optLocation = onLedger.commitLocation,
                  tx = tx,
                  l = scenarioRunner.ledger,
                ) match {
                  case Left(fas) =>
                    // Capture the error and exit.
                    throw ScenarioErrorCommitError(fas)
                  case Right(commitResult) =>
                    scenarioRunner.ledger = commitResult.newLedger
                    // Capture the result and exit.
                    result = commitResult.richTransaction
                }
              case PartialTransaction.IncompleteTransaction(ptx) =>
                throw new RuntimeException(s"Unexpected abort: $ptx")
            }
          case SResultFinalValue(v) =>
            // The final result should always be unit.
            throw new RuntimeException(s"FATAL: Unexpected non-unit final result: $v")
          case SResultScenarioCommit(_, _, _, _) =>
            throw new RuntimeException("FATAL: Encountered scenario commit in Daml Script")
          case SResultError(err) =>
            // Capture the error and exit.
            throw err
          case SResultNeedTime(callback) =>
            callback(scenarioRunner.ledger.currentTime)
          case SResultNeedPackage(pkg, callback @ _) =>
            throw new RuntimeException(
              s"FATAL: Missing package $pkg should have been reported at Script compilation"
            )
          case SResultScenarioInsertMustFail(committers @ _, optLocation @ _) =>
            throw new RuntimeException(
              "FATAL: Encountered scenario instruction for submitMustFail in Daml script"
            )
          case SResultScenarioMustFail(ptx @ _, committers @ _, callback @ _) =>
            throw new RuntimeException(
              "FATAL: Encountered scenario instruction for submitMustFail in Daml Script"
            )
          case SResultScenarioPassTime(relTime @ _, callback @ _) =>
            throw new RuntimeException(
              "FATAL: Encountered scenario instruction setTime in Daml Script"
            )
          case SResultScenarioGetParty(partyText @ _, callback @ _) =>
            throw new RuntimeException(
              "FATAL: Encountered scenario instruction getParty in Daml Script"
            )
        }
      }
      Right(result)
    }

  override def submit(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Either[StatusRuntimeException, Seq[ScriptLedgerClient.CommandResult]]] =
    unsafeSubmit(actAs, readAs, commands, optLocation).map {
      case Right(richTransaction) =>
        val transaction = richTransaction.transaction
        // Expected successful commit so clear.
        machine.clearCommit
        def convRootEvent(id: NodeId): ScriptLedgerClient.CommandResult = {
          val node = transaction.nodes.getOrElse(
            id,
            throw new IllegalArgumentException(s"Unknown root node id $id"),
          )
          node match {
            case create: NodeCreate[ContractId] => ScriptLedgerClient.CreateResult(create.coid)
            case exercise: NodeExercises[NodeId, ContractId] =>
              ScriptLedgerClient.ExerciseResult(
                exercise.templateId,
                exercise.choiceId,
                exercise.exerciseResult.get,
              )
            case _: NodeFetch[_] | _: NodeLookupByKey[_] | _: NodeRollback[_] =>
              throw new IllegalArgumentException(s"Invalid root node: $node")
          }
        }
        Right(transaction.roots.toSeq.map(convRootEvent(_)))
      case Left(err) =>
        // Unexpected failure, do not clear so we can display the partial
        // transaction.
        Left(err)
    }

  override def submitMustFail(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit ec: ExecutionContext, mat: Materializer): Future[Either[Unit, Unit]] = {
    unsafeSubmit(actAs, readAs, commands, optLocation)
      .map({
        case Right(_) => Left(())
        // We don't expect to hit this case but list it for completeness.
        case Left(_) => Right(())
      })
      .recoverWith({ case _: SError =>
        // Expected failed commit so clear, we do not clear on
        // unexpected successes to keep the partial transaction.
        machine.clearCommit
        Future.successful(Right(()))
      })
  }

  override def submitTree(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[ScriptLedgerClient.TransactionTree] = {
    unsafeSubmit(actAs, readAs, commands, optLocation).map {
      case Right(richTransaction) =>
        // Expected successful commit so clear.
        machine.clearCommit
        val transaction = richTransaction.transaction
        def convEvent(id: NodeId): Option[ScriptLedgerClient.TreeEvent] =
          transaction.nodes(id) match {
            case create: NodeCreate[ContractId] =>
              Some(ScriptLedgerClient.Created(create.templateId, create.coid, create.arg))
            case exercise: NodeExercises[NodeId, ContractId] =>
              Some(
                ScriptLedgerClient.Exercised(
                  exercise.templateId,
                  exercise.targetCoid,
                  exercise.choiceId,
                  exercise.chosenValue,
                  exercise.children.collect(Function.unlift(convEvent(_))).toList,
                )
              )
            case _: NodeFetch[_] | _: NodeLookupByKey[_] | _: NodeRollback[_] => None
          }
        ScriptLedgerClient.TransactionTree(
          transaction.roots.collect(Function.unlift(convEvent(_))).toList
        )
      case Left(err) => throw new IllegalStateException(err)
    }
  }

  // All parties known to the ledger. This may include parties that were not
  // allocated explicitly, e.g. parties created by `partyFromText`.
  private def getLedgerParties(): Iterable[Ref.Party] = {
    scenarioRunner.ledger.ledgerData.nodeInfos.values.flatMap(_.disclosures.keys)
  }

  override def allocateParty(partyIdHint: String, displayName: String)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ) = {
    val usedNames = getLedgerParties().toSet ++ allocatedParties.keySet
    Future.fromTry(for {
      name <-
        if (partyIdHint != "") {
          // Try to allocate the given hint as party name. Will fail if the name is already taken.
          if (usedNames contains partyIdHint) {
            Failure(new ScenarioErrorPartyAlreadyExists(partyIdHint))
          } else {
            Success(partyIdHint)
          }
        } else {
          // Allocate a fresh name based on the display name.
          val candidates = displayName #:: LazyList.from(1).map(displayName + _.toString())
          Success(candidates.find(s => !(usedNames contains s)).get)
        }
      // Create and store the new party.
      partyDetails = PartyDetails(
        party = Ref.Party.assertFromString(name),
        displayName = Some(displayName),
        isLocal = true,
      )
      _ = allocatedParties += (name -> partyDetails)
    } yield partyDetails.party)
  }

  override def listKnownParties()(implicit ec: ExecutionContext, mat: Materializer) = {
    val ledgerParties: Map[String, PartyDetails] = getLedgerParties()
      .map(p => (p -> PartyDetails(party = p, displayName = None, isLocal = true)))
      .toMap
    Future.successful((ledgerParties ++ allocatedParties).values.toList)
  }

  override def getStaticTime()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Time.Timestamp] = {
    Future.successful(scenarioRunner.ledger.currentTime)
  }

  override def setStaticTime(time: Time.Timestamp)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] = {
    val diff = time.micros - scenarioRunner.ledger.currentTime.micros
    // ScenarioLedger only provides pass, so we have to calculate the diff.
    // Note that ScenarioLedger supports going backwards in time.
    scenarioRunner.ledger = scenarioRunner.ledger.passTime(diff)
    Future.unit
  }

  override def tracelogIterator = traceLog.iterator
  override def clearTracelog = traceLog.clear
}
