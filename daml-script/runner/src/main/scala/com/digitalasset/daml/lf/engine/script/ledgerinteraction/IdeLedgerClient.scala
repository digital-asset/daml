// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package script
package ledgerinteraction

import akka.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.PartyDetails
import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.scenario.{ScenarioLedger, ScenarioRunner}
import com.daml.lf.speedy.{SValue, TraceLog, WarningLog}
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
import com.daml.script.converter.ConverterException
import io.grpc.StatusRuntimeException
import scalaz.OneAnd
import scalaz.OneAnd._
import scalaz.std.set._
import scalaz.syntax.foldable._

import scala.collection.compat.immutable.LazyList
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

// Client for the script service.
class IdeLedgerClient(
    val compiledPackages: CompiledPackages,
    traceLog: TraceLog,
    warningLog: WarningLog,
) extends ScriptLedgerClient {
  private var seed = crypto.Hash.hashPrivateKey(s"script-service")

  private var _currentSubmission: Option[ScenarioRunner.CurrentSubmission] = None

  def currentSubmission: Option[ScenarioRunner.CurrentSubmission] = _currentSubmission

  private[this] val preprocessor =
    // TODO: https://github.com/digital-asset/daml/pull/10504
    //  See if we can require CID Suffixes
    new preprocessing.CommandPreprocessor(compiledPackages.interface, requiredCidSuffix = false)

  private var _ledger: ScenarioLedger = ScenarioLedger.initialLedger(Time.Timestamp.Epoch)
  def ledger: ScenarioLedger = _ledger

  private var allocatedParties: Map[String, PartyDetails] = Map()

  override def query(parties: OneAnd[Set, Ref.Party], templateId: Identifier)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Seq[ScriptLedgerClient.ActiveContract]] = {
    val acs = ledger.query(
      view = ScenarioLedger.ParticipantView(Set(), Set(parties.toList: _*)),
      effectiveAt = ledger.currentTime,
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
    ledger.lookupGlobalContract(
      view = ScenarioLedger.ParticipantView(Set(), Set(parties.toList: _*)),
      effectiveAt = ledger.currentTime,
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
        ledger.ledgerData.activeKeys.get(gkey) match {
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
    ScenarioRunner.SubmissionResult[ScenarioLedger.CommitResult]
  ] =
    Future {
      val speedyCommands = preprocessor.unsafePreprocessCommands(commands.to(ImmArray))
      val translated = compiledPackages.compiler.unsafeCompile(speedyCommands)

      val ledgerApi = ScenarioRunner.ScenarioLedgerApi(ledger)
      val result = ScenarioRunner.submit(
        compiledPackages,
        ledgerApi,
        actAs.toSet,
        readAs,
        translated,
        optLocation,
        seed,
        traceLog,
        warningLog,
      )
      result
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
      case ScenarioRunner.Commit(result, _, _) =>
        _currentSubmission = None
        _ledger = result.newLedger
        seed = ScenarioRunner.nextSeed(
          crypto.Hash.deriveNodeSeed(seed, result.richTransaction.transaction.roots.length)
        )
        val transaction = result.richTransaction.transaction
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
      case ScenarioRunner.SubmissionError(err, ptx) =>
        _currentSubmission =
          Some(ScenarioRunner.CurrentSubmission(optLocation, ptx.finishIncomplete))
        throw err
    }

  override def submitMustFail(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit ec: ExecutionContext, mat: Materializer): Future[Either[Unit, Unit]] = {
    unsafeSubmit(actAs, readAs, commands, optLocation)
      .map({
        case commit: ScenarioRunner.Commit[_] =>
          _currentSubmission =
            Some(ScenarioRunner.CurrentSubmission(optLocation, commit.ptx.finishIncomplete))
          Left(())
        case error: ScenarioRunner.SubmissionError =>
          _currentSubmission = None
          _ledger = ledger.insertAssertMustFail(actAs.toSet, readAs, optLocation)
          seed = ScenarioRunner.nextSeed(
            error.ptx.unwind().context.nextActionChildSeed
          )
          Right(())
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
      case ScenarioRunner.Commit(result, _, _) =>
        _currentSubmission = None
        _ledger = result.newLedger
        seed = ScenarioRunner.nextSeed(
          crypto.Hash.deriveNodeSeed(seed, result.richTransaction.transaction.roots.length)
        )
        val transaction = result.richTransaction.transaction
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
      case ScenarioRunner.SubmissionError(err, ptx) =>
        _currentSubmission =
          Some(ScenarioRunner.CurrentSubmission(optLocation, ptx.finishIncomplete))
        throw new IllegalStateException(err)
    }
  }

  // All parties known to the ledger. This may include parties that were not
  // allocated explicitly, e.g. parties created by `partyFromText`.
  private def getLedgerParties(): Iterable[Ref.Party] = {
    ledger.ledgerData.nodeInfos.values.flatMap(_.disclosures.keys)
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
            Failure(scenario.Error.PartyAlreadyExists(partyIdHint))
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
    Future.successful(ledger.currentTime)
  }

  override def setStaticTime(time: Time.Timestamp)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] = {
    val diff = time.micros - ledger.currentTime.micros
    // ScenarioLedger only provides pass, so we have to calculate the diff.
    // Note that ScenarioLedger supports going backwards in time.
    _ledger = ledger.passTime(diff)
    Future.unit
  }
}
