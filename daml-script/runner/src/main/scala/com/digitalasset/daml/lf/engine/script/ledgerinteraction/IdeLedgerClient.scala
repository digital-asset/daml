// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package script
package ledgerinteraction

import akka.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.{PartyDetails, User, UserRight}
import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.scenario.{ScenarioLedger, ScenarioRunner}
import com.daml.lf.speedy.{SValue, TraceLog, WarningLog}
import com.daml.lf.transaction.{
  GlobalKey,
  IncompleteTransaction,
  Node,
  NodeId,
  Transaction,
  Versioned,
}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.script.converter.ConverterException
import io.grpc.StatusRuntimeException
import scalaz.OneAnd
import scalaz.OneAnd._
import scalaz.std.set._
import scalaz.syntax.foldable._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

// Client for the script service.
class IdeLedgerClient(
    val compiledPackages: CompiledPackages,
    traceLog: TraceLog,
    warningLog: WarningLog,
) extends ScriptLedgerClient {
  override def transport = "script service"

  private val nextSeed: () => crypto.Hash =
    // We seeds to secureRandom with a fix seed to get deterministic sequences of seeds
    // across different runs of IdeLedgerClient.
    crypto.Hash.secureRandom(crypto.Hash.hashPrivateKey(s"script-service"))

  private var _currentSubmission: Option[ScenarioRunner.CurrentSubmission] = None

  def currentSubmission: Option[ScenarioRunner.CurrentSubmission] = _currentSubmission

  private[this] val preprocessor =
    new preprocessing.CommandPreprocessor(
      compiledPackages.interface,
      requireV1ContractIdSuffix = false,
    )

  private var _ledger: ScenarioLedger = ScenarioLedger.initialLedger(Time.Timestamp.Epoch)
  def ledger: ScenarioLedger = _ledger

  private var allocatedParties: Map[String, PartyDetails] = Map()

  private val userManagementStore = new ide.UserManagementStore()

  override def query(parties: OneAnd[Set, Ref.Party], templateId: Identifier)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Seq[ScriptLedgerClient.ActiveContract]] = {
    val acs = ledger.query(
      view = ScenarioLedger.ParticipantView(Set(), Set(parties.toList: _*)),
      effectiveAt = ledger.currentTime,
    )
    val filtered = acs.collect {
      case ScenarioLedger.LookupOk(
            cid,
            Versioned(_, Value.ContractInstance(tpl, arg, _)),
            stakeholders,
          ) if tpl == templateId && parties.any(stakeholders.contains(_)) =>
        (cid, arg)
    }
    Future.successful(filtered.map { case (cid, c) =>
      ScriptLedgerClient.ActiveContract(templateId, cid, c)
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
      case ScenarioLedger.LookupOk(_, Versioned(_, Value.ContractInstance(_, arg, _)), stakeholders)
          if parties.any(stakeholders.contains(_)) =>
        Future.successful(Some(ScriptLedgerClient.ActiveContract(templateId, cid, arg)))
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
      translateKey: (Identifier, Value) => Either[String, SValue],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Option[ScriptLedgerClient.ActiveContract]] = {
    GlobalKey
      .build(templateId, key.toUnnormalizedValue)
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
  ] = Future {
    val unallocatedSubmitters: Set[Party] =
      (actAs.toSet union readAs) -- allocatedParties.values.map(_.party)
    if (unallocatedSubmitters.nonEmpty) {
      ScenarioRunner.SubmissionError(
        scenario.Error.PartiesNotAllocated(unallocatedSubmitters),
        IncompleteTransaction(
          transaction = Transaction(Map.empty, ImmArray.empty),
          locationInfo = Map.empty,
        ),
      )
    } else {

      val speedyCommands = preprocessor.unsafePreprocessCommands(commands.to(ImmArray))
      val translated = compiledPackages.compiler.unsafeCompile(speedyCommands)

      val ledgerApi = ScenarioRunner.ScenarioLedgerApi(ledger)
      val result =
        ScenarioRunner.submit(
          compiledPackages,
          ledgerApi,
          actAs.toSet,
          readAs,
          translated,
          optLocation,
          nextSeed(),
          traceLog,
          warningLog,
        )(Script.DummyLoggingContext)
      result match {
        case err: ScenarioRunner.SubmissionError => err
        case commit: ScenarioRunner.Commit[_] =>
          val referencedParties: Set[Party] =
            commit.result.richTransaction.blindingInfo.disclosure.values
              .foldLeft(Set.empty[Party])(_ union _)
          val unallocatedParties = referencedParties -- allocatedParties.values.map(_.party)
          if (unallocatedParties.nonEmpty) {
            ScenarioRunner.SubmissionError(
              scenario.Error.PartiesNotAllocated(unallocatedParties),
              commit.tx,
            )
          } else {
            commit
          }
      }
    }
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
        val transaction = result.richTransaction.transaction
        def convRootEvent(id: NodeId): ScriptLedgerClient.CommandResult = {
          val node = transaction.nodes.getOrElse(
            id,
            throw new IllegalArgumentException(s"Unknown root node id $id"),
          )
          node match {
            case create: Node.Create => ScriptLedgerClient.CreateResult(create.coid)
            case exercise: Node.Exercise =>
              ScriptLedgerClient.ExerciseResult(
                exercise.templateId,
                exercise.choiceId,
                exercise.exerciseResult.get,
              )
            case _: Node.Fetch | _: Node.LookupByKey | _: Node.Rollback =>
              throw new IllegalArgumentException(s"Invalid root node: $node")
          }
        }
        Right(transaction.roots.toSeq.map(convRootEvent(_)))
      case ScenarioRunner.SubmissionError(err, tx) =>
        _currentSubmission = Some(ScenarioRunner.CurrentSubmission(optLocation, tx))
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
          _currentSubmission = Some(ScenarioRunner.CurrentSubmission(optLocation, commit.tx))
          Left(())
        case _: ScenarioRunner.SubmissionError =>
          _currentSubmission = None
          _ledger = ledger.insertAssertMustFail(actAs.toSet, readAs, optLocation)
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
        val transaction = result.richTransaction.transaction
        def convEvent(id: NodeId): Option[ScriptLedgerClient.TreeEvent] =
          transaction.nodes(id) match {
            case create: Node.Create =>
              Some(ScriptLedgerClient.Created(create.templateId, create.coid, create.arg))
            case exercise: Node.Exercise =>
              Some(
                ScriptLedgerClient.Exercised(
                  exercise.templateId,
                  exercise.targetCoid,
                  exercise.choiceId,
                  exercise.chosenValue,
                  exercise.children.collect(Function.unlift(convEvent(_))).toList,
                )
              )
            case _: Node.Fetch | _: Node.LookupByKey | _: Node.Rollback => None
          }
        ScriptLedgerClient.TransactionTree(
          transaction.roots.collect(Function.unlift(convEvent(_))).toList
        )
      case ScenarioRunner.SubmissionError(err, tx) =>
        _currentSubmission = Some(ScenarioRunner.CurrentSubmission(optLocation, tx))
        throw new IllegalStateException(err)
    }
  }

  override def allocateParty(partyIdHint: String, displayName: String)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ) = {
    val usedNames = allocatedParties.keySet
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
          // Empty party ids are not allowed, fall back to "party" on empty display name.
          val namePrefix = if (displayName.isEmpty) { "party" }
          else { displayName }
          val candidates = namePrefix #:: LazyList.from(1).map(namePrefix + _.toString())
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
    Future.successful(allocatedParties.values.toList)
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

  override def createUser(
      user: User,
      rights: List[UserRight],
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[Unit]] =
    Future.successful(userManagementStore.createUser(user, rights.toSet))

  override def getUser(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[User]] =
    Future.successful(userManagementStore.getUser(id))

  override def deleteUser(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[Unit]] =
    Future.successful(userManagementStore.deleteUser(id))

  override def listAllUsers()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[List[User]] =
    Future.successful(userManagementStore.listUsers())

  override def grantUserRights(
      id: UserId,
      rights: List[UserRight],
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]] =
    Future.successful(userManagementStore.grantRights(id, rights.toSet).map(_.toList))

  override def revokeUserRights(
      id: UserId,
      rights: List[UserRight],
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]] =
    Future.successful(userManagementStore.revokeRights(id, rights.toSet).map(_.toList))

  override def listUserRights(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]] =
    Future.successful(userManagementStore.listUserRights(id).map(_.toList))
}
