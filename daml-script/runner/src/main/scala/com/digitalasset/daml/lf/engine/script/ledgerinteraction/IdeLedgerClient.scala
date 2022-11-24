// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package script
package ledgerinteraction

import akka.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.{IdentityProviderId, ObjectMeta, PartyDetails, User, UserRight}
import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.engine.preprocessing.ValueTranslator
import com.daml.lf.language.Ast
import com.daml.lf.language.Ast.TTyCon
import com.daml.lf.scenario.{ScenarioLedger, ScenarioRunner}
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.Speedy.Machine
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
import com.daml.logging.LoggingContext
import com.daml.platform.localstore.InMemoryUserManagementStore
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
      compiledPackages.pkgInterface,
      requireV1ContractIdSuffix = false,
    )

  private var _ledger: ScenarioLedger = ScenarioLedger.initialLedger(Time.Timestamp.Epoch)
  def ledger: ScenarioLedger = _ledger

  private var allocatedParties: Map[String, PartyDetails] = Map()

  private val userManagementStore = new InMemoryUserManagementStore(createAdmin = false)

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

  private def lookupContractInstance(
      parties: OneAnd[Set, Ref.Party],
      cid: ContractId,
  ): Option[Value.ContractInstance] = {

    ledger.lookupGlobalContract(
      view = ScenarioLedger.ParticipantView(Set(), Set(parties.toList: _*)),
      effectiveAt = ledger.currentTime,
      cid,
    ) match {
      case ScenarioLedger.LookupOk(
            _,
            Versioned(_, contractInstance),
            stakeholders,
          ) if parties.any(stakeholders.contains(_)) =>
        Some(contractInstance)
      case _ =>
        // Note that contrary to `fetch` in a scenario, we do not
        // abort on any of the error cases. This makes sense if you
        // consider this a wrapper around the ACS endpoint where
        // we cannot differentiate between visibility errors
        // and the contract not being active.
        None
    }
  }

  override def queryContractId(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
      cid: ContractId,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Option[ScriptLedgerClient.ActiveContract]] = {
    Future.successful(
      lookupContractInstance(parties, cid).map { case Value.ContractInstance(_, arg, _) =>
        ScriptLedgerClient.ActiveContract(templateId, cid, arg)
      }
    )
  }

  private[this] def computeView(
      templateId: TypeConName,
      interfaceId: TypeConName,
      arg: Value,
  ): Option[Value] = {

    val valueTranslator = new ValueTranslator(
      pkgInterface = compiledPackages.pkgInterface,
      requireV1ContractIdSuffix = false,
    )

    valueTranslator.translateValue(TTyCon(templateId), arg) match {
      case Left(_) =>
        sys.error("computeView: translateValue failed")

      case Right(argument) =>
        val compiler: speedy.Compiler = compiledPackages.compiler
        val iview = speedy.InterfaceView(templateId, argument, interfaceId)
        val sexpr = compiler.unsafeCompileInterfaceView(iview)
        val machine = Machine.fromPureSExpr(compiledPackages, sexpr)(Script.DummyLoggingContext)

        machine.run() match {
          case SResultFinal(svalue) =>
            val version = machine.tmplId2TxVersion(templateId)
            Some(svalue.toNormalizedValue(version))

          case _: SResultError =>
            None

          case res @ (_: SResultNeedPackage | _: SResultNeedContract | _: SResultNeedKey |
              _: SResultNeedTime | _: SResultScenarioGetParty | _: SResultScenarioPassTime |
              _: SResultScenarioSubmit) =>
            sys.error(s"computeView: expected SResultFinal, got: $res")
        }
    }
  }

  private[this] def implements(templateId: TypeConName, interfaceId: TypeConName): Boolean = {
    compiledPackages.pkgInterface.lookupInterfaceInstance(interfaceId, templateId).isRight
  }

  override def queryInterface(
      parties: OneAnd[Set, Ref.Party],
      interfaceId: Identifier,
      viewType: Ast.Type,
  )(implicit ec: ExecutionContext, mat: Materializer): Future[Seq[(ContractId, Option[Value])]] = {

    val acs: Seq[ScenarioLedger.LookupOk] = ledger.query(
      view = ScenarioLedger.ParticipantView(Set(), Set(parties.toList: _*)),
      effectiveAt = ledger.currentTime,
    )
    val filtered: Seq[(ContractId, Value.ContractInstance)] = acs.collect {
      case ScenarioLedger.LookupOk(
            cid,
            Versioned(_, contractInstance @ Value.ContractInstance(templateId, _, _)),
            stakeholders,
          ) if implements(templateId, interfaceId) && parties.any(stakeholders.contains(_)) =>
        (cid, contractInstance)
    }
    val res: Seq[(ContractId, Option[Value])] = {
      filtered.map { case (cid, contractInstance) =>
        contractInstance match {
          case Value.ContractInstance(templateId, arg, _) =>
            val viewOpt = computeView(templateId, interfaceId, arg)
            (cid, viewOpt)
        }
      }
    }
    Future.successful(res)
  }

  override def queryInterfaceContractId(
      parties: OneAnd[Set, Ref.Party],
      interfaceId: Identifier,
      viewType: Ast.Type,
      cid: ContractId,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Option[Value]] = {

    lookupContractInstance(parties, cid) match {
      case None => Future.successful(None)
      case Some(Value.ContractInstance(templateId, arg, _)) =>
        val viewOpt = computeView(templateId, interfaceId, arg)
        Future.successful(viewOpt)
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

      val speedyCommands = preprocessor.unsafePreprocessApiCommands(commands.to(ImmArray))
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
                exercise.interfaceId,
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
                  exercise.interfaceId,
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
        metadata = ObjectMeta.empty,
        identityProviderId = IdentityProviderId.Default,
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
    userManagementStore
      .createUser(user, rights.toSet)(LoggingContext.empty)
      .map(_.toOption.map(_ => ()))

  override def getUser(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[User]] =
    userManagementStore
      .getUser(id, IdentityProviderId.Default)(LoggingContext.empty)
      .map(_.toOption)

  override def deleteUser(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[Unit]] =
    userManagementStore
      .deleteUser(id, IdentityProviderId.Default)(LoggingContext.empty)
      .map(_.toOption)

  override def listAllUsers()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[List[User]] =
    userManagementStore.listAllUsers()

  override def grantUserRights(
      id: UserId,
      rights: List[UserRight],
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]] =
    userManagementStore
      .grantRights(id, rights.toSet, IdentityProviderId.Default)(
        LoggingContext.empty
      )
      .map(_.toOption.map(_.toList))

  override def revokeUserRights(
      id: UserId,
      rights: List[UserRight],
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]] =
    userManagementStore
      .revokeRights(id, rights.toSet, IdentityProviderId.Default)(
        LoggingContext.empty
      )
      .map(_.toOption.map(_.toList))

  override def listUserRights(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]] =
    userManagementStore
      .listUserRights(id, IdentityProviderId.Default)(
        LoggingContext.empty
      )
      .map(_.toOption.map(_.toList))
}
