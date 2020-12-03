// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package script

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.unmarshalling._
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import io.grpc.{Status, StatusRuntimeException}
import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.Uri.Path

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import spray.json._
import com.daml.api.util.TimestampConversion
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.akka.ClientAdapter
import com.daml.lf.scenario.ScenarioLedger
import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.data.Time
import com.daml.lf.iface.{EnvironmentInterface, InterfaceType}
import com.daml.lf.language.Ast._
import com.daml.lf.transaction.Node.{NodeCreate, NodeExercises}
import com.daml.lf.speedy.{ScenarioRunner, TraceLog}
import com.daml.lf.speedy.Speedy.{Machine, OffLedger, OnLedger}
import com.daml.lf.speedy.{PartialTransaction, SValue}
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SResult._
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.jwt.domain.Jwt
import com.daml.jwt.JwtDecoder
import com.daml.ledger.api.auth.{AuthServiceJWTCodec, AuthServiceJWTPayload}
import com.daml.ledger.api.domain.PartyDetails
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands._
import com.daml.ledger.api.v1.testing.time_service.{GetTimeRequest, SetTimeRequest, TimeServiceGrpc}
import com.daml.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeServiceStub
import com.daml.ledger.api.v1.transaction.TreeEvent
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}
import com.daml.ledger.api.validation.ValueValidator
import com.daml.ledger.client.LedgerClient
import com.daml.platform.participant.util.LfEngineToApi.{
  lfValueToApiRecord,
  lfValueToApiValue,
  toApiIdentifier
}
import com.daml.script.converter.ConverterException
import scalaz.OneAnd._
import scalaz.std.either._
import scalaz.std.set._
import scalaz.std.list._
import scalaz.syntax.equal._
import scalaz.syntax.tag._
import scalaz.syntax.foldable._
import scalaz.{-\/, OneAnd, \/-}

// We have our own type for time modes since TimeProviderType
// allows for more stuff that doesn’t make sense in DAML Script.
sealed trait ScriptTimeMode

object ScriptTimeMode {
  final case object Static extends ScriptTimeMode
  final case object WallClock extends ScriptTimeMode
}

object ScriptLedgerClient {

  sealed trait CommandResult
  final case class CreateResult(contractId: ContractId) extends CommandResult
  final case class ExerciseResult(
      templateId: Identifier,
      choice: ChoiceName,
      result: Value[ContractId])
      extends CommandResult

  final case class ActiveContract(
      templateId: Identifier,
      contractId: ContractId,
      argument: Value[ContractId])
}

// This abstracts over the interaction with the ledger. This allows
// us to plug in something that interacts with the JSON API as well as
// something that works against the gRPC API.
trait ScriptLedgerClient {
  def query(parties: OneAnd[Set, Ref.Party], templateId: Identifier)(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[Seq[ScriptLedgerClient.ActiveContract]]

  def queryContractId(parties: OneAnd[Set, Ref.Party], templateId: Identifier, cid: ContractId)(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[Option[ScriptLedgerClient.ActiveContract]]

  def queryContractKey(parties: OneAnd[Set, Ref.Party], templateId: Identifier, key: SValue)(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[Option[ScriptLedgerClient.ActiveContract]]

  def submit(party: Ref.Party, commands: List[command.Command], optLocation: Option[Location])(
      implicit ec: ExecutionContext,
      mat: Materializer)
    : Future[Either[StatusRuntimeException, Seq[ScriptLedgerClient.CommandResult]]]

  def submitMustFail(
      party: Ref.Party,
      commands: List[command.Command],
      optLocation: Option[Location])(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[Either[Unit, Unit]]

  def allocateParty(partyIdHint: String, displayName: String)(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[Ref.Party]

  def listKnownParties()(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[List[PartyDetails]]

  def getStaticTime()(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer): Future[Time.Timestamp]

  def setStaticTime(time: Time.Timestamp)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer): Future[Unit]

  def tracelogIterator: Iterator[(String, Option[Location])]

  def clearTracelog: Unit
}

class GrpcLedgerClient(val grpcClient: LedgerClient, val applicationId: ApplicationId)
    extends ScriptLedgerClient {
  override def query(parties: OneAnd[Set, Ref.Party], templateId: Identifier)(
      implicit ec: ExecutionContext,
      mat: Materializer) = {
    queryWithKey(parties, templateId).map(_.map(_._1))
  }

  private def transactionFilter(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier): TransactionFilter = {
    val filters = Filters(Some(InclusiveFilters(Seq(toApiIdentifier(templateId)))))
    TransactionFilter(parties.toList.map(p => (p, filters)).toMap)
  }

  // Helper shared by query, queryContractId and queryContractKey
  private def queryWithKey(parties: OneAnd[Set, Ref.Party], templateId: Identifier)(
      implicit ec: ExecutionContext,
      mat: Materializer)
    : Future[Seq[(ScriptLedgerClient.ActiveContract, Option[Value[ContractId]])]] = {
    val filter = transactionFilter(parties, templateId)
    val acsResponses =
      grpcClient.activeContractSetClient
        .getActiveContracts(filter, verbose = false)
        .runWith(Sink.seq)
    acsResponses.map(acsPages =>
      acsPages.flatMap(page =>
        page.activeContracts.map(createdEvent => {
          val argument = ValueValidator.validateRecord(createdEvent.getCreateArguments) match {
            case Left(err) => throw new ConverterException(err.toString)
            case Right(argument) => argument
          }
          val key: Option[Value[ContractId]] = createdEvent.contractKey.map { key =>
            ValueValidator.validateValue(key) match {
              case Left(err) => throw new ConverterException(err.toString)
              case Right(argument) => argument
            }
          }
          val cid =
            ContractId
              .fromString(createdEvent.contractId)
              .fold(
                err => throw new ConverterException(err),
                identity
              )
          (ScriptLedgerClient.ActiveContract(templateId, cid, argument), key)
        })))
  }

  override def queryContractId(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
      cid: ContractId)(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[Option[ScriptLedgerClient.ActiveContract]] = {
    // We cannot do better than a linear search over query here.
    for {
      activeContracts <- query(parties, templateId)
    } yield {
      activeContracts.find(c => c.contractId == cid)
    }
  }

  override def queryContractKey(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
      key: SValue)(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[Option[ScriptLedgerClient.ActiveContract]] = {
    // We cannot do better than a linear search over query here.
    for {
      activeContracts <- queryWithKey(parties, templateId)
    } yield {
      activeContracts.collectFirst({ case (c, Some(k)) if k === key.toValue => c })
    }
  }

  override def submit(
      party: Ref.Party,
      commands: List[command.Command],
      optLocation: Option[Location])(implicit ec: ExecutionContext, mat: Materializer) = {
    import scalaz.syntax.traverse._
    val ledgerCommands = commands.traverse(toCommand(_)) match {
      case Left(err) => throw new ConverterException(err)
      case Right(cmds) => cmds
    }
    val apiCommands = Commands(
      party = party,
      commands = ledgerCommands,
      ledgerId = grpcClient.ledgerId.unwrap,
      applicationId = applicationId.unwrap,
      commandId = UUID.randomUUID.toString,
    )
    val request = SubmitAndWaitRequest(Some(apiCommands))
    val transactionTreeF = grpcClient.commandServiceClient
      .submitAndWaitForTransactionTree(request)
      .map(Right(_))
      .recoverWith({
        case s: StatusRuntimeException
            // This is used for submit must fail so we only catch ABORTED and INVALID_ARGUMENT.
            // Errors like PERMISSION_DENIED are not caught.
            if s.getStatus.getCode == Status.Code.ABORTED || s.getStatus.getCode == Status.Code.INVALID_ARGUMENT =>
          Future.successful(Left(s))

      })
    transactionTreeF.map(r =>
      r.right.map(transactionTree => {
        val events = transactionTree.getTransaction.rootEventIds
          .map(evId => transactionTree.getTransaction.eventsById(evId))
          .toList
        events.traverse(fromTreeEvent(_)) match {
          case Left(err) => throw new ConverterException(err)
          case Right(results) => results
        }
      }))
  }

  override def submitMustFail(
      party: Ref.Party,
      commands: List[command.Command],
      optLocation: Option[Location])(implicit ec: ExecutionContext, mat: Materializer) = {
    submit(party, commands, optLocation).map({
      case Right(_) => Left(())
      case Left(_) => Right(())
    })
  }

  override def allocateParty(partyIdHint: String, displayName: String)(
      implicit ec: ExecutionContext,
      mat: Materializer) = {
    grpcClient.partyManagementClient
      .allocateParty(Some(partyIdHint), Some(displayName))
      .map(_.party)
  }

  override def listKnownParties()(implicit ec: ExecutionContext, mat: Materializer) = {
    grpcClient.partyManagementClient
      .listKnownParties()
  }

  override def getStaticTime()(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer): Future[Time.Timestamp] = {
    val timeService: TimeServiceStub = TimeServiceGrpc.stub(grpcClient.channel)
    for {
      resp <- ClientAdapter
        .serverStreaming(GetTimeRequest(grpcClient.ledgerId.unwrap), timeService.getTime)
        .runWith(Sink.head)
    } yield Time.Timestamp.assertFromInstant(TimestampConversion.toInstant(resp.getCurrentTime))
  }

  override def setStaticTime(time: Time.Timestamp)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer): Future[Unit] = {
    val timeService: TimeServiceStub = TimeServiceGrpc.stub(grpcClient.channel)
    for {
      oldTime <- ClientAdapter
        .serverStreaming(GetTimeRequest(grpcClient.ledgerId.unwrap), timeService.getTime)
        .runWith(Sink.head)
      _ <- timeService.setTime(
        SetTimeRequest(
          grpcClient.ledgerId.unwrap,
          oldTime.currentTime,
          Some(TimestampConversion.fromInstant(time.toInstant))))
    } yield ()
  }

  private def toCommand(cmd: command.Command): Either[String, Command] =
    cmd match {
      case command.CreateCommand(templateId, argument) =>
        for {
          arg <- lfValueToApiRecord(true, argument)
        } yield Command().withCreate(CreateCommand(Some(toApiIdentifier(templateId)), Some(arg)))
      case command.ExerciseCommand(templateId, contractId, choice, argument) =>
        for {
          arg <- lfValueToApiValue(true, argument)
        } yield
          Command().withExercise(
            ExerciseCommand(Some(toApiIdentifier(templateId)), contractId.coid, choice, Some(arg)))
      case command.ExerciseByKeyCommand(templateId, key, choice, argument) =>
        for {
          key <- lfValueToApiValue(true, key)
          argument <- lfValueToApiValue(true, argument)
        } yield
          Command().withExerciseByKey(
            ExerciseByKeyCommand(
              Some(toApiIdentifier(templateId)),
              Some(key),
              choice,
              Some(argument)))
      case command.CreateAndExerciseCommand(templateId, template, choice, argument) =>
        for {
          template <- lfValueToApiRecord(true, template)
          argument <- lfValueToApiValue(true, argument)
        } yield
          Command().withCreateAndExercise(
            CreateAndExerciseCommand(
              Some(toApiIdentifier(templateId)),
              Some(template),
              choice,
              Some(argument)))
    }

  private def fromTreeEvent(ev: TreeEvent): Either[String, ScriptLedgerClient.CommandResult] =
    ev match {
      case TreeEvent(TreeEvent.Kind.Created(created)) =>
        for {
          cid <- ContractId.fromString(created.contractId)
        } yield ScriptLedgerClient.CreateResult(cid)
      case TreeEvent(TreeEvent.Kind.Exercised(exercised)) =>
        for {
          result <- ValueValidator.validateValue(exercised.getExerciseResult).left.map(_.toString)
          templateId <- Converter.fromApiIdentifier(exercised.getTemplateId)
          choice <- ChoiceName.fromString(exercised.choice)
        } yield ScriptLedgerClient.ExerciseResult(templateId, choice, result)
      case TreeEvent(TreeEvent.Kind.Empty) =>
        throw new ConverterException("Invalid tree event Empty")
    }

  override def tracelogIterator = Iterator.empty
  override def clearTracelog = ()
}

// Client for the script service.
class IdeClient(val compiledPackages: CompiledPackages) extends ScriptLedgerClient {
  class ArrayBufferTraceLog extends TraceLog {
    val buffer = ArrayBuffer[(String, Option[Location])]()
    override def add(message: String, optLocation: Option[Location]): Unit = {
      buffer.append((message, optLocation))
    }
    override def iterator: Iterator[(String, Option[Location])] = {
      buffer.iterator
    }
    def clear: Unit = buffer.clear
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

  override def query(parties: OneAnd[Set, Ref.Party], templateId: Identifier)(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[Seq[ScriptLedgerClient.ActiveContract]] = {
    val acs = scenarioRunner.ledger.query(
      view = ScenarioLedger.ParticipantView(Set(parties.toList: _*)),
      effectiveAt = scenarioRunner.ledger.currentTime)
    val filtered = acs.collect {
      case ScenarioLedger.LookupOk(cid, Value.ContractInst(tpl, arg, _), stakeholders)
          if tpl == templateId && parties.any(stakeholders.contains(_)) =>
        (cid, arg)
    }
    Future.successful(filtered.map {
      case (cid, c) => ScriptLedgerClient.ActiveContract(templateId, cid, c.value)
    })
  }

  override def queryContractId(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
      cid: ContractId)(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[Option[ScriptLedgerClient.ActiveContract]] = {
    scenarioRunner.ledger.lookupGlobalContract(
      view = ScenarioLedger.ParticipantView(Set(parties.toList: _*)),
      effectiveAt = scenarioRunner.ledger.currentTime,
      cid) match {
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
      key: SValue)(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[Option[ScriptLedgerClient.ActiveContract]] = {
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
      party: Ref.Party,
      commands: List[command.Command],
      optLocation: Option[Location])(implicit ec: ExecutionContext)
    : Future[Either[StatusRuntimeException, Seq[ScriptLedgerClient.CommandResult]]] = Future {
    // Clear state at the beginning like in SBSBeginCommit for scenarios.
    machine.returnValue = null
    onLedger.commitLocation = optLocation
    onLedger.localContracts = Map.empty
    onLedger.globalDiscriminators = Set.empty
    val speedyCommands = preprocessor.unsafePreprocessCommands(commands.to[ImmArray])._1
    val translated = compiledPackages.compiler.unsafeCompile(speedyCommands)
    machine.setExpressionToEvaluate(SEApp(translated, Array(SEValue.Token)))
    onLedger.committers = Set(party)
    var result: Seq[ScriptLedgerClient.CommandResult] = null
    while (result == null) {
      machine.run() match {
        case SResultNeedContract(coid, tid @ _, committers, cbMissing, cbPresent) =>
          scenarioRunner.lookupContract(coid, committers, cbMissing, cbPresent).toTry.get
        case SResultNeedKey(keyWithMaintainers, committers, cb) =>
          scenarioRunner.lookupKey(keyWithMaintainers.globalKey, committers, cb).toTry.get
        case SResultFinalValue(SUnit) =>
          onLedger.ptx.finish match {
            case PartialTransaction.CompleteTransaction(tx) =>
              val results: ImmArray[ScriptLedgerClient.CommandResult] = tx.roots.map { n =>
                tx.nodes(n) match {
                  case create: NodeCreate.WithTxValue[ContractId] =>
                    ScriptLedgerClient.CreateResult(create.coid)
                  case exercise: NodeExercises.WithTxValue[_, ContractId] =>
                    ScriptLedgerClient.ExerciseResult(
                      exercise.templateId,
                      exercise.choiceId,
                      exercise.exerciseResult.get.value)
                  case n =>
                    // Root nodes can only be creates and exercises.
                    throw new RuntimeException(s"Unexpected node: $n")
                }
              }
              ScenarioLedger.commitTransaction(
                committer = party,
                effectiveAt = scenarioRunner.ledger.currentTime,
                optLocation = onLedger.commitLocation,
                tx = tx,
                l = scenarioRunner.ledger
              ) match {
                case Left(fas) =>
                  // Capture the error and exit.
                  throw ScenarioErrorCommitError(fas)
                case Right(commitResult) =>
                  scenarioRunner.ledger = commitResult.newLedger
                  // Capture the result and exit.
                  result = results.toSeq
              }
            case PartialTransaction.IncompleteTransaction(ptx) =>
              throw new RuntimeException(s"Unexpected abort: $ptx")
          }
        case SResultFinalValue(v) =>
          // The final result should always be unit.
          throw new RuntimeException(s"FATAL: Unexpected non-unit final result: $v")
        case SResultScenarioCommit(_, _, _, _) =>
          throw new RuntimeException("FATAL: Encountered scenario commit in DAML Script")
        case SResultError(err) =>
          // Capture the error and exit.
          throw err
        case SResultNeedTime(callback) =>
          callback(scenarioRunner.ledger.currentTime)
        case SResultNeedPackage(pkg, callback @ _) =>
          throw new RuntimeException(
            s"FATAL: Missing package $pkg should have been reported at Script compilation")
        case SResultScenarioInsertMustFail(committers @ _, optLocation @ _) =>
          throw new RuntimeException(
            "FATAL: Encountered scenario instruction for submitMustFail in DAML script")
        case SResultScenarioMustFail(ptx @ _, committers @ _, callback @ _) =>
          throw new RuntimeException(
            "FATAL: Encountered scenario instruction for submitMustFail in DAML Script")
        case SResultScenarioPassTime(relTime @ _, callback @ _) =>
          throw new RuntimeException(
            "FATAL: Encountered scenario instruction setTime in DAML Script")
        case SResultScenarioGetParty(partyText @ _, callback @ _) =>
          throw new RuntimeException(
            "FATAL: Encountered scenario instruction getParty in DAML Script")
      }
    }
    Right(result)
  }

  override def submit(
      party: Ref.Party,
      commands: List[command.Command],
      optLocation: Option[Location])(implicit ec: ExecutionContext, mat: Materializer)
    : Future[Either[StatusRuntimeException, Seq[ScriptLedgerClient.CommandResult]]] =
    unsafeSubmit(party, commands, optLocation).map {
      case Right(x) =>
        // Expected successful commit so clear.
        machine.clearCommit
        Right(x)
      case Left(err) =>
        // Unexpected failure, do not clear so we can display the partial
        // transaction.
        Left(err)
    }

  override def submitMustFail(
      party: Ref.Party,
      commands: List[command.Command],
      optLocation: Option[Location])(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[Either[Unit, Unit]] = {
    unsafeSubmit(party, commands, optLocation)
      .map({
        case Right(_) => Left(())
        // We don't expect to hit this case but list it for completeness.
        case Left(_) => Right(())
      })
      .recoverWith({
        case _: SError =>
          // Expected failed commit so clear, we do not clear on
          // unexpected successes to keep the partial transaction.
          machine.clearCommit
          Future.successful(Right(()))
      })
  }

  // All parties known to the ledger. This may include parties that were not
  // allocated explicitly, e.g. parties created by `partyFromText`.
  private def getLedgerParties(): Iterable[Ref.Party] = {
    scenarioRunner.ledger.ledgerData.nodeInfos.values.flatMap(_.disclosures.keys)
  }

  override def allocateParty(partyIdHint: String, displayName: String)(
      implicit ec: ExecutionContext,
      mat: Materializer) = {
    val usedNames = getLedgerParties.toSet ++ allocatedParties.keySet
    Future.fromTry(for {
      name <- if (partyIdHint != "") {
        // Try to allocate the given hint as party name. Will fail if the name is already taken.
        if (usedNames contains partyIdHint) {
          Failure(new ScenarioErrorPartyAlreadyExists(partyIdHint))
        } else {
          Success(partyIdHint)
        }
      } else {
        // Allocate a fresh name based on the display name.
        val candidates = displayName #:: Stream.from(1).map(displayName + _.toString())
        Success(candidates.find(s => !(usedNames contains s)).get)
      }
      // Create and store the new party.
      partyDetails = PartyDetails(
        party = Ref.Party.assertFromString(name),
        displayName = Some(displayName),
        isLocal = true)
      _ = allocatedParties += (name -> partyDetails)
    } yield partyDetails.party)
  }

  override def listKnownParties()(implicit ec: ExecutionContext, mat: Materializer) = {
    val ledgerParties = getLedgerParties
      .map(p => (p -> PartyDetails(party = p, displayName = None, isLocal = true)))
      .toMap
    Future.successful((ledgerParties ++ allocatedParties).values.toList)
  }

  override def getStaticTime()(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer): Future[Time.Timestamp] = {
    Future.successful(scenarioRunner.ledger.currentTime)
  }

  override def setStaticTime(time: Time.Timestamp)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer): Future[Unit] = {
    val diff = time.micros - scenarioRunner.ledger.currentTime.micros
    // ScenarioLedger only provides pass, so we have to calculate the diff.
    // Note that ScenarioLedger supports going backwards in time.
    scenarioRunner.ledger = scenarioRunner.ledger.passTime(diff)
    Future.unit
  }

  override def tracelogIterator = traceLog.iterator
  override def clearTracelog = traceLog.clear
}

// Current limitations and issues when running DAML script over the JSON API:
// 1. Multi-command submissions are not supported. This is simply not possible until
//    we have an endpoint for this in the JSON API.
// 2. This is the biggest issue imho: parties are kind of a mess. `submit` and `query` pretend
//    that you can choose the party you submitting commands as. However, this is not the case
//    for the JSON API since it always infers the party from the JWT (which also means it does
//    not support multi-party tokens). We add a validation step to `submit` and `query` that
//    errors out if the token party does not match the party pased as an argument.

class JsonLedgerClient(
    uri: Uri,
    token: Jwt,
    envIface: EnvironmentInterface,
    actorSystem: ActorSystem)
    extends ScriptLedgerClient {
  import JsonLedgerClient.JsonProtocol._
  import JsonLedgerClient._

  private val decodedJwt = JwtDecoder.decode(token) match {
    case -\/(e) => throw new IllegalArgumentException(e.toString)
    case \/-(a) => a
  }
  private[script] val tokenPayload: AuthServiceJWTPayload =
    AuthServiceJWTCodec.readFromString(decodedJwt.payload) match {
      case Failure(e) => throw e
      case Success(s) => s
    }

  implicit val system = actorSystem
  implicit val executionContext = system.dispatcher

  private def damlLfTypeLookup(id: Identifier) =
    envIface.typeDecls.get(id).map(_.`type`)

  case class FailedJsonApiRequest(
      path: Path,
      reqBody: Option[JsValue],
      respStatus: StatusCode,
      errors: List[String])
      extends RuntimeException(
        s"Request to $path with ${reqBody.map(_.compactPrint)} failed with status $respStatus: $errors")

  def request[A, B](path: Path, a: A)(
      implicit wa: JsonWriter[A],
      rb: JsonReader[B]): Future[Response[B]] = {
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri = uri.withPath(path),
      entity = HttpEntity(
        ContentTypes.`application/json`,
        a.toJson.compactPrint
      ),
      headers = List(Authorization(OAuth2BearerToken(token.value)))
    )
    Http().singleRequest(req).flatMap(resp => Unmarshal(resp.entity).to[Response[B]])
  }

  def request[A](path: Path)(implicit ra: JsonReader[A]): Future[Response[A]] = {
    val req = HttpRequest(
      method = HttpMethods.GET,
      uri = uri.withPath(path),
      headers = List(Authorization(OAuth2BearerToken(token.value)))
    )
    Http().singleRequest(req).flatMap(resp => Unmarshal(resp.entity).to[Response[A]])
  }

  def requestSuccess[A, B](path: Path, a: A)(
      implicit wa: JsonWriter[A],
      rb: JsonReader[B]): Future[B] =
    request[A, B](path, a).flatMap {
      case ErrorResponse(errors, status) =>
        Future.failed(FailedJsonApiRequest(path, Some(a.toJson), status, errors))
      case SuccessResponse(result, _) => Future.successful(result)
    }

  def requestSuccess[A](path: Path)(implicit rb: JsonReader[A]): Future[A] =
    request[A](path).flatMap {
      case ErrorResponse(errors, status) =>
        Future.failed(FailedJsonApiRequest(path, None, status, errors))
      case SuccessResponse(result, _) => Future.successful(result)
    }

  override def query(parties: OneAnd[Set, Ref.Party], templateId: Identifier)(
      implicit ec: ExecutionContext,
      mat: Materializer) = {
    for {
      () <- validateTokenParties(parties, "query")
      queryResponse <- requestSuccess[QueryArgs, QueryResponse](
        uri.path./("v1")./("query"),
        QueryArgs(templateId))
    } yield {
      val ctx = templateId.qualifiedName
      val ifaceType = Converter.toIfaceType(ctx, TTyCon(templateId)).right.get
      val parsedResults = queryResponse.results.map(r => {
        val payload = r.payload.convertTo[Value[ContractId]](
          LfValueCodec.apiValueJsonReader(ifaceType, damlLfTypeLookup(_)))
        val cid = ContractId.assertFromString(r.contractId)
        ScriptLedgerClient.ActiveContract(templateId, cid, payload)
      })
      parsedResults
    }
  }
  override def queryContractId(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
      cid: ContractId)(implicit ec: ExecutionContext, mat: Materializer) = {
    for {
      () <- validateTokenParties(parties, "queryContractId")
      fetchResponse <- requestSuccess[FetchArgs, FetchResponse](
        uri.path./("v1")./("fetch"),
        FetchArgs(cid))
    } yield {
      val ctx = templateId.qualifiedName
      val ifaceType = Converter.toIfaceType(ctx, TTyCon(templateId)).right.get
      fetchResponse.result.map(r => {
        val payload = r.payload.convertTo[Value[ContractId]](
          LfValueCodec.apiValueJsonReader(ifaceType, damlLfTypeLookup(_)))
        val cid = ContractId.assertFromString(r.contractId)
        ScriptLedgerClient.ActiveContract(templateId, cid, payload)
      })
    }
  }
  override def queryContractKey(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
      key: SValue)(implicit ec: ExecutionContext, mat: Materializer) = {
    for {
      _ <- validateTokenParties(parties, "queryContractKey")
      fetchResponse <- requestSuccess[FetchKeyArgs, FetchResponse](
        uri.path./("v1")./("fetch"),
        FetchKeyArgs(templateId, key.toValue))
    } yield {
      val ctx = templateId.qualifiedName
      val ifaceType = Converter.toIfaceType(ctx, TTyCon(templateId)).right.get
      fetchResponse.result.map(r => {
        val payload = r.payload.convertTo[Value[ContractId]](
          LfValueCodec.apiValueJsonReader(ifaceType, damlLfTypeLookup(_)))
        val cid = ContractId.assertFromString(r.contractId)
        ScriptLedgerClient.ActiveContract(templateId, cid, payload)
      })
    }
  }
  override def submit(
      party: Ref.Party,
      commands: List[command.Command],
      optLocation: Option[Location])(implicit ec: ExecutionContext, mat: Materializer)
    : Future[Either[StatusRuntimeException, Seq[ScriptLedgerClient.CommandResult]]] = {
    for {
      () <- validateTokenParties(OneAnd(party, Set()), "submit a command")
      result <- commands match {
        case Nil => Future { Right(List()) }
        case cmd :: Nil =>
          cmd match {
            case command.CreateCommand(tplId, argument) =>
              create(tplId, argument)
            case command.ExerciseCommand(tplId, cid, choice, argument) =>
              exercise(tplId, cid, choice, argument)
            case command.ExerciseByKeyCommand(tplId, key, choice, argument) =>
              exerciseByKey(tplId, key, choice, argument)
            case command.CreateAndExerciseCommand(tplId, template, choice, argument) =>
              createAndExercise(tplId, template, choice, argument)
          }
        case _ =>
          Future.failed(
            new RuntimeException(
              "Multi-command submissions are not supported by the HTTP JSON API."))
      }
    } yield result
  }
  override def submitMustFail(
      party: Ref.Party,
      commands: List[command.Command],
      optLocation: Option[Location])(implicit ec: ExecutionContext, mat: Materializer) = {
    submit(party, commands, optLocation).map({
      case Right(_) => Left(())
      case Left(_) => Right(())
    })
  }
  override def allocateParty(partyIdHint: String, displayName: String)(
      implicit ec: ExecutionContext,
      mat: Materializer) = {
    for {
      response <- requestSuccess[AllocatePartyArgs, AllocatePartyResponse](
        uri.path./("v1")./("parties")./("allocate"),
        AllocatePartyArgs(partyIdHint, displayName))
    } yield {
      response.identifier
    }
  }

  override def listKnownParties()(implicit ec: ExecutionContext, mat: Materializer) = {
    requestSuccess[List[PartyDetails]](uri.path./("v1")./("parties"))
  }

  override def getStaticTime()(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer): Future[Time.Timestamp] = {
    // There is no time service in the JSON API so we default to the Unix epoch.
    Future { Time.Timestamp.assertFromInstant(Instant.EPOCH) }
  }

  override def setStaticTime(time: Time.Timestamp)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer): Future[Unit] = {
    // No time service in the JSON API
    Future.failed(
      new RuntimeException("setTime is not supported when running DAML Script over the JSON API."))
  }

  // Check that the parties in the token are a superset of the given parties.
  private def validateTokenParties(parties: OneAnd[Set, Ref.Party], what: String): Future[Unit] = {
    import scalaz.std.string._
    val tokenParties = Set(tokenPayload.readAs ++ tokenPayload.actAs: _*)
    // First check is just for a nicer error message and would be covered by the second
    if (tokenParties.isEmpty) {
      Future.failed(
        new RuntimeException(
          s"Tried to $what as ${parties.toList.mkString(" ")} but token contains no parties."))
    } else if (tokenParties === parties.toSet.toSet[String]) {
      Future.unit
    } else {
      Future.failed(new RuntimeException(s"Tried to $what as ${parties.toList
        .mkString(" ")} but token provides claims for ${tokenParties.mkString(" ")}"))
    }
  }

  private def create(tplId: Identifier, argument: Value[ContractId])
    : Future[Either[StatusRuntimeException, List[ScriptLedgerClient.CreateResult]]] = {
    val jsonArgument = LfValueCodec.apiValueToJsValue(argument)
    commandRequest[CreateArgs, CreateResponse]("create", CreateArgs(tplId, jsonArgument))
      .map(_.map {
        case CreateResponse(cid) =>
          List(ScriptLedgerClient.CreateResult(ContractId.assertFromString(cid)))
      })
  }

  private def exercise(
      tplId: Identifier,
      contractId: ContractId,
      choice: ChoiceName,
      argument: Value[ContractId])
    : Future[Either[StatusRuntimeException, List[ScriptLedgerClient.ExerciseResult]]] = {
    val choiceDef = envIface
      .typeDecls(tplId)
      .asInstanceOf[InterfaceType.Template]
      .template
      .choices(choice)
    val jsonArgument = LfValueCodec.apiValueToJsValue(argument)
    commandRequest[ExerciseArgs, ExerciseResponse](
      "exercise",
      ExerciseArgs(tplId, contractId, choice, jsonArgument))
      .map(_.map {
        case ExerciseResponse(result) =>
          List(
            ScriptLedgerClient.ExerciseResult(
              tplId,
              choice,
              result.convertTo[Value[ContractId]](
                LfValueCodec.apiValueJsonReader(choiceDef.returnType, damlLfTypeLookup(_)))))
      })
  }

  private def exerciseByKey(
      tplId: Identifier,
      key: Value[ContractId],
      choice: ChoiceName,
      argument: Value[ContractId])
    : Future[Either[StatusRuntimeException, List[ScriptLedgerClient.ExerciseResult]]] = {
    val choiceDef = envIface
      .typeDecls(tplId)
      .asInstanceOf[InterfaceType.Template]
      .template
      .choices(choice)
    val jsonKey = LfValueCodec.apiValueToJsValue(key)
    val jsonArgument = LfValueCodec.apiValueToJsValue(argument)
    commandRequest[ExerciseByKeyArgs, ExerciseResponse](
      "exercise",
      JsonLedgerClient
        .ExerciseByKeyArgs(tplId, jsonKey, choice, jsonArgument)).map(_.map {
      case ExerciseResponse(result) =>
        List(
          ScriptLedgerClient.ExerciseResult(
            tplId,
            choice,
            result.convertTo[Value[ContractId]](
              LfValueCodec.apiValueJsonReader(choiceDef.returnType, damlLfTypeLookup(_)))))
    })
  }

  private def createAndExercise(
      tplId: Identifier,
      template: Value[ContractId],
      choice: ChoiceName,
      argument: Value[ContractId])
    : Future[Either[StatusRuntimeException, List[ScriptLedgerClient.CommandResult]]] = {
    val choiceDef = envIface
      .typeDecls(tplId)
      .asInstanceOf[InterfaceType.Template]
      .template
      .choices(choice)
    val jsonTemplate = LfValueCodec.apiValueToJsValue(template)
    val jsonArgument = LfValueCodec.apiValueToJsValue(argument)
    commandRequest[CreateAndExerciseArgs, CreateAndExerciseResponse](
      "create-and-exercise",
      JsonLedgerClient
        .CreateAndExerciseArgs(tplId, jsonTemplate, choice, jsonArgument))
      .map(_.map {
        case CreateAndExerciseResponse(cid, result) =>
          List(
            ScriptLedgerClient
              .CreateResult(ContractId.assertFromString(cid)): ScriptLedgerClient.CommandResult,
            ScriptLedgerClient.ExerciseResult(
              tplId,
              choice,
              result.convertTo[Value[ContractId]](
                LfValueCodec.apiValueJsonReader(choiceDef.returnType, damlLfTypeLookup(_))))
          )
      })
  }

  def commandRequest[In, Out](endpoint: String, argument: In)(
      implicit argumentWriter: JsonWriter[In],
      outputReader: RootJsonReader[Out]): Future[Either[StatusRuntimeException, Out]] = {
    request[In, Out](uri.path./("v1")./(endpoint), argument).flatMap {
      case ErrorResponse(errors, status) if status == StatusCodes.InternalServerError =>
        // TODO (MK) Using a grpc exception here doesn’t make that much sense.
        // We should refactor this to provide something more general.
        Future.successful(
          Left(new StatusRuntimeException(Status.UNKNOWN.withDescription(errors.toString))))
      case ErrorResponse(errors, status) =>
        // A non-500 failure is something like invalid JSON or “cannot resolve template ID”.
        // We don’t want to treat that failures as ones that can be caught
        // via `submitMustFail` so fail hard.
        Future.failed(
          new FailedJsonApiRequest(
            uri.path./("v1")./(endpoint),
            Some(argument.toJson),
            status,
            errors))
      case SuccessResponse(result, _) => Future.successful(Right(result))
    }
  }

  override def tracelogIterator = Iterator.empty
  override def clearTracelog = ()
}

object JsonLedgerClient {
  sealed trait Response[A] {
    def status: StatusCode
  }
  final case class ErrorResponse[A](errors: List[String], status: StatusCode) extends Response[A]
  final case class SuccessResponse[A](result: A, status: StatusCode) extends Response[A]

  final case class QueryArgs(templateId: Identifier)
  final case class QueryResponse(results: List[ActiveContract])
  final case class ActiveContract(contractId: String, payload: JsValue)
  final case class FetchArgs(contractId: ContractId)
  final case class FetchKeyArgs(templateId: Identifier, key: Value[ContractId])
  final case class FetchResponse(result: Option[ActiveContract])

  final case class CreateArgs(templateId: Identifier, payload: JsValue)
  final case class CreateResponse(contractId: String)

  final case class ExerciseArgs(
      templateId: Identifier,
      contractId: ContractId,
      choice: ChoiceName,
      argument: JsValue)
  final case class ExerciseResponse(result: JsValue)

  final case class ExerciseByKeyArgs(
      templateId: Identifier,
      key: JsValue,
      choice: ChoiceName,
      argument: JsValue)

  final case class CreateAndExerciseArgs(
      templateId: Identifier,
      payload: JsValue,
      choice: ChoiceName,
      argument: JsValue)
  final case class CreateAndExerciseResponse(contractId: String, result: JsValue)

  final case class AllocatePartyArgs(
      identifierHint: String,
      displayName: String
  )
  final case class AllocatePartyResponse(identifier: Ref.Party)

  object JsonProtocol extends SprayJsonSupport with DefaultJsonProtocol {
    implicit def optionReader[A: JsonReader]: JsonReader[Option[A]] =
      v =>
        v match {
          case JsNull => None
          case _ => Some(v.convertTo[A])
      }
    implicit def listReader[A: JsonReader]: JsonReader[List[A]] =
      v =>
        v match {
          case JsArray(xs) => xs.toList.map(_.convertTo[A])
          case _ => deserializationError(s"Expected JsArray but got $v")
      }
    implicit def responseReader[A: JsonReader]: RootJsonReader[Response[A]] = v => {
      implicit val statusCodeReader: JsonReader[StatusCode] = v =>
        v match {
          case JsNumber(value) => StatusCode.int2StatusCode(value.toIntExact)
          case _ => deserializationError("Expected status code")
      }
      val obj = v.asJsObject
      (obj.fields.get("status"), obj.fields.get("errors"), obj.fields.get("result")) match {
        case (Some(status), Some(err), None) =>
          ErrorResponse(
            err.convertTo[List[String]](DefaultJsonProtocol.listFormat),
            status.convertTo[StatusCode])
        case (Some(status), _, Some(res)) =>
          SuccessResponse(res.convertTo[A], status.convertTo[StatusCode])
        case _ => deserializationError("Expected status and either errors or result field")
      }
    }

    implicit val partyReader: JsonReader[Ref.Party] = v =>
      v match {
        case JsString(s) => Ref.Party.fromString(s).fold(deserializationError(_), identity)
        case _ => deserializationError(s"Expected Party but got $v")
    }
    implicit val partyDetailsReader: JsonReader[PartyDetails] = v => {
      val o = v.asJsObject
      (o.fields.get("identifier"), o.fields.get("displayName"), o.fields.get("isLocal")) match {
        case (Some(id), optName, Some(isLocal)) =>
          PartyDetails(
            id.convertTo[Party],
            optName.map(_.convertTo[String]),
            isLocal.convertTo[Boolean])
        case _ => deserializationError(s"Expected PartyDetails but got $v")
      }
    }

    implicit val choiceNameWriter: JsonWriter[ChoiceName] = choice => JsString(choice.toString)
    implicit val identifierWriter: JsonWriter[Identifier] = identifier =>
      JsString(
        identifier.packageId + ":" + identifier.qualifiedName.module.toString + ":" + identifier.qualifiedName.name.toString)

    implicit val queryWriter: JsonWriter[QueryArgs] = args =>
      JsObject("templateIds" -> JsArray(identifierWriter.write(args.templateId)))
    implicit val queryReader: RootJsonReader[QueryResponse] = v =>
      QueryResponse(v.convertTo[List[ActiveContract]])
    implicit val fetchWriter: JsonWriter[FetchArgs] = args =>
      JsObject("contractId" -> args.contractId.coid.toString.toJson)
    implicit val fetchKeyWriter: JsonWriter[FetchKeyArgs] = args =>
      JsObject(
        "templateId" -> args.templateId.toJson,
        "key" -> LfValueCodec.apiValueToJsValue(args.key))
    implicit val fetchReader: RootJsonReader[FetchResponse] = v =>
      FetchResponse(v.convertTo[Option[ActiveContract]])

    implicit val activeContractReader: RootJsonReader[ActiveContract] = v => {
      v.asJsObject.getFields("contractId", "payload") match {
        case Seq(JsString(s), v) => ActiveContract(s, v)
        case _ => deserializationError(s"Could not parse ActiveContract: $v")
      }
    }

    implicit val createWriter: JsonWriter[CreateArgs] = args =>
      JsObject("templateId" -> args.templateId.toJson, "payload" -> args.payload)
    implicit val createReader: RootJsonReader[CreateResponse] = v =>
      v.asJsObject.getFields("contractId") match {
        case Seq(JsString(cid)) => CreateResponse(cid)
        case _ => deserializationError(s"Could not parse CreateResponse: $v")
    }

    implicit val exerciseWriter: JsonWriter[ExerciseArgs] = args =>
      JsObject(
        "templateId" -> args.templateId.toJson,
        "contractId" -> args.contractId.coid.toString.toJson,
        "choice" -> args.choice.toJson,
        "argument" -> args.argument)
    implicit val exerciseByKeyWriter: JsonWriter[ExerciseByKeyArgs] = args =>
      JsObject(
        "templateId" -> args.templateId.toJson,
        "key" -> args.key,
        "choice" -> args.choice.toJson,
        "argument" -> args.argument)
    implicit val exerciseReader: RootJsonReader[ExerciseResponse] = v =>
      v.asJsObject.getFields("exerciseResult") match {
        case Seq(result) => ExerciseResponse(result)
        case _ => deserializationError(s"Could not parse ExerciseResponse: $v")
    }

    implicit val createAndExerciseWriter: JsonWriter[CreateAndExerciseArgs] = args =>
      JsObject(
        "templateId" -> args.templateId.toJson,
        "payload" -> args.payload,
        "choice" -> args.choice.toJson,
        "argument" -> args.argument)
    implicit val createAndExerciseReader: RootJsonReader[CreateAndExerciseResponse] = v =>
      v.asJsObject.getFields("exerciseResult", "events") match {
        case Seq(result, events) =>
          events match {
            case JsArray(Seq(event, _*)) =>
              event.asJsObject.getFields("created") match {
                case Seq(created) =>
                  created.asJsObject.getFields("contractId") match {
                    case Seq(JsString(cid)) => CreateAndExerciseResponse(cid, result)
                    case _ =>
                      deserializationError(s"Could not parse CreateAndExerciseResponse: $v")
                  }
                case _ => deserializationError(s"Could not parse CreateAndExerciseResponse: $v")
              }
            case _ => deserializationError(s"Could not parse CreateAndExerciseResponse: $v")
          }
        case _ => deserializationError(s"Could not parse CreateAndExerciseResponse: $v")
    }

    implicit val allocatePartyWriter: JsonFormat[AllocatePartyArgs] = jsonFormat2(AllocatePartyArgs)
    implicit val allocatePartyReader: RootJsonReader[AllocatePartyResponse] = v =>
      v.asJsObject.getFields("identifier") match {
        case Seq(id) => AllocatePartyResponse(id.convertTo[Party])
        case _ => deserializationError(s"Could not parse AllocatePartyResponse: $v")
    }
  }
}
