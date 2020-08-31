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
import akka.util.ByteString
import io.grpc.{Status, StatusRuntimeException}
import java.time.Instant
import java.util.UUID

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scalaz.{-\/, \/-}
import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import spray.json._

import com.daml.api.util.TimestampConversion
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.akka.ClientAdapter
import com.daml.lf.scenario.ScenarioLedger
import com.daml.lf.data.Ref._
import com.daml.lf.data.{Ref, ImmArray}
import com.daml.lf.data.{Time}
import com.daml.lf.iface.{EnvironmentInterface, InterfaceType}
import com.daml.lf.language.Ast._
import com.daml.lf.transaction.Node.{NodeCreate, NodeExercises}
import com.daml.lf.speedy.ScenarioRunner
import com.daml.lf.speedy.Speedy.Machine
import com.daml.lf.speedy.{PartialTransaction, SExpr, SValue}
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SResult._
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

// We have our own type for time modes since TimeProviderType
// allows for more stuff that doesn’t make sense in DAML Script.
sealed trait ScriptTimeMode

object ScriptTimeMode {
  final case object Static extends ScriptTimeMode
  final case object WallClock extends ScriptTimeMode
}

object ScriptLedgerClient {

  sealed trait Command
  final case class CreateCommand(templateId: Identifier, argument: SValue) extends Command
  final case class ExerciseCommand(
      templateId: Identifier,
      contractId: ContractId,
      choice: ChoiceName,
      argument: SValue)
      extends Command
  final case class ExerciseByKeyCommand(
      templateId: Identifier,
      key: SValue,
      choice: ChoiceName,
      argument: SValue)
      extends Command
  final case class CreateAndExerciseCommand(
      templateId: Identifier,
      template: SValue,
      choice: ChoiceName,
      argument: SValue)
      extends Command

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
  def query(party: SParty, templateId: Identifier)(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[Seq[ScriptLedgerClient.ActiveContract]]

  def submit(
      party: SParty,
      commands: List[ScriptLedgerClient.Command],
      optLocation: Option[Location])(implicit ec: ExecutionContext, mat: Materializer)
    : Future[Either[StatusRuntimeException, Seq[ScriptLedgerClient.CommandResult]]]

  def submitMustFail(
      party: SParty,
      commands: List[ScriptLedgerClient.Command],
      optLocation: Option[Location])(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[Either[Unit, Unit]]

  def allocateParty(partyIdHint: String, displayName: String)(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[SParty]

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
}

class GrpcLedgerClient(val grpcClient: LedgerClient, val applicationId: ApplicationId)
    extends ScriptLedgerClient {
  override def query(party: SParty, templateId: Identifier)(
      implicit ec: ExecutionContext,
      mat: Materializer) = {
    val filter = TransactionFilter(
      List((party.value, Filters(Some(InclusiveFilters(Seq(toApiIdentifier(templateId))))))).toMap)
    val acsResponses =
      grpcClient.activeContractSetClient
        .getActiveContracts(filter, verbose = true)
        .runWith(Sink.seq)
    acsResponses.map(acsPages =>
      acsPages.flatMap(page =>
        page.activeContracts.map(createdEvent => {
          val argument = ValueValidator.validateRecord(createdEvent.getCreateArguments) match {
            case Left(err) => throw new ConverterException(err.toString)
            case Right(argument) => argument
          }
          val cid =
            ContractId
              .fromString(createdEvent.contractId)
              .fold(
                err => throw new ConverterException(err),
                identity
              )
          ScriptLedgerClient.ActiveContract(templateId, cid, argument)
        })))
  }

  override def submit(
      party: SParty,
      commands: List[ScriptLedgerClient.Command],
      optLocation: Option[Location])(implicit ec: ExecutionContext, mat: Materializer) = {
    val ledgerCommands = commands.traverse(toCommand(_)) match {
      case Left(err) => throw new ConverterException(err)
      case Right(cmds) => cmds
    }
    val apiCommands = Commands(
      party = party.value,
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
      party: SParty,
      commands: List[ScriptLedgerClient.Command],
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
      .map(r => SParty(r.party))
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

  private def toCommand(command: ScriptLedgerClient.Command): Either[String, Command] =
    command match {
      case ScriptLedgerClient.CreateCommand(templateId, argument) =>
        for {
          arg <- lfValueToApiRecord(true, argument.toValue)
        } yield Command().withCreate(CreateCommand(Some(toApiIdentifier(templateId)), Some(arg)))
      case ScriptLedgerClient.ExerciseCommand(templateId, contractId, choice, argument) =>
        for {
          arg <- lfValueToApiValue(true, argument.toValue)
        } yield
          Command().withExercise(
            ExerciseCommand(Some(toApiIdentifier(templateId)), contractId.coid, choice, Some(arg)))
      case ScriptLedgerClient.ExerciseByKeyCommand(templateId, key, choice, argument) =>
        for {
          key <- lfValueToApiValue(true, key.toValue)
          argument <- lfValueToApiValue(true, argument.toValue)
        } yield
          Command().withExerciseByKey(
            ExerciseByKeyCommand(
              Some(toApiIdentifier(templateId)),
              Some(key),
              choice,
              Some(argument)))
      case ScriptLedgerClient.CreateAndExerciseCommand(templateId, template, choice, argument) =>
        for {
          template <- lfValueToApiRecord(true, template.toValue)
          argument <- lfValueToApiValue(true, argument.toValue)
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
}

// Client for the script service.
class IdeClient(val compiledPackages: CompiledPackages) extends ScriptLedgerClient {
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
    inputValueVersions = value.ValueVersions.DevOutputVersions,
    outputTransactionVersions = transaction.TransactionVersions.DevOutputVersions,
  )
  val scenarioRunner = ScenarioRunner(machine)
  private var allocatedParties: Map[String, PartyDetails] = Map()

  override def query(party: SParty, templateId: Identifier)(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[Seq[ScriptLedgerClient.ActiveContract]] = {
    val acs = scenarioRunner.ledger.query(
      view = ScenarioLedger.ParticipantView(party.value),
      effectiveAt = scenarioRunner.ledger.currentTime)
    // Filter to contracts of the given template id.
    val filtered = acs.collect {
      case (cid, Value.ContractInst(tpl, arg, _)) if tpl == templateId => (cid, arg)
    }
    Future.successful(filtered.map {
      case (cid, c) => ScriptLedgerClient.ActiveContract(templateId, cid, c.value)
    })
  }

  // Translate from a ledger command to an Update expression
  // corresponding to the same command.
  private def translateCommand(cmd: ScriptLedgerClient.Command): speedy.Command = {
    // Ledger commands like create or exercise look pretty complicated in
    // SExpr. Therefore we express them in the high-level AST and compile them
    // to a function that we apply to the arguments.
    cmd match {
      case ScriptLedgerClient.CreateCommand(tplId, arg) =>
        speedy.Command.Create(tplId, arg)
      case ScriptLedgerClient.ExerciseCommand(tplId, cid, choice, arg) =>
        speedy.Command.Exercise(tplId, SContractId(cid), choice, arg)
      case ScriptLedgerClient.CreateAndExerciseCommand(tplId, tpl, choice, arg) =>
        speedy.Command.CreateAndExercise(tplId, tpl, choice, arg)
      case ScriptLedgerClient.ExerciseByKeyCommand(tplId, key, choice, arg) =>
        speedy.Command.ExerciseByKey(tplId, key, choice, arg)
    }
  }

  // Translate a list of commands submitted by the given party
  // into an expression corresponding to a scenario commit of the same
  // commands of type `Scenario ()`.
  private def translateCommands(commands: List[ScriptLedgerClient.Command]): SExpr = {
    val cmds: ImmArray[speedy.Command] = ImmArray(commands.map(translateCommand(_)))
    compiledPackages.compiler.unsafeCompile(cmds)
  }

  // unsafe version of submit that does not clear the commit.
  private def unsafeSubmit(
      party: SParty,
      commands: List[ScriptLedgerClient.Command],
      optLocation: Option[Location])(implicit ec: ExecutionContext)
    : Future[Either[StatusRuntimeException, Seq[ScriptLedgerClient.CommandResult]]] = Future {
    // Clear state at the beginning like in SBSBeginCommit for scenarios.
    machine.commitLocation = optLocation
    machine.returnValue = null
    machine.localContracts = Map.empty
    machine.globalDiscriminators = Set.empty
    val translated = translateCommands(commands)
    machine.setExpressionToEvaluate(SEApp(translated, Array(SEValue.Token)))
    machine.committers = Set(party.value)
    var result: Seq[ScriptLedgerClient.CommandResult] = null
    while (result == null) {
      machine.run() match {
        case SResultNeedContract(coid, tid @ _, committers, cbMissing, cbPresent) =>
          scenarioRunner.lookupContract(coid, committers, cbMissing, cbPresent).toTry.get
        case SResultNeedKey(keyWithMaintainers, committers, cb) =>
          scenarioRunner.lookupKey(keyWithMaintainers.globalKey, committers, cb).toTry.get
        case SResultFinalValue(SUnit) =>
          machine.ptx.finish(
            machine.outputTransactionVersions,
            machine.compiledPackages.packageLanguageVersion) match {
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
                committer = party.value,
                effectiveAt = scenarioRunner.ledger.currentTime,
                optLocation = machine.commitLocation,
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
            case err: PartialTransaction.SerializationError =>
              throw new RuntimeException(err.prettyMessage)
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
      party: SParty,
      commands: List[ScriptLedgerClient.Command],
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
      party: SParty,
      commands: List[ScriptLedgerClient.Command],
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
    } yield SParty(partyDetails.party))
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

  override def query(party: SParty, templateId: Identifier)(
      implicit ec: ExecutionContext,
      mat: Materializer) = {
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri = uri.withPath(uri.path./("v1")./("query")),
      entity = HttpEntity(
        ContentTypes.`application/json`,
        JsonLedgerClient.QueryArgs(templateId).toJson.prettyPrint),
      headers = List(Authorization(OAuth2BearerToken(token.value)))
    )
    for {
      () <- validateTokenParty(party, "query")
      resp <- Http().singleRequest(req)
      queryResponse <- if (resp.status.isSuccess) {
        Unmarshal(resp.entity).to[JsonLedgerClient.QueryResponse]
      } else {
        getResponseDataBytes(resp).flatMap {
          case body => Future.failed(new RuntimeException(s"Failed to query ledger: $resp, $body"))
        }
      }
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
  override def submit(
      party: SParty,
      commands: List[ScriptLedgerClient.Command],
      optLocation: Option[Location])(implicit ec: ExecutionContext, mat: Materializer)
    : Future[Either[StatusRuntimeException, Seq[ScriptLedgerClient.CommandResult]]] = {
    for {
      () <- validateTokenParty(party, "submit a command")
      result <- commands match {
        case Nil => Future { Right(List()) }
        case command :: Nil =>
          command match {
            case ScriptLedgerClient.CreateCommand(tplId, argument) =>
              create(tplId, argument)
            case ScriptLedgerClient.ExerciseCommand(tplId, cid, choice, argument) =>
              exercise(tplId, cid, choice, argument)
            case ScriptLedgerClient.ExerciseByKeyCommand(tplId, key, choice, argument) =>
              exerciseByKey(tplId, key, choice, argument)
            case ScriptLedgerClient.CreateAndExerciseCommand(tplId, template, choice, argument) =>
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
      party: SParty,
      commands: List[ScriptLedgerClient.Command],
      optLocation: Option[Location])(implicit ec: ExecutionContext, mat: Materializer) = {
    submit(party, commands, optLocation).map({
      case Right(_) => Left(())
      case Left(_) => Right(())
    })
  }
  override def allocateParty(partyIdHint: String, displayName: String)(
      implicit ec: ExecutionContext,
      mat: Materializer) = {
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri = uri.withPath(uri.path./("v1")./("parties")./("allocate")),
      entity = HttpEntity(
        ContentTypes.`application/json`,
        JsonLedgerClient.AllocatePartyArgs(partyIdHint, displayName).toJson.prettyPrint),
      headers = List(Authorization(OAuth2BearerToken(token.value)))
    )
    for {
      resp <- Http().singleRequest(req)
      response <- if (resp.status.isSuccess) {
        Unmarshal(resp.entity).to[JsonLedgerClient.AllocatePartyResponse]
      } else {
        getResponseDataBytes(resp).flatMap {
          case body =>
            Future.failed(new RuntimeException(s"Failed to allocate party: $resp, $body"))
        }
      }
    } yield {
      SParty(response.identifier)
    }
  }

  override def listKnownParties()(implicit ec: ExecutionContext, mat: Materializer) = {
    Future.failed(
      new RuntimeException(
        s"listKnownParties is not supported when running DAML Script over the JSON API"))
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

  // Check that the party in the token matches the given party.
  private def validateTokenParty(party: SParty, what: String): Future[Unit] = {
    tokenPayload.party match {
      case None =>
        Future.failed(new RuntimeException(
          s"Tried to $what as ${party.value} but token does not provide a unique party identifier"))
      case Some(tokenParty) if (!(tokenParty == party.value)) =>
        Future.failed(
          new RuntimeException(
            s"Tried to $what as ${party.value} but token is only valid for $tokenParty"))
      case _ => Future.unit
    }
  }

  private def create(tplId: Identifier, argument: SValue)
    : Future[Either[StatusRuntimeException, List[ScriptLedgerClient.CreateResult]]] = {
    val jsonArgument = LfValueCodec.apiValueToJsValue(argument.toValue)
    commandRequest[JsonLedgerClient.CreateArgs, JsonLedgerClient.CreateResponse](
      "create",
      JsonLedgerClient.CreateArgs(tplId, jsonArgument))
      .map(_.map {
        case JsonLedgerClient.CreateResponse(cid) =>
          List(ScriptLedgerClient.CreateResult(ContractId.assertFromString(cid)))
      })
  }

  private def exercise(
      tplId: Identifier,
      contractId: ContractId,
      choice: ChoiceName,
      argument: SValue)
    : Future[Either[StatusRuntimeException, List[ScriptLedgerClient.ExerciseResult]]] = {
    val choiceDef = envIface
      .typeDecls(tplId)
      .asInstanceOf[InterfaceType.Template]
      .template
      .choices(choice)
    val jsonArgument = LfValueCodec.apiValueToJsValue(argument.toValue)
    commandRequest[JsonLedgerClient.ExerciseArgs, JsonLedgerClient.ExerciseResponse](
      "exercise",
      JsonLedgerClient.ExerciseArgs(tplId, contractId, choice, jsonArgument))
      .map(_.map {
        case JsonLedgerClient.ExerciseResponse(result) =>
          List(
            ScriptLedgerClient.ExerciseResult(
              tplId,
              choice,
              result.convertTo[Value[ContractId]](
                LfValueCodec.apiValueJsonReader(choiceDef.returnType, damlLfTypeLookup(_)))))
      })
  }

  private def exerciseByKey(tplId: Identifier, key: SValue, choice: ChoiceName, argument: SValue)
    : Future[Either[StatusRuntimeException, List[ScriptLedgerClient.ExerciseResult]]] = {
    val choiceDef = envIface
      .typeDecls(tplId)
      .asInstanceOf[InterfaceType.Template]
      .template
      .choices(choice)
    val jsonKey = LfValueCodec.apiValueToJsValue(key.toValue)
    val jsonArgument = LfValueCodec.apiValueToJsValue(argument.toValue)
    commandRequest[JsonLedgerClient.ExerciseByKeyArgs, JsonLedgerClient.ExerciseResponse](
      "exercise",
      JsonLedgerClient
        .ExerciseByKeyArgs(tplId, jsonKey, choice, jsonArgument)).map(_.map {
      case JsonLedgerClient.ExerciseResponse(result) =>
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
      template: SValue,
      choice: ChoiceName,
      argument: SValue)
    : Future[Either[StatusRuntimeException, List[ScriptLedgerClient.CommandResult]]] = {
    val choiceDef = envIface
      .typeDecls(tplId)
      .asInstanceOf[InterfaceType.Template]
      .template
      .choices(choice)
    val jsonTemplate = LfValueCodec.apiValueToJsValue(template.toValue)
    val jsonArgument = LfValueCodec.apiValueToJsValue(argument.toValue)
    commandRequest[
      JsonLedgerClient.CreateAndExerciseArgs,
      JsonLedgerClient.CreateAndExerciseResponse](
      "create-and-exercise",
      JsonLedgerClient
        .CreateAndExerciseArgs(tplId, jsonTemplate, choice, jsonArgument))
      .map(_.map {
        case JsonLedgerClient.CreateAndExerciseResponse(cid, result) =>
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

  def getResponseDataBytes(resp: HttpResponse)(implicit mat: Materializer): Future[String] = {
    val fb = resp.entity.dataBytes.runFold(ByteString.empty)((b, a) => b ++ a).map(_.utf8String)
    fb
  }

  def commandRequest[In, Out](endpoint: String, argument: In)(
      implicit argumentWriter: JsonWriter[In],
      outputReader: RootJsonReader[Out]): Future[Either[StatusRuntimeException, Out]] = {
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri = uri.withPath(uri.path./("v1")./(endpoint)),
      entity = HttpEntity(ContentTypes.`application/json`, argument.toJson.prettyPrint),
      headers = List(Authorization(OAuth2BearerToken(token.value)))
    )
    Http().singleRequest(req).flatMap { resp =>
      if (resp.status.isSuccess) {
        Unmarshal(resp.entity).to[Out].map(Right(_))
      } else if (resp.status == StatusCodes.InternalServerError) {
        // TODO (MK) Using a grpc exception here doesn’t make that much sense.
        // We should refactor this to provide something more general.
        getResponseDataBytes(resp).map(description =>
          Left(new StatusRuntimeException(Status.UNKNOWN.withDescription(description))))
      } else {
        // A non-500 failure is something like invalid JSON or “cannot resolve template ID”.
        // We don’t want to treat that failures as ones that can be caught
        // via `submitMustFail` so fail hard.
        getResponseDataBytes(resp).flatMap(
          description =>
            Future.failed(
              new RuntimeException(s"Request failed: $description, status code: ${resp.status}")))
      }
    }
  }
}

object JsonLedgerClient {
  final case class QueryArgs(templateId: Identifier)
  final case class QueryResponse(results: List[ActiveContract])
  final case class ActiveContract(contractId: String, payload: JsValue)

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
    implicit val choiceNameWriter: JsonWriter[ChoiceName] = choice => JsString(choice.toString)
    implicit val identifierWriter: JsonWriter[Identifier] = identifier =>
      JsString(
        identifier.packageId + ":" + identifier.qualifiedName.module.toString + ":" + identifier.qualifiedName.name.toString)

    implicit val queryWriter: JsonWriter[QueryArgs] = args =>
      JsObject("templateIds" -> JsArray(identifierWriter.write(args.templateId)))
    implicit val queryReader: RootJsonReader[QueryResponse] = v => {
      v.asJsObject.getFields("result") match {
        case Seq(JsArray(results)) => QueryResponse(results.toList.map(_.convertTo[ActiveContract]))
        case _ => deserializationError(s"Could not parse QueryResponse: $v")
      }
    }
    implicit val activeContractReader: RootJsonReader[ActiveContract] = v => {
      v.asJsObject.getFields("contractId", "payload") match {
        case Seq(JsString(s), v) => ActiveContract(s, v)
        case _ => deserializationError(s"Could not parse ActiveContract: $v")
      }
    }

    implicit val createWriter: JsonWriter[CreateArgs] = args =>
      JsObject("templateId" -> args.templateId.toJson, "payload" -> args.payload)
    implicit val createReader: RootJsonReader[CreateResponse] = v => {
      v.asJsObject.getFields("result") match {
        case Seq(result) =>
          result.asJsObject.getFields("contractId") match {
            case Seq(JsString(cid)) => CreateResponse(cid)
            case _ => deserializationError(s"Could not parse CreateResponse: $v")
          }
        case _ => deserializationError(s"Could not parse CreateResponse: $v")
      }
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
    implicit val exerciseReader: RootJsonReader[ExerciseResponse] = v => {
      v.asJsObject.getFields("result") match {
        case Seq(result) =>
          result.asJsObject.getFields("exerciseResult") match {
            case Seq(result) => ExerciseResponse(result)
            case _ => deserializationError(s"Could not parse ExerciseResponse: $v")
          }
        case _ => deserializationError(s"Could not parse ExerciseResponse: $v")
      }
    }

    implicit val createAndExerciseWriter: JsonWriter[CreateAndExerciseArgs] = args =>
      JsObject(
        "templateId" -> args.templateId.toJson,
        "payload" -> args.payload,
        "choice" -> args.choice.toJson,
        "argument" -> args.argument)
    implicit val createAndExerciseReader: RootJsonReader[CreateAndExerciseResponse] = v => {
      v.asJsObject.getFields("result") match {
        case Seq(result) =>
          result.asJsObject.getFields("exerciseResult", "events") match {
            case Seq(result, events) =>
              events match {
                case JsArray(events) if events.size >= 1 =>
                  events.head.asJsObject.getFields("created") match {
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
        case _ => deserializationError(s"Could not parse CreateAndExerciseResponse: $v")
      }
    }

    implicit val allocatePartyWriter: JsonFormat[AllocatePartyArgs] = jsonFormat2(AllocatePartyArgs)
    implicit val allocatePartyReader: RootJsonReader[AllocatePartyResponse] = v =>
      v.asJsObject.getFields("result") match {
        case Seq(result) =>
          result.asJsObject.getFields("identifier") match {
            case Seq(JsString(identifier)) =>
              AllocatePartyResponse(Ref.Party.assertFromString(identifier))
            case _ => deserializationError(s"Could not parse AllocatePartyResponse: $v")
          }
        case _ => deserializationError(s"Could not parse AllocatePartyResponse: $v")
    }
  }
}
