// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.v2.ledgerinteraction
package grpcLedgerClient

import java.util.UUID

import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import com.daml.api.util.TimestampConversion
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands._
import com.daml.ledger.api.v1.event.{CreatedEvent, InterfaceView}
import com.daml.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeServiceStub
import com.daml.ledger.api.v1.testing.time_service.{GetTimeRequest, SetTimeRequest, TimeServiceGrpc}
import com.daml.ledger.api.v1.transaction.TreeEvent
import com.daml.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  InterfaceFilter,
  TransactionFilter,
}
import com.daml.ledger.api.v1.{value => api}
import com.daml.ledger.api.validation.NoLoggingValueValidator
import com.daml.ledger.client.LedgerClient
import com.daml.lf.command
import com.daml.lf.data.Ref._
import com.daml.lf.data.{Bytes, Ref, Time}
import com.daml.lf.engine.script.v2.Converter
import com.daml.lf.language.Ast
import com.daml.lf.speedy.{SValue, svalue}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.platform.participant.util.LfEngineToApi.{
  lfValueToApiRecord,
  lfValueToApiValue,
  toApiIdentifier,
}
import com.daml.script.converter.ConverterException
import io.grpc.{Status, StatusRuntimeException}
import scalaz.OneAnd
import scalaz.OneAnd._
import scalaz.std.either._
import scalaz.std.list._
import scalaz.std.set._
import scalaz.syntax.foldable._
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}

class GrpcLedgerClient(
    val grpcClient: LedgerClient,
    val applicationId: ApplicationId,
    val oAdminClient: Option[AdminLedgerClient],
    override val enableContractUpgrading: Boolean = false,
) extends ScriptLedgerClient {
  override val transport = "gRPC API"

  var providePackageId: Boolean = !enableContractUpgrading

  override def query(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Vector[ScriptLedgerClient.ActiveContract]] = {
    queryWithKey(parties, templateId).map(_.map(_._1))
  }

  // Omits the package id on an identifier if contract upgrades are enabled
  private def toApiIdentifierUpgrades(identifier: Identifier): api.Identifier = {
    val converted = toApiIdentifier(identifier)
    if (providePackageId) converted else converted.copy(packageId = "")
  }

  private def templateFilter(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
  ): TransactionFilter = {
    val filters = Filters(Some(InclusiveFilters(Seq(toApiIdentifierUpgrades(templateId)))))
    TransactionFilter(parties.toList.map(p => (p, filters)).toMap)
  }

  private def interfaceFilter(
      parties: OneAnd[Set, Ref.Party],
      interfaceId: Identifier,
  ): TransactionFilter = {
    val filters =
      Filters(
        Some(
          InclusiveFilters(
            List(),
            List(InterfaceFilter(Some(toApiIdentifier(interfaceId)), true)),
          )
        )
      )
    TransactionFilter(parties.toList.map(p => (p, filters)).toMap)
  }

  // Helper shared by query, queryContractId and queryContractKey
  private def queryWithKey(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Vector[(ScriptLedgerClient.ActiveContract, Option[Value])]] = {
    val filter = templateFilter(parties, templateId)
    val acsResponses =
      grpcClient.activeContractSetClient
        .getActiveContracts(filter, verbose = false)
        .runWith(Sink.seq)
    acsResponses.map(acsPages =>
      acsPages.toVector.flatMap(page =>
        page.activeContracts.toVector.map(createdEvent => {
          val argument =
            NoLoggingValueValidator.validateRecord(createdEvent.getCreateArguments) match {
              case Left(err) => throw new ConverterException(err.toString)
              case Right(argument) => argument
            }
          val key: Option[Value] = createdEvent.contractKey.map { key =>
            NoLoggingValueValidator.validateValue(key) match {
              case Left(err) => throw new ConverterException(err.toString)
              case Right(argument) => argument
            }
          }
          val cid =
            ContractId
              .fromString(createdEvent.contractId)
              .fold(
                err => throw new ConverterException(err),
                identity,
              )
          val blob =
            Bytes.fromByteString(createdEvent.createdEventBlob)
          (ScriptLedgerClient.ActiveContract(templateId, cid, argument, blob), key)
        })
      )
    )
  }

  override def queryContractId(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
      cid: ContractId,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Option[ScriptLedgerClient.ActiveContract]] = {
    // We cannot do better than a linear search over query here.
    for {
      activeContracts <- query(parties, templateId)
    } yield {
      activeContracts.find(c => c.contractId == cid)
    }
  }

  override def queryInterface(
      parties: OneAnd[Set, Ref.Party],
      interfaceId: Identifier,
      viewType: Ast.Type,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Seq[(ContractId, Option[Value])]] = {
    val filter = interfaceFilter(parties, interfaceId)
    val acsResponses =
      grpcClient.activeContractSetClient
        .getActiveContracts(filter, verbose = false)
        .runWith(Sink.seq)
    acsResponses.map { acsPages: Seq[GetActiveContractsResponse] =>
      acsPages.toVector.flatMap { page: GetActiveContractsResponse =>
        page.activeContracts.toVector.flatMap { createdEvent: CreatedEvent =>
          val cid =
            ContractId
              .fromString(createdEvent.contractId)
              .fold(
                err => throw new ConverterException(err),
                identity,
              )
          createdEvent.interfaceViews.map { iv: InterfaceView =>
            val viewValue: Value.ValueRecord =
              NoLoggingValueValidator.validateRecord(iv.getViewValue) match {
                case Left(err) => throw new ConverterException(err.toString)
                case Right(argument) => argument
              }
            // Because we filter for a specific interfaceId,
            // we will get at most one view for a given cid.
            (cid, if (viewValue.fields.isEmpty) None else Some(viewValue))
          }
        }
      }
    }
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
    for {
      activeViews <- queryInterface(parties, interfaceId, viewType)
    } yield {
      activeViews.collectFirst {
        case (k, Some(v)) if (k == cid) => v
      }
    }
  }

  // TODO (MK) https://github.com/digital-asset/daml/issues/11737
  private val catchableStatusCodes =
    Set(
      Status.Code.NOT_FOUND,
      Status.Code.INVALID_ARGUMENT,
      Status.Code.FAILED_PRECONDITION,
      Status.Code.ALREADY_EXISTS,
    )

  private def isSubmitMustFailError(status: StatusRuntimeException): Boolean = {
    val code = status.getStatus.getCode
    // We handle ABORTED for backwards compatibility with pre-1.18 error codes.
    catchableStatusCodes.contains(code) || code == Status.Code.ABORTED
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
    // We cannot do better than a linear search over query here.
    import scalaz.std.option._
    import scalaz.std.scalaFuture._
    import scalaz.std.vector._
    import scalaz.syntax.traverse._
    for {
      activeContracts <- queryWithKey(parties, templateId)
      speedyContracts <- activeContracts.traverse { case (t, kOpt) =>
        Converter.toFuture(kOpt.traverse(translateKey(templateId, _)).map(k => (t, k)))
      }
    } yield {
      // Note that the Equal instance on Value performs structural equality
      // and also compares optional field and constructor names and is
      // therefore not correct here.
      // Equality.areEqual corresponds to the Daml-LF value equality
      // which we want here.
      speedyContracts.collectFirst({ case (c, Some(k)) if svalue.Equality.areEqual(k, key) => c })
    }
  }

  override def trySubmit(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      disclosures: List[Bytes],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit ec: ExecutionContext, mat: Materializer) =
    internalSubmit(actAs, readAs, disclosures, commands)
      .map(Right(_))
      .recoverWith({ case s: StatusRuntimeException =>
        Future.successful(Left(GrpcErrorParser.convertStatusRuntimeException(s)))
      })

  override def trySubmitConcurrently(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      commandss: List[List[command.ApiCommand]],
      optLocation: Option[Location],
  )(implicit ec: ExecutionContext, mat: Materializer) =
    Future.traverse(commandss)(trySubmit(actAs, readAs, List.empty, _, optLocation))

  override def submit(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      disclosures: List[Bytes],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit ec: ExecutionContext, mat: Materializer) =
    internalSubmit(actAs, readAs, disclosures, commands)
      .map(Right(_))
      .recoverWith({
        case s: StatusRuntimeException if isSubmitMustFailError(s) =>
          Future.successful(Left(s))
      })

  def internalSubmit(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      disclosures: List[Bytes],
      commands: List[command.ApiCommand],
  )(implicit ec: ExecutionContext) = {
    import scalaz.syntax.traverse._
    val ledgerDisclosures =
      disclosures.map(b => DisclosedContract(createdEventBlob = b.toByteString))
    val ledgerCommands = commands.traverse(toCommand(_)) match {
      case Left(err) => throw new ConverterException(err)
      case Right(cmds) => cmds
    }
    // We need to remember the original package ID for each command result, so we can reapply them
    // after we get the results (for upgrades)
    val commandResultPackageIds = commands.flatMap(toCommandPackageIds(_))

    val apiCommands = Commands(
      party = actAs.head,
      actAs = actAs.toList,
      readAs = readAs.toList,
      commands = ledgerCommands,
      ledgerId = grpcClient.ledgerId.unwrap,
      applicationId = applicationId.unwrap,
      commandId = UUID.randomUUID.toString,
      disclosedContracts = ledgerDisclosures,
    )
    val request = SubmitAndWaitRequest(Some(apiCommands))
    val transactionTreeF = grpcClient.commandServiceClient
      .submitAndWaitForTransactionTree(request)

    transactionTreeF.map { transactionTree =>
      val events = transactionTree.getTransaction.rootEventIds
        .map(evId => transactionTree.getTransaction.eventsById(evId))
        .toList
      events.zip(commandResultPackageIds).traverse { case (event, intendedPackageId) =>
        fromTreeEvent(event, intendedPackageId)
      } match {
        case Left(err) => throw new ConverterException(err)
        case Right(results) => results
      }
    }
  }

  override def submitMustFail(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      disclosures: List[Bytes],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit ec: ExecutionContext, mat: Materializer) = {
    submit(actAs, readAs, disclosures, commands, optLocation).map({
      case Right(_) => Left(())
      case Left(_) => Right(())
    })
  }

  override def submitTree(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      disclosures: List[Bytes],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[ScriptLedgerClient.TransactionTree] = {
    import scalaz.std.list._
    import scalaz.syntax.traverse._
    for {
      _ <- Converter.noDisclosures(disclosures)
      ledgerCommands <- Converter.toFuture(commands.traverse(toCommand(_)))
      // We need to remember the original package ID for each command result, so we can reapply them
      // after we get the results (for upgrades)
      commandResultPackageIds = commands.flatMap(toCommandPackageIds(_))
      apiCommands = Commands(
        party = actAs.head,
        actAs = actAs.toList,
        readAs = readAs.toList,
        commands = ledgerCommands,
        ledgerId = grpcClient.ledgerId.unwrap,
        applicationId = applicationId.unwrap,
        commandId = UUID.randomUUID.toString,
      )
      request = SubmitAndWaitRequest(Some(apiCommands))
      resp <- grpcClient.commandServiceClient
        .submitAndWaitForTransactionTree(request)
      converted <- Converter.toFuture(
        Converter.fromTransactionTree(resp.getTransaction, commandResultPackageIds)
      )
    } yield converted
  }

  override def allocateParty(partyIdHint: String, displayName: String)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ) = {
    grpcClient.partyManagementClient
      .allocateParty(Some(partyIdHint), Some(displayName))
      .map(_.party)
  }

  override def listKnownParties()(implicit ec: ExecutionContext, mat: Materializer) = {
    grpcClient.partyManagementClient
      .listKnownParties()
  }

  override def getStaticTime()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Time.Timestamp] = {
    val timeService: TimeServiceStub = TimeServiceGrpc.stub(grpcClient.channel)
    for {
      resp <- ClientAdapter
        .serverStreaming(GetTimeRequest(grpcClient.ledgerId.unwrap), timeService.getTime)
        .runWith(Sink.head)
    } yield TimestampConversion.toLf(resp.getCurrentTime, TimestampConversion.ConversionMode.HalfUp)
  }

  override def setStaticTime(time: Time.Timestamp)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] = {
    val timeService: TimeServiceStub = TimeServiceGrpc.stub(grpcClient.channel)
    for {
      oldTime <- ClientAdapter
        .serverStreaming(GetTimeRequest(grpcClient.ledgerId.unwrap), timeService.getTime)
        .runWith(Sink.head)
      _ <- timeService.setTime(
        SetTimeRequest(
          grpcClient.ledgerId.unwrap,
          oldTime.currentTime,
          Some(TimestampConversion.fromInstant(time.toInstant)),
        )
      )
    } yield ()
  }

  // Note that CreateAndExerciseCommand gives two results, so we duplicate the package id
  private def toCommandPackageIds(cmd: command.ApiCommand): List[PackageId] =
    cmd match {
      case command.CreateAndExerciseCommand(templateId, _, _, _) =>
        List(templateId.packageId, templateId.packageId)
      case cmd => List(cmd.typeId.packageId)
    }

  private def toCommand(cmd: command.ApiCommand): Either[String, Command] =
    cmd match {
      case command.CreateCommand(templateId, argument) =>
        for {
          arg <- lfValueToApiRecord(true, argument)
        } yield Command().withCreate(
          CreateCommand(Some(toApiIdentifierUpgrades(templateId)), Some(arg))
        )
      case command.ExerciseCommand(typeId, contractId, choice, argument) =>
        for {
          arg <- lfValueToApiValue(true, argument)
        } yield Command().withExercise(
          // TODO: https://github.com/digital-asset/daml/issues/14747
          //  Fix once the new field interface_id have been added to the API Exercise Command
          ExerciseCommand(Some(toApiIdentifierUpgrades(typeId)), contractId.coid, choice, Some(arg))
        )
      case command.ExerciseByKeyCommand(templateId, key, choice, argument) =>
        for {
          key <- lfValueToApiValue(true, key)
          argument <- lfValueToApiValue(true, argument)
        } yield Command().withExerciseByKey(
          ExerciseByKeyCommand(
            Some(toApiIdentifierUpgrades(templateId)),
            Some(key),
            choice,
            Some(argument),
          )
        )
      case command.CreateAndExerciseCommand(templateId, template, choice, argument) =>
        for {
          template <- lfValueToApiRecord(true, template)
          argument <- lfValueToApiValue(true, argument)
        } yield Command().withCreateAndExercise(
          CreateAndExerciseCommand(
            Some(toApiIdentifierUpgrades(templateId)),
            Some(template),
            choice,
            Some(argument),
          )
        )
    }

  private def fromTreeEvent(
      ev: TreeEvent,
      intendedPackageId: PackageId,
  ): Either[String, ScriptLedgerClient.CommandResult] = {
    import scalaz.std.option._
    import scalaz.syntax.traverse._
    ev match {
      case TreeEvent(TreeEvent.Kind.Created(created)) =>
        for {
          cid <- ContractId.fromString(created.contractId)
        } yield ScriptLedgerClient.CreateResult(cid)
      case TreeEvent(TreeEvent.Kind.Exercised(exercised)) =>
        for {
          result <- NoLoggingValueValidator
            .validateValue(exercised.getExerciseResult)
            .left
            .map(_.toString)
          templateId <- Converter.fromApiIdentifier(exercised.getTemplateId)
          ifaceId <- exercised.interfaceId.traverse(Converter.fromApiIdentifier)
          choice <- ChoiceName.fromString(exercised.choice)
        } yield ScriptLedgerClient.ExerciseResult(
          templateId.copy(packageId = intendedPackageId),
          ifaceId,
          choice,
          result,
        )
      case TreeEvent(TreeEvent.Kind.Empty) =>
        throw new ConverterException("Invalid tree event Empty")
    }
  }

  override def createUser(
      user: User,
      rights: List[UserRight],
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[Unit]] =
    grpcClient.userManagementClient.createUser(user, rights).map(_ => Some(())).recover {
      case e: StatusRuntimeException if e.getStatus.getCode == Status.Code.ALREADY_EXISTS => None
    }

  override def getUser(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[User]] =
    grpcClient.userManagementClient.getUser(id).map(Some(_)).recover {
      case e: StatusRuntimeException if e.getStatus.getCode == Status.Code.NOT_FOUND => None
    }

  override def deleteUser(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[Unit]] =
    grpcClient.userManagementClient.deleteUser(id).map(Some(_)).recover {
      case e: StatusRuntimeException if e.getStatus.getCode == Status.Code.NOT_FOUND => None
    }

  override def listAllUsers()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[List[User]] = {
    val pageSize = 100
    def listWithPageToken(pageToken: String): Future[List[User]] = {
      grpcClient.userManagementClient
        .listUsers(pageToken = pageToken, pageSize = pageSize)
        .flatMap { case (users, nextPageToken) =>
          // A note on loop termination:
          // We terminate the loop when the nextPageToken is empty.
          // However, we may not terminate the loop with 'users.size < pageSize', because the server
          // does not guarantee to deliver pageSize users even if there are that many.
          if (nextPageToken == "") Future.successful(users.toList)
          else {
            listWithPageToken(nextPageToken).map { more =>
              users.toList ++ more
            }
          }
        }
    }
    listWithPageToken("") // empty-string as pageToken asks for the first page
  }

  override def grantUserRights(
      id: UserId,
      rights: List[UserRight],
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]] =
    grpcClient.userManagementClient.grantUserRights(id, rights).map(_.toList).map(Some(_)).recover {
      case e: StatusRuntimeException if e.getStatus.getCode == Status.Code.NOT_FOUND => None
    }

  override def revokeUserRights(
      id: UserId,
      rights: List[UserRight],
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]] =
    grpcClient.userManagementClient
      .revokeUserRights(id, rights)
      .map(_.toList)
      .map(Some(_))
      .recover {
        case e: StatusRuntimeException if e.getStatus.getCode == Status.Code.NOT_FOUND => None
      }

  override def listUserRights(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]] =
    grpcClient.userManagementClient.listUserRights(id).map(_.toList).map(Some(_)).recover {
      case e: StatusRuntimeException if e.getStatus.getCode == Status.Code.NOT_FOUND => None
    }

  override def vetPackages(packages: List[ScriptLedgerClient.ReadablePackageId])(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] = unsupportedOn("vetPackages")

  override def unvetPackages(packages: List[ScriptLedgerClient.ReadablePackageId])(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] = unsupportedOn("unvetPackages")

  override def listVettedPackages()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[List[ScriptLedgerClient.ReadablePackageId]] = unsupportedOn("listVettedPackages")

  override def listAllPackages()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[List[ScriptLedgerClient.ReadablePackageId]] = unsupportedOn("listAllPackages")

  override def vetDar(name: String)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] = {
    val adminClient = oAdminClient.getOrElse(
      throw new IllegalArgumentException("Attempted to use vetDar without specifying a adminPort")
    )
    adminClient.vetDar(name)
  }

  override def unvetDar(name: String)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] = {
    val adminClient = oAdminClient.getOrElse(
      throw new IllegalArgumentException("Attempted to use unvetDar without specifying a adminPort")
    )
    adminClient.unvetDar(name)
  }

  override def setProvidePackageId(shouldProvide: Boolean)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] =
    if (enableContractUpgrading)
      Future.successful { providePackageId = shouldProvide }
    else
      Future.failed(
        new ConverterException(
          "Contract-upgrading must be enabled via the --enable-contract-upgrading flag to use this command."
        )
      )
}
