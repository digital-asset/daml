// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script
package v2.ledgerinteraction
package grpcLedgerClient

import java.time.Instant
import java.util.UUID
import org.apache.pekko.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.ledger.api.domain.{PartyDetails, User, UserRight}
import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v2.commands._
import com.daml.ledger.api.v2.event.InterfaceView
import com.daml.ledger.api.v2.testing.time_service.TimeServiceGrpc.TimeServiceStub
import com.daml.ledger.api.v2.testing.time_service.{GetTimeRequest, SetTimeRequest, TimeServiceGrpc}
import com.daml.ledger.api.v2.transaction_filter.TransactionFilter
import com.daml.ledger.api.v2.transaction_filter.{
  Filters,
  InclusiveFilters,
  InterfaceFilter,
  TemplateFilter,
}
import com.daml.ledger.api.v2.{value => api}
import com.daml.lf.CompiledPackages
import com.digitalasset.canton.ledger.api.validation.NoLoggingValueValidator
import com.digitalasset.canton.ledger.client.LedgerClient
import com.daml.lf.command
import com.daml.lf.data.Ref._
import com.daml.lf.data.{Bytes, Ref, Time}
import com.daml.lf.engine.script.v2.Converter
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.speedy.{SValue, svalue}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.digitalasset.canton.ledger.api.util.LfEngineToApi.{
  lfValueToApiRecord,
  lfValueToApiValue,
  toApiIdentifier,
  toTimestamp,
}
import com.daml.script.converter.ConverterException
import io.grpc.{Status, StatusRuntimeException}
import io.grpc.protobuf.StatusProto
import com.google.rpc.status.{Status => GoogleStatus}
import scalaz.OneAnd
import scalaz.OneAnd._
import scalaz.std.either._
import scalaz.std.list._
import scalaz.std.set._
import scalaz.syntax.foldable._

import scala.concurrent.{ExecutionContext, Future}

class GrpcLedgerClient(
    val grpcClient: LedgerClient,
    val applicationId: Option[Ref.ApplicationId],
    val oAdminClient: Option[AdminLedgerClient],
    override val enableContractUpgrading: Boolean = false,
    val compiledPackages: CompiledPackages,
) extends ScriptLedgerClient {
  override val transport = "gRPC API"

  override def query(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Vector[ScriptLedgerClient.ActiveContract]] = {
    queryWithKey(parties, templateId).map(_.map(_._1))
  }

  // Omits the package id on an identifier if contract upgrades are enabled unless explicitPackageId is true
  private def toApiIdentifierUpgrades(
      identifier: Identifier,
      explicitPackageId: Boolean,
  ): api.Identifier = {
    val converted = toApiIdentifier(identifier)
    val pkgName = Runner
      .getPackageName(compiledPackages, identifier.packageId)
      .getOrElse(throw new IllegalArgumentException("Couldn't get package name"))
    if (explicitPackageId || !enableContractUpgrading) converted
    else converted.copy(packageId = "#" + pkgName)
  }

  // TODO[SW]: Currently do not support querying with explicit package id, interface for this yet to be determined
  // See https://github.com/digital-asset/daml/issues/17703
  private def templateFilter(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
  ): TransactionFilter = {
    val filters = Filters(
      Some(
        InclusiveFilters(templateFilters =
          Seq(
            TemplateFilter(
              Some(toApiIdentifierUpgrades(templateId, false)),
              includeCreatedEventBlob = true,
            )
          )
        )
      )
    )
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
            List(InterfaceFilter(Some(toApiIdentifier(interfaceId)), true))
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
    val acsResponse =
      grpcClient.v2.stateService
        .getActiveContracts(filter, verbose = false)
        .map(_._1)
    acsResponse.map(activeContracts =>
      activeContracts.toVector.map(activeContract => {
        val createdEvent = activeContract.getCreatedEvent
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
        // TODO: upstream that fix
        (
          ScriptLedgerClient.ActiveContract(
            Converter.fromApiIdentifier(createdEvent.templateId.get).getOrElse(???),
            cid,
            argument,
            blob,
          ),
          key,
        )
      })
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
    val acsResponse =
      grpcClient.v2.stateService
        .getActiveContracts(filter, verbose = false)
        .map(_._1)
    acsResponse.map(activeContracts =>
      activeContracts.toVector.flatMap(activeContract => {
        val createdEvent = activeContract.getCreatedEvent
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
      })
    )
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

  override def submit(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      disclosures: List[Disclosure],
      commands: List[ScriptLedgerClient.CommandWithMeta],
      optLocation: Option[Location],
      languageVersionLookup: PackageId => Either[String, LanguageVersion],
      errorBehaviour: ScriptLedgerClient.SubmissionErrorBehaviour,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Either[
    ScriptLedgerClient.SubmitFailure,
    (Seq[ScriptLedgerClient.CommandResult], ScriptLedgerClient.TransactionTree),
  ]] = {
    import scalaz.syntax.traverse._
    val ledgerDisclosures =
      disclosures.map { case Disclosure(tmplId, cid, blob) =>
        DisclosedContract(
          templateId = Some(toApiIdentifier(tmplId)),
          contractId = cid.coid,
          createdEventBlob = blob.toByteString,
        )
      }
    for {
      ledgerCommands <- Converter.toFuture(commands.traverse(toCommand(_)))
      // We need to remember the original package ID for each command result, so we can reapply them
      // after we get the results (for upgrades)
      commandResultPackageIds = commands.flatMap(toCommandPackageIds(_))

      apiCommands = Commands(
        actAs = actAs.toList,
        readAs = readAs.toList,
        commands = ledgerCommands,
        applicationId = applicationId.getOrElse(""),
        commandId = UUID.randomUUID.toString,
        disclosedContracts = ledgerDisclosures,
      )
      _ = println(s"Submitting ${apiCommands.toProtoString}")
      eResp <- grpcClient.v2.commandService
        .submitAndWaitForTransactionTree(apiCommands)

      result <- eResp match {
        case Right(resp) =>
          for {
            tree <- Converter.toFuture(
              Converter.fromTransactionTree(resp.getTransaction, commandResultPackageIds)
            )
            results = ScriptLedgerClient.transactionTreeToCommandResults(tree)
          } yield Right((results, tree))
        case Left(status) =>
          Future.successful(
            Left(
              ScriptLedgerClient.SubmitFailure(
                StatusProto.toStatusRuntimeException(GoogleStatus.toJavaProto(status)),
                GrpcErrorParser.convertStatusRuntimeException(status),
              )
            )
          )
      }
    } yield result
  }

  override def allocateParty(partyIdHint: String, displayName: String)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ) = {
    grpcClient.partyManagementClient
      .allocateParty(Some(partyIdHint), Some(displayName))
      .map(_.party)
  }

  override def listKnownParties()(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[List[PartyDetails]] = {
    def listParties(pageToken: String): Future[List[PartyDetails]] = for {
      response <- grpcClient.partyManagementClient.listKnownParties(
        pageToken = pageToken,
        pageSize = 0, // lets the server pick the page size
      )
      (parties, nextPageToken) = response
      tail <- if (nextPageToken.isEmpty) Future.successful(Nil) else listParties(nextPageToken)
    } yield parties ++ tail

    listParties("")
  }

  override def getStaticTime()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Time.Timestamp] = {
    val timeService: TimeServiceStub = TimeServiceGrpc.stub(grpcClient.channel)
    for {
      resp <- timeService.getTime(GetTimeRequest())
      instant = Instant.ofEpochSecond(resp.getCurrentTime.seconds, resp.getCurrentTime.nanos.toLong)
    } yield Time.Timestamp.assertFromInstant(instant, java.math.RoundingMode.HALF_UP)
  }

  override def setStaticTime(time: Time.Timestamp)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] = {
    val timeService: TimeServiceStub = TimeServiceGrpc.stub(grpcClient.channel)
    for {
      oldTime <- timeService.getTime(GetTimeRequest())
      _ <- timeService.setTime(
        SetTimeRequest(
          oldTime.currentTime,
          Some(toTimestamp(time.toInstant)),
        )
      )
    } yield ()
  }

  // Note that CreateAndExerciseCommand gives two results, so we duplicate the package id
  private def toCommandPackageIds(cmd: ScriptLedgerClient.CommandWithMeta): List[PackageId] =
    cmd.command match {
      case command.CreateAndExerciseCommand(tmplRef, _, _, _) =>
        List(tmplRef.assertToTypeConName.packageId, tmplRef.assertToTypeConName.packageId)
      case cmd =>
        List(cmd.typeRef.assertToTypeConName.packageId)
    }

  private def toCommand(cmd: ScriptLedgerClient.CommandWithMeta): Either[String, Command] =
    cmd.command match {
      case command.CreateCommand(tmplRef, argument) =>
        for {
          arg <- lfValueToApiRecord(true, argument)
        } yield Command().withCreate(
          CreateCommand(
            Some(toApiIdentifierUpgrades(tmplRef.assertToTypeConName, cmd.explicitPackageId)),
            Some(arg),
          )
        )
      case command.ExerciseCommand(typeRef, contractId, choice, argument) =>
        for {
          arg <- lfValueToApiValue(true, argument)
        } yield Command().withExercise(
          // TODO: https://github.com/digital-asset/daml/issues/14747
          //  Fix once the new field interface_id have been added to the API Exercise Command
          ExerciseCommand(
            Some(toApiIdentifierUpgrades(typeRef.assertToTypeConName, cmd.explicitPackageId)),
            contractId.coid,
            choice,
            Some(arg),
          )
        )
      case command.ExerciseByKeyCommand(tmplRef, key, choice, argument) =>
        for {
          key <- lfValueToApiValue(true, key)
          argument <- lfValueToApiValue(true, argument)
        } yield Command().withExerciseByKey(
          ExerciseByKeyCommand(
            Some(toApiIdentifierUpgrades(tmplRef.assertToTypeConName, cmd.explicitPackageId)),
            Some(key),
            choice,
            Some(argument),
          )
        )
      case command.CreateAndExerciseCommand(tmplRef, template, choice, argument) =>
        for {
          template <- lfValueToApiRecord(true, template)
          argument <- lfValueToApiValue(true, argument)
        } yield Command().withCreateAndExercise(
          CreateAndExerciseCommand(
            Some(toApiIdentifierUpgrades(tmplRef.assertToTypeConName, cmd.explicitPackageId)),
            Some(template),
            choice,
            Some(argument),
          )
        )
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
}
