// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script
package v2.ledgerinteraction
package grpcLedgerClient

import java.time.Instant
import java.util.UUID
import org.apache.pekko.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.ledger.api.{PartyDetails, User, UserRight}
import com.daml.ledger.api.v2.admin.package_management_service.{
  UpdateVettedPackagesRequest,
  VettedPackagesChange,
  VettedPackagesRef,
}
import com.daml.ledger.api.v2.admin.package_management_service.VettedPackagesChange.{
  Operation,
  Unvet,
  Vet,
}
import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v2.commands._
import com.daml.ledger.api.v2.event.InterfaceView
import com.daml.ledger.api.v2.package_service.{
  ListVettedPackagesRequest,
  PackageMetadataFilter,
  TopologyStateFilter,
}
import com.daml.ledger.api.v2.testing.time_service.TimeServiceGrpc.TimeServiceStub
import com.daml.ledger.api.v2.testing.time_service.{GetTimeRequest, SetTimeRequest, TimeServiceGrpc}
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  InterfaceFilter,
  TemplateFilter,
}
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.api.v2.{value => api}
import com.daml.timer.RetryStrategy
import com.digitalasset.daml.lf.CompiledPackages
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.daml.lf.command
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{Bytes, Ref, Time}
import com.digitalasset.daml.lf.engine.script.v2.Converter
import com.digitalasset.daml.lf.engine.{Enricher, ResultDone, preprocessing}
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion, Reference}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.canton.ledger.api.util.LfEngineToApi.{
  lfValueToApiRecord,
  lfValueToApiValue,
  toApiIdentifier,
  toTimestamp,
}
import com.digitalasset.daml.lf.script.converter.ConverterException
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.command.ApiContractKey
import io.grpc.{Status, StatusRuntimeException}
import io.grpc.protobuf.StatusProto
import com.google.rpc.status.{Status => GoogleStatus}
import scalaz.OneAnd
import scalaz.OneAnd._
import scalaz.std.either._
import scalaz.std.list._
import scalaz.std.set._
import scalaz.syntax.foldable._
import com.digitalasset.daml.lf.crypto

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt

class GrpcLedgerClient(
    val grpcClient: LedgerClient,
    val userId: Option[Ref.UserId],
    val oAdminClient: Option[AdminLedgerClient],
    val compiledPackages: CompiledPackages,
) extends ScriptLedgerClient {
  override val transport = "gRPC API"
  implicit val traceContext: TraceContext = TraceContext.empty

  private[this] val preprocessor = new preprocessing.CommandPreprocessor(
    compiledPackages.pkgInterface,
    forbidLocalContractIds = true,
  )

  val enricher = new Enricher(
    compiledPackages = compiledPackages,
    // Cannot load packages in GrpcLedgerClient
    loadPackage = { (_: PackageId, _: Reference) => ResultDone(()) },
    addTypeInfo = true,
    addFieldNames = true,
    addTrailingNoneFields = true,
    forbidLocalContractIds = true,
  )

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
      identifier: TypeConRef,
      explicitPackageId: Boolean,
  ): api.Identifier = {
    identifier.pkg match {
      case PackageRef.Name(name) =>
        if (explicitPackageId)
          throw new IllegalArgumentException(
            "Cannot set explicitPackageId = true on an ApiCommand that uses a PackageName"
          )
        else
          api.Identifier(
            "#" + name,
            identifier.qualifiedName.module.toString(),
            identifier.qualifiedName.name.toString(),
          )
      case PackageRef.Id(pkgId) =>
        val converted = toApiIdentifier(identifier.assertToTypeConId)
        packageIdToUpgradeName(explicitPackageId, pkgId)
          .fold(converted)(name => converted.copy(packageId = "#" + name.toString))
    }
  }

  private def packageIdToUpgradeName(
      explicitPackageId: Boolean,
      pkgId: PackageId,
  ): Option[PackageName] = {
    compiledPackages.pkgInterface
      .lookupPackage(pkgId)
      .toOption
      .filter(pkgSig => pkgSig.supportsUpgrades(pkgId) && !explicitPackageId)
      .map(_.metadata.name)
  }

  private def getIdentifierPkgId(
      pkgPrefs: List[PackageId],
      identifier: TypeConRef,
  ): PackageId = {
    def handleName(name: Ref.PackageName): PackageId = {
      val matchingSigs = compiledPackages.signatures.filter(_._2.metadata.name == name)
      matchingSigs
        .filter(sig => pkgPrefs.contains(sig._1))
        .headOption
        .getOrElse(matchingSigs.maxBy(_._2.metadata.version))
        ._1
    }

    identifier.pkg match {
      case PackageRef.Name(name) => handleName(name)
      case PackageRef.Id(pkgId) =>
        // [djt]TODO: We likely also want to apply upgrading to pkgIds when
        // explicitPackageId is passed, but this is outside the scope of current
        // changes and would need to be validated with existing GrpcLedgerClient
        // users.
        // Implementation would look something like:
        // packageIdToUpgradeName(explicitPackageId, pkgId)
        //   .fold(pkgId)(name => handleName(name))
        pkgId
    }
  }

  // TODO[SW]: Currently do not support querying with explicit package id, interface for this yet to be determined
  // See https://github.com/digital-asset/daml/issues/17703
  private def templateFormat(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
      verbose: Boolean,
  ): EventFormat = {
    val filters = Filters(
      Seq(
        CumulativeFilter(
          IdentifierFilter.TemplateFilter(
            TemplateFilter(
              Some(toApiIdentifierUpgrades(templateId.toRef, false)),
              includeCreatedEventBlob = true,
            )
          )
        )
      )
    )
    EventFormat(
      filtersByParty = parties.toList.map(p => (p, filters)).toMap,
      filtersForAnyParty = None,
      verbose = verbose,
    )
  }

  private def interfaceFormat(
      parties: OneAnd[Set, Ref.Party],
      interfaceId: Identifier,
      verbose: Boolean,
  ): EventFormat = {
    val filters =
      Filters(
        Seq(
          CumulativeFilter(
            IdentifierFilter.InterfaceFilter(
              InterfaceFilter(Some(toApiIdentifierUpgrades(interfaceId.toRef, false)), true, false)
            )
          )
        )
      )
    EventFormat(
      filtersByParty = parties.toList.map(p => (p, filters)).toMap,
      filtersForAnyParty = None,
      verbose = verbose,
    )
  }

  // Helper shared by query, queryContractId and queryContractKey
  private def queryWithKey(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Vector[(ScriptLedgerClient.ActiveContract, Option[crypto.Hash])]] = {
    val format = templateFormat(parties, templateId, verbose = false)
    val acsResponse =
      grpcClient.stateService.getLedgerEndOffset().flatMap { offset =>
        grpcClient.stateService
          .getActiveContracts(
            eventFormat = format,
            validAtOffset = offset,
            token = None,
          )
      }
    acsResponse.map(activeContracts =>
      activeContracts.toVector.map(activeContract => {
        val createdEvent = activeContract.getCreatedEvent
        val argument =
          NoLoggingValueValidator.validateRecord(createdEvent.getCreateArguments) match {
            case Left(err) => throw new ConverterException(err.toString)
            case Right(argument) => argument
          }
        val keyHash: Option[crypto.Hash] =
          if (createdEvent.contractKeyHash.isEmpty) None
          else
            crypto.Hash.fromBytes(Bytes.fromByteString(createdEvent.contractKeyHash)) match {
              case Right(hash) => Some(hash)
              case Left(err) => throw new ConverterException(err)
            }

        val enrichedArgument = enricher.enrichContract(templateId, argument).consume() match {
          case Right(arg) => arg
          case Left(err) => throw new ConverterException(err.toString)
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
        val disclosureTemplateId =
          Converter
            .fromApiIdentifier(
              createdEvent.templateId.getOrElse(
                throw new ConverterException("missing required template_id in CreatedEvent")
              )
            )
            .getOrElse(throw new ConverterException("invalid template_id in CreatedEvent"))
        (
          ScriptLedgerClient.ActiveContract(
            disclosureTemplateId,
            cid,
            enrichedArgument,
            blob,
          ),
          keyHash,
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
    val format = interfaceFormat(parties, interfaceId, verbose = false)
    val acsResponse =
      grpcClient.stateService.getLedgerEndOffset().flatMap { offset =>
        grpcClient.stateService
          .getActiveContracts(
            eventFormat = format,
            validAtOffset = offset,
            token = None,
          )
      }
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
          val enrichedviewValue =
            if (viewValue.fields.isEmpty)
              None
            else
              Some(enricher.enrichView(interfaceId, viewValue).consume() match {
                case Right(viewValue) => viewValue
                case Left(err) => throw new ConverterException(err.toString)
              })
          // Because we filter for a specific interfaceId,
          // we will get at most one view for a given cid.
          (cid, enrichedviewValue)
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

  private[this] def computeKeyHash(templateId: Identifier, key: Value)(implicit
      ec: ExecutionContext
  ): Future[crypto.Hash] =
    Future(
      preprocessor
        .unsafePreprocessApiContractKey(Map.empty, ApiContractKey(templateId.toRef, key))
        .hash
    )

  override def queryContractKey(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
      key: Value,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Option[ScriptLedgerClient.ActiveContract]] = {
    // We cannot do better than a linear search over query here.
    for {
      activeContracts <- queryWithKey(parties, templateId)
      ownHash <- computeKeyHash(templateId, key)
    } yield {
      activeContracts.collectFirst({ case (c, Some(kHash)) if kHash == ownHash => c })
    }
  }

  override def submit(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      disclosures: List[Disclosure],
      optPackagePreference: Option[List[PackageId]],
      commands: List[ScriptLedgerClient.CommandWithMeta],
      prefetchContractKeys: List[AnyContractKey],
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
          synchronizerId = "",
        )
      }
    for {
      ledgerCommands <- Converter.toFuture(commands.traverse(toCommand(_)))
      // We need to remember the original package ID for each command result, so we can reapply them
      // after we get the results (for upgrades)
      commandResultPackageIds = commands.flatMap(
        toCommandPackageIds(optPackagePreference.getOrElse(List.empty), _)
      )
      ledgerPrefetchContractKeys <- Converter.toFuture(
        prefetchContractKeys.traverse(toPrefetchContractKey)
      )

      apiCommands = Commands.defaultInstance
        .withActAs(actAs.toList)
        .withReadAs(readAs.toList)
        .withCommands(ledgerCommands)
        .withUserId(userId.getOrElse(""))
        .withCommandId(UUID.randomUUID.toString)
        .withDisclosedContracts(ledgerDisclosures)
        .withPrefetchContractKeys(ledgerPrefetchContractKeys)
        .withPackageIdSelectionPreference(optPackagePreference.getOrElse(List.empty))
      eResp <- grpcClient.commandService
        .submitAndWaitForTransaction(apiCommands, TRANSACTION_SHAPE_LEDGER_EFFECTS)

      result <- eResp match {
        case Right(resp) =>
          for {
            tree <- Converter.toFuture(
              Converter.fromTransaction(resp.getTransaction, commandResultPackageIds, enricher)
            )
            results = ScriptLedgerClient.transactionTreeToCommandResults(tree)
          } yield Right((results, tree))
        case Left(status) =>
          val submitErr = GrpcErrorParser.convertStatusRuntimeException(status)
          val runtimeErr = StatusProto.toStatusRuntimeException(GoogleStatus.toJavaProto(status))

          Future.successful(
            Left(
              ScriptLedgerClient.SubmitFailure(
                submitErr match {
                  // If we have a trace, place it into the runtime error so it is shown by daml-script runner
                  case SubmitError.FailureStatusError(_, Some(trace)) =>
                    new StatusRuntimeException(
                      runtimeErr.getStatus().augmentDescription(trace),
                      runtimeErr.getTrailers(),
                    )
                  case _ => runtimeErr
                },
                submitErr,
              )
            )
          )
      }
    } yield result
  }

  override def allocateParty(partyIdHint: String)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ) =
    for {
      party <- grpcClient.partyManagementClient
        .allocateParty(hint = Some(partyIdHint), token = None)
        .map(_.party)
      _ <- RetryStrategy.constant(5, 200.milliseconds) { case (_, _) =>
        for {
          res <- grpcClient.stateService
            .getConnectedSynchronizers(party = party, token = None)
          _ <-
            if (res.connectedSynchronizers.isEmpty)
              Future.failed(
                new java.util.concurrent.TimeoutException(
                  "Party not allocated on any synchronizer within 1 second"
                )
              )
            else Future.unit
        } yield ()
      }
    } yield party

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
  private def toCommandPackageIds(
      pkgPrefs: List[PackageId],
      cmd: ScriptLedgerClient.CommandWithMeta,
  ): List[PackageId] =
    cmd.command match {
      case command.CreateAndExerciseCommand(tmplRef, _, _, _) =>
        List(
          getIdentifierPkgId(pkgPrefs, tmplRef),
          getIdentifierPkgId(pkgPrefs, tmplRef),
        )
      case command =>
        List(getIdentifierPkgId(pkgPrefs, command.typeRef))
    }

  private def toCommand(cmd: ScriptLedgerClient.CommandWithMeta): Either[String, Command] =
    cmd.command match {
      case command.CreateCommand(tmplRef, argument) =>
        for {
          arg <- lfValueToApiRecord(true, argument)
        } yield Command.defaultInstance.withCreate(
          CreateCommand(
            Some(toApiIdentifierUpgrades(tmplRef, cmd.explicitPackageId)),
            Some(arg),
          )
        )
      case command.ExerciseCommand(typeRef, contractId, choice, argument) =>
        for {
          arg <- lfValueToApiValue(true, argument)
        } yield Command.defaultInstance.withExercise(
          // TODO: https://github.com/digital-asset/daml/issues/14747
          //  Fix once the new field interface_id have been added to the API Exercise Command
          ExerciseCommand(
            Some(toApiIdentifierUpgrades(typeRef, cmd.explicitPackageId)),
            contractId.coid,
            choice,
            Some(arg),
          )
        )
      case command.ExerciseByKeyCommand(tmplRef, key, choice, argument) =>
        for {
          key <- lfValueToApiValue(true, key)
          argument <- lfValueToApiValue(true, argument)
        } yield Command.defaultInstance.withExerciseByKey(
          ExerciseByKeyCommand(
            Some(toApiIdentifierUpgrades(tmplRef, cmd.explicitPackageId)),
            Some(key),
            choice,
            Some(argument),
          )
        )
      case command.CreateAndExerciseCommand(tmplRef, template, choice, argument) =>
        for {
          template <- lfValueToApiRecord(true, template)
          argument <- lfValueToApiValue(true, argument)
        } yield Command.defaultInstance.withCreateAndExercise(
          CreateAndExerciseCommand(
            Some(toApiIdentifierUpgrades(tmplRef, cmd.explicitPackageId)),
            Some(template),
            choice,
            Some(argument),
          )
        )
    }

  private def toPrefetchContractKey(key: AnyContractKey): Either[String, PrefetchContractKey] = {
    for {
      nonExtendedKeyValue <- Converter.castCommandExtendedValue(key.key)
      contractKey <- lfValueToApiValue(true, nonExtendedKeyValue)
    } yield PrefetchContractKey(
      templateId = Some(toApiIdentifierUpgrades(key.templateId.toRef, false)),
      contractKey = Some(contractKey),
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
  ): Future[Unit] = {
    grpcClient.packageManagementClient
      .updateVettedPackages(
        UpdateVettedPackagesRequest.of(
          Seq(
            VettedPackagesChange.of(
              Operation.Vet(
                Vet(
                  packages.map(pkg => VettedPackagesRef("", pkg.name, pkg.version.toString)),
                  newValidFromInclusive = None,
                  newValidUntilExclusive = None,
                )
              )
            )
          ),
          dryRun = false,
          synchronizerId = "",
          expectedTopologySerial = None,
          updateVettedPackagesForceFlags = Seq.empty,
        )
      )
      .map(_ => ())
  }

  override def unvetPackages(packages: List[ScriptLedgerClient.ReadablePackageId])(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] = {
    grpcClient.packageManagementClient
      .updateVettedPackages(
        UpdateVettedPackagesRequest.of(
          Seq(
            VettedPackagesChange.of(
              Operation.Unvet(
                Unvet(packages.map(pkg => VettedPackagesRef("", pkg.name, pkg.version.toString)))
              )
            )
          ),
          dryRun = false,
          synchronizerId = "",
          expectedTopologySerial = None,
          updateVettedPackagesForceFlags = Seq.empty,
        )
      )
      .map(_ => ())
  }

  override def waitUntilVettingVisible(
      packages: Iterable[ScriptLedgerClient.ReadablePackageId],
      onParticipantUid: String,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    RetryStrategy.constant(25, 200.milliseconds) { case (_, _) =>
      for {
        vettedPackages <- listPackages(packages, onParticipantUid, "")
        _ <-
          if (packages.forall(vettedPackages.contains(_)))
            Future.unit
          else
            Future.failed(
              new java.util.concurrent.TimeoutException(
                s"Not all packages on participant $onParticipantUid have been vetted within 5 seconds: $packages"
              )
            )
      } yield ()
    }
  }

  override def waitUntilUnvettingVisible(
      packages: Iterable[ScriptLedgerClient.ReadablePackageId],
      onParticipantUid: String,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    RetryStrategy.constant(25, 200.milliseconds) { case (_, _) =>
      for {
        vettedPackages <- listPackages(packages, onParticipantUid, "")
        _ <-
          if (packages.forall(!vettedPackages.contains(_)))
            Future.unit
          else
            Future.failed(
              new java.util.concurrent.TimeoutException(
                s"Not all packages on participant $onParticipantUid have been unvetted within 5 seconds: $packages"
              )
            )
      } yield ()
    }
  }

  private def listPackages(
      packages: Iterable[ScriptLedgerClient.ReadablePackageId],
      onParticipantUid: String,
      pageToken: String,
  )(implicit ec: ExecutionContext): Future[Seq[ScriptLedgerClient.ReadablePackageId]] = for {
    response <- grpcClient.packageService
      .listVettedPackages(
        ListVettedPackagesRequest.of(
          packageMetadataFilter = Some(
            PackageMetadataFilter
              .of(packageIds = Seq.empty, packageNamePrefixes = packages.map(_.name).toSeq)
          ),
          topologyStateFilter = Some(
            TopologyStateFilter
              .of(participantIds = Seq(onParticipantUid), synchronizerIds = Seq.empty)
          ),
          pageToken = pageToken,
          pageSize = 0,
        )
      )
    tail <-
      if (response.nextPageToken.isEmpty) Future.successful(Nil)
      else listPackages(packages, onParticipantUid, response.nextPageToken)
    readableVettedPackages = response.vettedPackages.flatMap(
      _.packages.map(pkg =>
        ScriptLedgerClient.ReadablePackageId(
          Ref.PackageName.assertFromString(pkg.packageName),
          Ref.PackageVersion.assertFromString(pkg.packageVersion),
        )
      )
    )
  } yield readableVettedPackages ++ tail

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

  override def allocatePartyOnMultipleParticipants(
      party: Ref.Party,
      participantIds: Iterable[String],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Unit] = {
    val adminClient = oAdminClient.getOrElse(
      throw new IllegalArgumentException(
        "Attempted to use exportParty without specifying a adminPort"
      )
    )
    adminClient.allocatePartyOnMultipleParticipants(party, participantIds)
  }

  override def aggregateAllocatePartyOnMultipleParticipants(
      clients: List[ScriptLedgerClient],
      partyHint: String,
      namespace: String,
      participantIds: Iterable[String],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Ref.Party] = {
    val party = Party.assertFromString(partyHint + "::" + namespace)
    for {
      _ <- Future.traverse(clients)(_.allocatePartyOnMultipleParticipants(party, participantIds))
    } yield party
  }

  override def waitUntilHostingVisible(
      party: Ref.Party,
      onParticipantUids: Iterable[String],
  ): Future[Unit] = {
    val adminClient = oAdminClient.getOrElse(
      throw new IllegalArgumentException(
        "Attempted to use waitUntilHostingVisible without specifying a adminPort"
      )
    )
    adminClient.waitUntilHostingVisible(party, onParticipantUids)
  }

  override def getParticipantUid: String = oAdminClient
    .getOrElse(
      throw new IllegalArgumentException(
        "Attempted to use getParticipantUid without specifying a adminPort"
      )
    )
    .participantUid
}
