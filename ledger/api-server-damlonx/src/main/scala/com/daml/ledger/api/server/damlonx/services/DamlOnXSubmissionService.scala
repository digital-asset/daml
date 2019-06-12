// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.services

import akka.stream.ActorMaterializer
import com.daml.ledger.participant.state.index.v1.IndexService
import com.daml.ledger.participant.state.v1.{SubmitterInfo, TransactionMeta, WriteService}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.{
  Engine,
  Blinding,
  Result,
  ResultDone,
  ResultError,
  ResultNeedContract,
  ResultNeedKey,
  ResultNeedPackage,
  Error => DamlLfError
}
import com.digitalasset.daml.lf.lfpackage.{Ast, Decode}
import com.digitalasset.daml.lf.transaction.Transaction.{Value => TxValue}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger.api.domain.{LedgerId => ApiLedgerId}
import com.digitalasset.ledger.api.domain.{Commands => ApiCommands}
import com.digitalasset.ledger.api.messages.command.submission.SubmitRequest
import com.digitalasset.platform.server.api.services.domain.CommandSubmissionService
import com.digitalasset.platform.server.api.services.grpc.GrpcCommandSubmissionService
import com.digitalasset.platform.server.api.validation.{ErrorFactories, IdentifierResolver}
import com.digitalasset.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  CommandSubmissionServiceLogging
}
import io.grpc.BindableService
import org.slf4j.LoggerFactory
import scalaz.syntax.tag._
import com.digitalasset.daml_lf.DamlLf

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try
import java.util.concurrent.Executors

object DamlOnXSubmissionService {

  def create(
      identifierResolver: IdentifierResolver,
      ledgerId: String,
      indexService: IndexService,
      writeService: WriteService,
      engine: Engine)(
      implicit ec: ExecutionContext,
      mat: ActorMaterializer): CommandSubmissionServiceGrpc.CommandSubmissionService
    with BindableService
    with CommandSubmissionServiceLogging =
    new GrpcCommandSubmissionService(
      new DamlOnXSubmissionService(indexService, writeService, engine),
      ApiLedgerId(ledgerId),
      identifierResolver
    ) with CommandSubmissionServiceLogging

}

class DamlOnXSubmissionService private (
    indexService: IndexService,
    writeService: WriteService,
    engine: Engine,
)(implicit ec: ExecutionContext, mat: ActorMaterializer)
    extends CommandSubmissionService
    with ErrorFactories
    with AutoCloseable {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // Over time we might want to adapt damle interface
  override def submit(request: SubmitRequest): Future[Unit] = {
    val commands = request.commands
    logger.debug(
      "Received composite command {} with let {}.",
      if (logger.isTraceEnabled()) commands else commands.commandId.unwrap,
      commands.ledgerEffectiveTime: Any)
    recordOnLedger(commands)
  }

  // NOTE(JM): Dummy caching of archive decoding. Leaving this in
  // as this code dies soon.
  private val packageLoadContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
  private val packageCache: scala.collection.mutable.Map[Ref.PackageId, Ast.Package] =
    scala.collection.mutable.Map.empty

  private def recordOnLedger(commands: ApiCommands): Future[Unit] = {
    val getPackage: Ref.PackageId => Future[Option[Ast.Package]] =
      (packageId: Ref.PackageId) =>
        indexService
          .getPackage(packageId)
          .flatMap { optArchive: Option[DamlLf.Archive] =>
            packageCache.get(packageId) match {
              case None =>
                logger.info(s"getPackage: decoding $packageId...")
                Future
                  .fromTry(Try(optArchive.map(Decode.decodeArchive(_)._2)))
                  .map { optPkg: Option[Ast.Package] =>
                    optPkg.foreach { pkg =>
                      packageCache(packageId) = pkg
                    }
                    optPkg
                  }
              case Some(pkg) =>
                logger.info(s"getPackage: loaded cached package $packageId")
                Future.successful(Some(pkg))
            }
          }(packageLoadContext)

    val getContract =
      (coid: AbsoluteContractId) => indexService.lookupActiveContract(commands.submitter, coid)

    consume(engine.submit(commands.commands))(getPackage, getContract)
      .flatMap {
        case Left(err) =>
          Future.failed(invalidArgument("error: " + err.detailMsg))

        case Right(updateTx) =>
          // NOTE(JM): Authorizing the transaction here so that we do not submit
          // malformed transactions. A side-effect is that we compute blinding info
          // but we don't actually use that for anything.
          Blinding.checkAuthorizationAndBlind(
            updateTx,
            Set(Ref.Party.assertFromString(commands.submitter))) match {
            case Left(err) =>
              Future.failed(invalidArgument("error: " + err.toString))

            case Right(_) =>
              logger.debug(
                s"Submitting transaction from ${commands.submitter} with ${commands.commandId.unwrap}")
              FutureConverters
                .toScala(
                  writeService
                    .submitTransaction(
                      submitterInfo = SubmitterInfo(
                        submitter = commands.submitter,
                        applicationId = commands.applicationId.unwrap,
                        maxRecordTime = Timestamp.assertFromInstant(commands.maximumRecordTime), // FIXME(JM): error handling
                        commandId = commands.commandId.unwrap
                      ),
                      transactionMeta = TransactionMeta(
                        ledgerEffectiveTime =
                          Timestamp.assertFromInstant(commands.ledgerEffectiveTime),
                        workflowId = commands.workflowId.map(_.unwrap)
                      ),
                      transaction = updateTx
                    )
                )
                .map(_ => ())
          }
      }
  }

  private def consume[A](result: Result[A])(
      getPackage: Ref.PackageId => Future[Option[Ast.Package]],
      getContract: Value.AbsoluteContractId => Future[
        Option[Value.ContractInst[TxValue[Value.AbsoluteContractId]]]])(
      implicit ec: ExecutionContext): Future[Either[DamlLfError, A]] = {

    def resolveStep(result: Result[A]): Future[Either[DamlLfError, A]] = {
      result match {
        case ResultNeedPackage(packageId, resume) =>
          getPackage(packageId).flatMap(p => resolveStep(resume(p)))
        case ResultDone(r) =>
          Future.successful(Right(r))
        case ResultNeedKey(key, resume) =>
          Future.failed(new IllegalArgumentException("Contract keys not implemented yet"))
        case ResultNeedContract(acoid, resume) =>
          getContract(acoid).flatMap(o => resolveStep(resume(o)))
        case ResultError(err) => Future.successful(Left(err))
      }
    }

    resolveStep(result)
  }

  override def close(): Unit = ()

}
