// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.services

import akka.stream.ActorMaterializer
import com.daml.ledger.participant.state.index.v1.IndexService
import com.daml.ledger.participant.state.v1.{LedgerId, SubmitterInfo, TransactionMeta, WriteService}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.{
  Engine,
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

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object DamlOnXSubmissionService {

  def create(
      identifierResolver: IdentifierResolver,
      ledgerId: LedgerId,
      indexService: IndexService,
      writeService: WriteService,
      engine: Engine)(implicit ec: ExecutionContext, mat: ActorMaterializer)
    : CommandSubmissionServiceGrpc.CommandSubmissionService with BindableService with CommandSubmissionServiceLogging =
    new GrpcCommandSubmissionService(
      new DamlOnXSubmissionService(indexService, writeService, engine),
      ledgerId.underlyingString,
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
    with AutoCloseable
    with DamlOnXServiceUtils {

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

  private def recordOnLedger(commands: ApiCommands): Future[Unit] = {
    val ledgerId = Ref.SimpleString.assertFromString(commands.ledgerId.unwrap)
    val getPackage =
      (packageId: Ref.PackageId) =>
        consumeAsyncResult(
          indexService.getPackage(ledgerId, packageId)
        ).flatMap { optArchive =>
          Future.fromTry(Try(optArchive.map(Decode.decodeArchive(_)._2)))
      }
    val getContract =
      (coid: AbsoluteContractId) =>
        consumeAsyncResult(
          indexService
            .lookupActiveContract(ledgerId, coid))

    consume(engine.submit(commands.commands))(getPackage, getContract)
      .flatMap {
        case Left(err) =>
          Future.failed(invalidArgument(err.detailMsg))

        case Right(updateTx) =>
          Future {
            logger.debug(
              s"Submitting transaction from ${commands.submitter.underlyingString} with ${commands.commandId.unwrap}")
            writeService.submitTransaction(
              submitterInfo = SubmitterInfo(
                submitter = Ref.SimpleString.assertFromString(commands.submitter.underlyingString),
                applicationId = commands.applicationId.unwrap,
                maxRecordTime = Timestamp.assertFromInstant(commands.maximumRecordTime), // FIXME(JM): error handling
                commandId = commands.commandId.unwrap
              ),
              transactionMeta = TransactionMeta(
                ledgerEffectiveTime = Timestamp.assertFromInstant(commands.ledgerEffectiveTime),
                workflowId = commands.workflowId.fold("")(_.unwrap) // FIXME(JM): sensible defaulting?
              ),
              transaction = updateTx
            )
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
