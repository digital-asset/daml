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
import com.digitalasset.ledger.api.domain.Commands
import com.digitalasset.ledger.api.messages.command.submission.SubmitRequest
import com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceLogging
import com.digitalasset.platform.participant.util.ApiToLfEngine.apiCommandsToLfCommands
import com.digitalasset.platform.server.api.services.domain.CommandSubmissionService
import com.digitalasset.platform.server.api.services.grpc.GrpcCommandSubmissionService
import com.digitalasset.platform.server.api.validation.{ErrorFactories, IdentifierResolver}
import io.grpc.BindableService
import org.slf4j.LoggerFactory
import scalaz.syntax.tag._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

object DamlOnXSubmissionService {

  def create(
      identifierResolver: IdentifierResolver,
      indexService: IndexService,
      writeService: WriteService,
      engine: Engine)(implicit ec: ExecutionContext, mat: ActorMaterializer)
    : GrpcCommandSubmissionService with BindableService with CommandSubmissionServiceLogging =
    new GrpcCommandSubmissionService(
      new DamlOnXSubmissionService(indexService, writeService, engine),
      Await.result(indexService.getCurrentIndexId(), 5.seconds), // FIXME(JM): ???
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

  private def recordOnLedger(commands: Commands): Future[Unit] = {
    val getPackage =
      (packageId: Ref.PackageId) =>
        consumeAsyncResult(
          indexService.getPackage(commands.ledgerId.unwrap, packageId)
        ).flatMap { optArchive =>
          Future.fromTry(Try(optArchive.map(Decode.decodeArchive(_)._2)))
      }
    val getContract =
      (coid: AbsoluteContractId) =>
        consumeAsyncResult(
          indexService
            .lookupActiveContract(commands.ledgerId.unwrap, coid))

    apiCommandsToLfCommands(commands)
      .consumeAsync(getPackage)
      .flatMap {
        case Left(e) =>
          logger.error(s"Could not translate commands: $e")
          Future.failed(invalidArgument(s"Could not translate command: $e"))

        case Right(lfCommands) =>
          consume(engine.submit(lfCommands))(getPackage, getContract)
            .flatMap {
              case Left(err) =>
                Future.failed(invalidArgument(err.detailMsg))

              case Right(updateTx) =>
                indexService.getCurrentStateId.map { stateId =>
                  logger.debug(
                    s"Submitting transaction from ${commands.submitter.unwrap} with ${commands.commandId.unwrap}")
                  writeService.submitTransaction(
                    stateId = stateId,
                    submitterInfo = SubmitterInfo(
                      submitter = commands.submitter.unwrap,
                      applicationId = commands.applicationId.unwrap,
                      maxRecordTime = Timestamp.assertFromInstant(commands.maximumRecordTime), // FIXME(JM): error handling
                      commandId = commands.commandId.unwrap
                    ),
                    transactionMeta = TransactionMeta(
                      ledgerEffectiveTime =
                        Timestamp.assertFromInstant(commands.ledgerEffectiveTime),
                      workflowId = commands.workflowId.fold("")(_.unwrap) // FIXME(JM): defaulting?
                    ),
                    transaction = updateTx
                  )
                }
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
