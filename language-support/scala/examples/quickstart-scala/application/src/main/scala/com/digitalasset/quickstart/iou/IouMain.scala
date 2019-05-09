// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.quickstart.iou

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.grpc.adapter.AkkaExecutionSequencerPool
import com.digitalasset.ledger.api.refinements.ApiTypes.{ApplicationId, WorkflowId}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.binding.{Contract, Primitive => P}
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.digitalasset.quickstart.iou.ClientUtil.workflowIdFromParty
import com.digitalasset.quickstart.iou.DecodeUtil.{decodeAllCreated, decodeArchived, decodeCreated}
import com.digitalasset.quickstart.iou.FutureUtil.toFuture
import com.digitalasset.quickstart.iou.model.{Iou => M}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object IouMain extends App with StrictLogging {

  if (args.length != 2) {
    logger.error("Usage: LEDGER_HOST LEDGER_PORT")
    System.exit(-1)
  }

  private val ledgerHost = args(0)
  private val ledgerPort = args(1).toInt

  private val issuer = P.Party("Alice")
  private val newOwner = P.Party("Bob")

  private val asys = ActorSystem()
  private val amat = ActorMaterializer()(asys)
  private val aesf = new AkkaExecutionSequencerPool("clientPool")(asys)

  private implicit val ec: ExecutionContext = asys.dispatcher

  private val applicationId = ApplicationId("IOU Example")

  private val timeProvider = TimeProvider.Constant(Instant.EPOCH)

  private val clientConfig = LedgerClientConfiguration(
    applicationId = ApplicationId.unwrap(applicationId),
    ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
    commandClient = CommandClientConfiguration.default,
    sslContext = None
  )

  private val clientF: Future[LedgerClient] =
    LedgerClient.singleHost(ledgerHost, ledgerPort, clientConfig)(ec, aesf)

  private val clientUtilF: Future[ClientUtil] =
    clientF.map(client => new ClientUtil(client, applicationId, 30.seconds, timeProvider))

  private val offset0F: Future[LedgerOffset] = clientUtilF.flatMap(_.ledgerEnd)

  private val issuerWorkflowId: WorkflowId = workflowIdFromParty(issuer)
  private val newOwnerWorkflowId: WorkflowId = workflowIdFromParty(newOwner)

  val newOwnerAcceptsAllTransfers: Future[Unit] = for {
    clientUtil <- clientUtilF
    offset0 <- offset0F
    _ <- clientUtil.subscribe(newOwner, offset0, None) { tx =>
      logger.info(s"$newOwner received transaction: $tx")
      decodeCreated[M.IouTransfer](tx).foreach { contract: Contract[M.IouTransfer] =>
        logger.info(s"$newOwner received contract: $contract")
        val exerciseCmd = contract.contractId.exerciseIouTransfer_Accept(actor = newOwner)
        clientUtil.submitCommand(newOwner, newOwnerWorkflowId, exerciseCmd) onComplete {
          case Success(_) =>
            logger.info(s"$newOwner sent exercise command: $exerciseCmd")
            logger.info(s"$newOwner accepted IOU Transfer: $contract")
          case Failure(e) =>
            logger.error(s"$newOwner failed to send exercise command: $exerciseCmd", e)
        }
      }
    }(amat)
  } yield ()

  val iou = M.Iou(
    issuer = issuer,
    owner = issuer,
    currency = "USD",
    amount = BigDecimal("1000.00"),
    observers = List())

  val issuerFlow: Future[Unit] = for {
    clientUtil <- clientUtilF
    offset0 <- offset0F
    _ = logger.info(s"Client API initialization completed, Ledger ID: ${clientUtil.toString}")

    createCmd = iou.create
    _ <- clientUtil.submitCommand(issuer, issuerWorkflowId, createCmd)
    _ = logger.info(s"$issuer created IOU: $iou")
    _ = logger.info(s"$issuer sent create command: $createCmd")

    tx0 <- clientUtil.nextTransaction(issuer, offset0)(amat)
    _ = logger.info(s"$issuer received transaction: $tx0")
    iouContract <- toFuture(decodeCreated[M.Iou](tx0))
    _ = logger.info(s"$issuer received contract: $iouContract")

    offset1 <- clientUtil.ledgerEnd

    exerciseCmd = iouContract.contractId.exerciseIou_Transfer(actor = issuer, newOwner = newOwner)
    _ <- clientUtil.submitCommand(issuer, issuerWorkflowId, exerciseCmd)
    _ = logger.info(s"$issuer sent exercise command: $exerciseCmd")
    _ = logger.info(s"$issuer transferred IOU: $iouContract to: $newOwner")

    tx1 <- clientUtil.nextTransaction(issuer, offset1)(amat)
    _ = logger.info(s"$issuer received final transaction: $tx1")
    archivedIouContractId <- toFuture(decodeArchived[M.Iou](tx1)): Future[P.ContractId[M.Iou]]
    _ = logger.info(
      s"$issuer received Archive Event for the original IOU contract ID: $archivedIouContractId")
    _ <- Future(assert(iouContract.contractId == archivedIouContractId))
    iouTransferContract <- toFuture(decodeAllCreated[M.IouTransfer](tx1).headOption)
    _ = logger.info(s"$issuer received confirmation for the IOU Transfer: $iouTransferContract")

  } yield ()

  val returnCode = Try(Await.result(issuerFlow, Duration.Inf)) match {
    case Success(a) =>
      logger.info(s"Terminating with result: $a")
      0
    case Failure(e) =>
      logger.error(s"Terminating with error", e)
      1
  }

  asys.terminate()
  System.exit(returnCode)
}
