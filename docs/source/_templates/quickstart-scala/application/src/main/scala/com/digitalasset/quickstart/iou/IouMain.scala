// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.quickstart.iou

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.grpc.adapter.AkkaExecutionSequencerPool
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, WorkflowId}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.binding.Contract
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.quickstart.iou.ClientUtil.workflowIdFromParty
import com.daml.quickstart.iou.DecodeUtil.{decodeAllCreated, decodeArchived, decodeCreated}
import com.daml.quickstart.iou.FutureUtil.toFuture
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

// <doc-ref:imports>
import com.daml.ledger.client.binding.{Primitive => P}
import com.daml.quickstart.iou.model.{Iou => M}
// </doc-ref:imports>

object IouMain extends App with StrictLogging {

  if (args.length != 2) {
    logger.error("Usage: LEDGER_HOST LEDGER_PORT")
    System.exit(-1)
  }

  private val ledgerHost = args(0)
  private val ledgerPort = args(1).toInt

  // <doc-ref:issuer-definition>
  private val issuer = P.Party("Alice")
  // </doc-ref:issuer-definition>
  // <doc-ref:new-owner-definition>
  private val newOwner = P.Party("Bob")
  // </doc-ref:new-owner-definition>

  private val asys = ActorSystem()
  private val amat = Materializer(asys)
  private val aesf = new AkkaExecutionSequencerPool("clientPool")(asys)

  private def shutdown(): Unit = {
    logger.info("Shutting down...")
    Await.result(asys.terminate(), 10.seconds)
    ()
  }

  private implicit val ec: ExecutionContext = asys.dispatcher

  private val applicationId = ApplicationId("IOU Example")

  // <doc-ref:ledger-client-configuration>
  private val clientConfig = LedgerClientConfiguration(
    applicationId = ApplicationId.unwrap(applicationId),
    ledgerIdRequirement = LedgerIdRequirement.none,
    commandClient = CommandClientConfiguration.default,
    sslContext = None,
    token = None,
  )
  // </doc-ref:ledger-client-configuration>

  private val clientF: Future[LedgerClient] =
    LedgerClient.singleHost(ledgerHost, ledgerPort, clientConfig)(ec, aesf)

  private val clientUtilF: Future[ClientUtil] =
    clientF.map(client => new ClientUtil(client, applicationId))

  private val offset0F: Future[LedgerOffset] = clientUtilF.flatMap(_.ledgerEnd)

  private val issuerWorkflowId: WorkflowId = workflowIdFromParty(issuer)
  private val newOwnerWorkflowId: WorkflowId = workflowIdFromParty(newOwner)

  val newOwnerAcceptsAllTransfers: Future[Unit] = for {
    clientUtil <- clientUtilF
    offset0 <- offset0F
    // <doc-ref:subscribe-and-decode-iou-transfer>
    _ <- clientUtil.subscribe(newOwner, offset0, None) { tx =>
      logger.info(s"$newOwner received transaction: $tx")
      decodeCreated[M.IouTransfer](tx).foreach { contract: Contract[M.IouTransfer] =>
        logger.info(s"$newOwner received contract: $contract")
        // </doc-ref:subscribe-and-decode-iou-transfer>
        // <doc-ref:submit-iou-transfer-accept-exercise-command>
        val exerciseCmd = contract.contractId.exerciseIouTransfer_Accept(actor = newOwner)
        clientUtil.submitCommand(newOwner, newOwnerWorkflowId, exerciseCmd) onComplete {
          case Success(_) =>
            logger.info(s"$newOwner sent exercise command: $exerciseCmd")
            logger.info(s"$newOwner accepted IOU Transfer: $contract")
          case Failure(e) =>
            logger.error(s"$newOwner failed to send exercise command: $exerciseCmd", e)
        }
      // </doc-ref:submit-iou-transfer-accept-exercise-command>
      }
    }(amat)
  } yield ()

  // <doc-ref:iou-contract-instance>
  val iou = M.Iou(
    issuer = issuer,
    owner = issuer,
    currency = "USD",
    amount = BigDecimal("1000.00"),
    observers = List(),
  )
  // </doc-ref:iou-contract-instance>

  val issuerFlow: Future[Unit] = for {
    clientUtil <- clientUtilF
    offset0 <- offset0F
    _ = logger.info(s"Client API initialization completed, Ledger ID: ${clientUtil.toString}")

    // <doc-ref:submit-iou-create-command>
    createCmd = iou.create
    _ <- clientUtil.submitCommand(issuer, issuerWorkflowId, createCmd)
    _ = logger.info(s"$issuer created IOU: $iou")
    _ = logger.info(s"$issuer sent create command: $createCmd")
    // </doc-ref:submit-iou-create-command>

    tx0 <- clientUtil.nextTransaction(issuer, offset0)(amat)
    _ = logger.info(s"$issuer received transaction: $tx0")
    iouContract <- toFuture(decodeCreated[M.Iou](tx0))
    _ = logger.info(s"$issuer received contract: $iouContract")

    offset1 <- clientUtil.ledgerEnd

    // <doc-ref:iou-exercise-transfer-cmd>
    exerciseCmd = iouContract.contractId.exerciseIou_Transfer(actor = issuer, newOwner = newOwner)
    // </doc-ref:iou-exercise-transfer-cmd>
    _ <- clientUtil.submitCommand(issuer, issuerWorkflowId, exerciseCmd)
    _ = logger.info(s"$issuer sent exercise command: $exerciseCmd")
    _ = logger.info(s"$issuer transferred IOU: $iouContract to: $newOwner")

    tx1 <- clientUtil.nextTransaction(issuer, offset1)(amat)
    _ = logger.info(s"$issuer received final transaction: $tx1")
    archivedIouContractId <- toFuture(decodeArchived[M.Iou](tx1)): Future[P.ContractId[M.Iou]]
    _ = logger.info(
      s"$issuer received Archive Event for the original IOU contract ID: $archivedIouContractId"
    )
    _ <- Future(assert(iouContract.contractId == archivedIouContractId))
    iouTransferContract <- toFuture(decodeAllCreated[M.IouTransfer](tx1).headOption)
    _ = logger.info(s"$issuer received confirmation for the IOU Transfer: $iouTransferContract")

  } yield ()

  val returnCodeF: Future[Int] = issuerFlow.transform {
    case Success(_) =>
      logger.info("IOU flow completed.")
      Success(0)
    case Failure(e) =>
      logger.error("IOU flow completed with an error", e)
      Success(1)
  }

  val returnCode: Int = Await.result(returnCodeF, 10.seconds)
  shutdown()
  System.exit(returnCode)
}
