// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.quickstart.iou

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.grpc.adapter.AkkaExecutionSequencerPool
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, WorkflowId}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.daml.quickstart.iou.ClientUtil.workflowIdFromParty
import com.daml.quickstart.iou.DecodeUtil.decodeCreatedEvent
import com.daml.quickstart.iou.FutureUtil.toFuture
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

object IouMain extends App with StrictLogging {

  if (args.length != 3) {
    logger.error("Usage: LEDGER_HOST LEDGER_PORT IOU_PACKAGE_ID")
    System.exit(-1)
  }

  private val ledgerHost = args(0)
  private val ledgerPort = args(1).toInt
  private val packageId = args(2)

  private val iouTemplateId =
    Identifier(packageId = packageId, moduleName = "Iou", entityName = "Iou")

  private val issuer = "Alice"
  private val newOwner = "Bob"

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

  private val clientConfig = LedgerClientConfiguration(
    applicationId = ApplicationId.unwrap(applicationId),
    ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
    commandClient = CommandClientConfiguration.default,
    sslContext = None
  )

  private val clientF: Future[LedgerClient] =
    LedgerClient.singleHost(ledgerHost, ledgerPort, clientConfig)(ec, aesf)

  private val clientUtilF: Future[ClientUtil] =
    clientF.map(client => new ClientUtil(client, applicationId))

  private val offset0F: Future[LedgerOffset] = clientUtilF.flatMap(_.ledgerEnd)

  private val issuerWorkflowId: WorkflowId = workflowIdFromParty(issuer)
  private val newOwnerWorkflowId: WorkflowId = workflowIdFromParty(newOwner)

  def validatePackageId(allPackageIds: Set[String], packageId: String): Future[Unit] =
    if (allPackageIds(packageId)) Future.successful(())
    else
      Future.failed(
        new IllegalArgumentException(
          s"Uknown package ID passed: $packageId, all package IDs: $allPackageIds"))

  val issuerFlow: Future[Unit] = for {
    clientUtil <- clientUtilF
    offset0 <- offset0F
    _ = logger.info(s"Client API initialization completed, Ledger ID: ${clientUtil.toString}")

    allPackageIds <- clientUtil.listPackages
    _ = logger.info(s"All package IDs: $allPackageIds")

    _ <- validatePackageId(allPackageIds, packageId)

    createCmd = IouCommands.iouCreateCommand(
      iouTemplateId,
      "Alice",
      "Alice",
      "USD",
      BigDecimal("99999.00"))
    _ <- clientUtil.submitCommand(issuer, issuerWorkflowId, createCmd)
    _ = logger.info(s"$issuer sent create command: ${createCmd.toString}")

    tx0 <- clientUtil.nextTransaction(issuer, offset0)(amat)
    _ = logger.info(s"$issuer received transaction: ${tx0.toString}")

    createdEvent <- toFuture(decodeCreatedEvent(tx0))
    _ = logger.info(s"$issuer received created event: ${createdEvent.toString}")

    exerciseCmd = IouCommands.iouTransferExerciseCommand(
      iouTemplateId,
      createdEvent.contractId,
      newOwner)
    _ <- clientUtil.submitCommand(issuer, issuerWorkflowId, exerciseCmd)
    _ = logger.info(s"$issuer sent exercise command: ${exerciseCmd.toString}")

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
