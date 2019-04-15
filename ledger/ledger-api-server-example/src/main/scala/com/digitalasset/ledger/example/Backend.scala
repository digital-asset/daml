// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.example

import akka.NotUsed
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.VersionedValue
import com.digitalasset.ledger.backend.api.v1._
import com.digitalasset.platform.sandbox.config.DamlPackageContainer
import com.digitalasset.platform.services.time.TimeModel
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

/**
  * This is the class that provides the LedgerBackend services. It uses an example in-memory
  * ledger internally, to implement the desired semantics.
  *
  * @param thisLedgerId
  * @param packages
  * @param timeModel
  * @param timeProvider
  * @param mat
  */
class Backend(
    thisLedgerId: String,
    packages: DamlPackageContainer,
    timeModel: TimeModel,
    timeProvider: TimeProvider,
    mat: ActorMaterializer)
    extends LedgerBackend {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val ledger = new Ledger(packages, timeModel, timeProvider, mat)
  private val ec = mat.system.dispatcher

  override def ledgerId: String = this.thisLedgerId

  override def beginSubmission(): Future[SubmissionHandle] = {
    logger.debug("beginSubmission called.")
    getCurrentLedgerEnd.flatMap(offset => Future.successful(new Handle(ledger, offset, ec)))(ec)
  }

  override def ledgerSyncEvents(
      offset: Option[LedgerSyncOffset]): Source[LedgerSyncEvent, NotUsed] = {
    logger.debug("ledgerSyncEvents called.")
    ledger
      .ledgerSyncEvents(offset)
      .map(event => {
        logger.debug("Publishing event: " + event.toString)
        event
      })
  }

  override def activeContractSetSnapshot()
    : Future[(LedgerSyncOffset, Source[ActiveContract, NotUsed])] = {
    logger.debug("activeContractSetSnapshot called.")
    val (offset, snapshot) = ledger.activeContractSetSnapshot()
    val msgs = snapshot.toList.map {
      case (contractId, contract) =>
        ActiveContract(
          contractId,
          contract.let,
          contract.transactionId,
          contract.workflowId,
          contract.contract,
          contract.witnesses)
    }
    Future.successful {
      (
        offset,
        Source
          .fromIterator(
            () => msgs.toIterator
          ))
    }
  }

  override def getCurrentLedgerEnd: Future[LedgerSyncOffset] = {
    logger.debug("getCurrentLedgerEnd called.")
    Future.successful(ledger.getCurrentLedgerEnd)
  }

  def shutdownTasks(): Unit = {
    logger.debug("shutdownTasks called.")
    ledger.shutdownTasks()
  }

  override def close(): Unit = {}

}

class Handle(ledger: Ledger, ledgerSyncOffset: LedgerSyncOffset, ec: ExecutionContext)
    extends SubmissionHandle {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def abort: Future[Unit] = throw new UnsupportedOperationException()

  /**
    * Submit a transaction asynchronously.
    *
    * @param submission
    * @return
    */
  override def submit(submission: TransactionSubmission): Future[Unit] = {
    logger.debug("submit called: " + submission.toString)
    Future {
      ledger.submitAndNotify(submission)
    }(ec)
  }

  override def lookupActiveContract(submitter: Party, contractId: Value.AbsoluteContractId)
    : Future[Option[Value.ContractInst[VersionedValue[Value.AbsoluteContractId]]]] = {
    logger.debug("lookupActiveContract called: " + contractId.toString)
    Future.successful {
      ledger
        .lookupActiveContract(contractId)
        .map(_.contract)
    }
  }

  override def lookupContractKey(key: Node.GlobalKey): Future[Option[Value.AbsoluteContractId]] =
    sys.error("contract keys not implemented in example backend")

}
