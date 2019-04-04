// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.ledger.acs.data

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.{Done, pattern}
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.digitalasset.ledger.api.v1.active_contracts_service.{
  GetActiveContractsRequest,
  GetActiveContractsResponse
}
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.client.services.acs.ActiveContractSetSource
import com.digitalasset.platform.common.util.DirectExecutionContext
import org.scalatest.Matchers
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.{ExecutionContext, Future}

trait AcsFutures extends Matchers with ScalaFutures {

  /**
    * Executes `init` once, then keeps trying `body` until it succeeds or we timeout.
    * This fn is recursive in the implicit [[ExecutionContext]], so one should avoid supplying `DirectExecutionContext`.
    */
  def waitForProperty[T, U](init: => Future[T])(
      body: T => Future[U])(implicit actorSystem: ActorSystem, ec: ExecutionContext): Future[U] = {
    val deadline = System.currentTimeMillis() + patienceConfig.timeout.toMillis

    def go(t: T): Future[U] = body(t).recoverWith {
      case failure: Exception =>
        if (System.currentTimeMillis() > deadline) {
          Future.failed(new RuntimeException("Timeout waiting for property to pass", failure))
        } else {
          pattern.after(patienceConfig.interval, actorSystem.scheduler)(go(t))
        }
    }

    // Don't race with scalatest timeouts. go() will timeout with the same patience config
    init.flatMap(go)(DirectExecutionContext)
  }

  /**
    * Keep trying an ACS query until we get the number of responses we expect, or we timeout.
    * This is a recursive loop, and using a DirectExecutionContext may risk stack overflow.
    * Also asserts we don't get more contracts than expected.
    */
  def waitForActiveContracts(
      service: ActiveContractsService,
      ledgerId: String,
      transactionFilter: Map[String, Filters],
      expectedCount: Int,
      verbose: Boolean = false)(
      implicit actorSystem: ActorSystem,
      am: ActorMaterializer,
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory): Future[Seq[GetActiveContractsResponse]] =
    waitForProperty(Future.successful(Done)) { _ =>
      ActiveContractSetSource(
        service.getActiveContracts,
        GetActiveContractsRequest(ledgerId, Some(TransactionFilter(transactionFilter)), verbose))
        .runWith(Sink.collection) flatMap { seq: Seq[GetActiveContractsResponse] =>
        val contractCount = seq.foldLeft(0)({ case (i, resp) => i + resp.activeContracts.length })

        if (contractCount == expectedCount) {
          Future.successful(seq)
        } else {
          Future(fail(s"Expected $expectedCount contracts, got $contractCount before timeout"))(
            DirectExecutionContext)
        }
      }
    }
}
