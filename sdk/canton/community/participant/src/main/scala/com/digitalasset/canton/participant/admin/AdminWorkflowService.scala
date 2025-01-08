// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.syntax.either.*
import com.daml.ledger.api.v2.reassignment.Reassignment
import com.daml.ledger.api.v2.state_service.ActiveContract
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.TransactionFilter
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.participant.ledger.api.client.CommandResult
import com.digitalasset.canton.tracing.TraceContext

import scala.util.chaining.scalaUtilChainingOps

/** Admin service that connects to the ledger and process events
  */
trait AdminWorkflowService extends NamedLogging with AutoCloseable {

  /** The filters for this service */
  private[admin] def filters: TransactionFilter

  /** Processing the transaction must not block; shutdown problems occur otherwise.
    * Long-running computations or blocking calls should be spawned off into an asynchronous computation
    * so that the service itself can synchronize its closing with the spawned-off computation if needed.
    */
  private[admin] def processTransaction(tx: Transaction): Unit

  /** Process a reassignment */
  private[admin] def processReassignment(tx: Reassignment): Unit

  /** Process the initial active contract set for this service */
  private[admin] def processAcs(acs: Seq[ActiveContract])(implicit
      traceContext: TraceContext
  ): Unit

  protected def handleCommandResult(
      operation: String
  )(result: CommandResult)(implicit traceContext: TraceContext): Either[String, Unit] =
    result match {
      case CommandResult.Success(transactionId) =>
        logger.info(
          s"Successfully submitted $operation with transactionId=$transactionId, waiting for response"
        )
        Either.unit[String]
      case CommandResult.Failed(_, errorStatus) =>
        Left(s"Failed $operation: Failed to submit $operation: $errorStatus".tap(logger.warn(_)))
      case CommandResult.AbortedDueToShutdown =>
        Left(s"Failed $operation: Aborted $operation due to shutdown".tap(logger.info(_)))
      case CommandResult.TimeoutReached(_, lastErrorStatus) =>
        Left(
          s"Failed $operation: Timeout out while attempting to submit $operation: $lastErrorStatus"
            .tap(logger.info(_))
        )
    }

}
