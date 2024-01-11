// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.daml.ledger.api.v1.commands.Command as ScalaCommand
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.package_service.{GetPackageStatusResponse, PackageStatus}
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.javaapi.data.Party
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.ledger.api.refinements.ApiTypes.WorkflowId
import com.digitalasset.canton.lifecycle.AsyncOrSyncCloseable
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.ledger.api.client.{CommandResult, LedgerAcs}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TraceContext

import java.time.Duration
import scala.concurrent.{Future, Promise}

/** Mock for capturing a single submitted command.
  */
class MockLedgerAcs(override val logger: TracedLogger, override val sender: Party)
    extends LedgerAcs {
  private val lastCommandPromise = Promise[ScalaCommand]()
  val lastCommand: Future[ScalaCommand] = lastCommandPromise.future
  override val timeouts = DefaultProcessingTimeouts.testing

  override def submitCommand(
      command: Seq[ScalaCommand],
      commandId: Option[String] = None,
      workflowId: Option[WorkflowId] = None,
      deduplictionTime: Option[NonNegativeFiniteDuration] = None,
      timeout: Option[Duration] = None,
  )(implicit traceContext: TraceContext): Future[CommandResult] = {
    command.headOption.foreach(lastCommandPromise.trySuccess)
    Future.successful(CommandResult.Success("someTxId"))
  }

  override def submitAsync(
      commands: Seq[ScalaCommand],
      commandId: Option[String],
      workflowId: Option[WorkflowId],
      deduplicationTime: Option[NonNegativeFiniteDuration] = None,
  )(implicit traceContext: TraceContext): Future[Unit] =
    throw new IllegalArgumentException("Method not defined")

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = List.empty

  override def activeContracts(
      filter: TransactionFilter
  ): Future[(Seq[CreatedEvent], LedgerOffset)] =
    Future.successful((Seq(), LedgerOffset(value = LedgerOffset.Value.Empty)))

  override def getPackageStatus(packageId: String): Future[GetPackageStatusResponse] =
    Future.successful(GetPackageStatusResponse(PackageStatus.REGISTERED))
}
