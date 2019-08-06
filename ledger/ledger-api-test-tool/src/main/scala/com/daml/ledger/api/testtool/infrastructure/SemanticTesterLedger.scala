// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.digitalasset.daml.lf.command.Commands
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml.lf.engine.Event
import com.digitalasset.daml.lf.engine.testing.SemanticTester
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.ledger.api.v1.transaction.TransactionTree
import com.digitalasset.platform.common.PlatformTypes.Events
import com.digitalasset.platform.participant.util.LfEngineToApi

import scala.concurrent.Future
import scala.concurrent.duration.DurationLong

private[infrastructure] final class SemanticTesterLedger(bindings: LedgerBindings)(
    parties: Set[Ref.Party],
    packages: Map[Ref.PackageId, Ast.Package])(implicit context: LedgerTestContext)
    extends SemanticTester.GenericLedger {

  private def lfCommandToApiCommand(party: String, commands: Commands) =
    for (ledgerId <- context.ledgerId)
      yield
        LfEngineToApi.lfCommandToApiCommand(
          party,
          ledgerId,
          commands.commandsReference,
          context.applicationId,
          Some(LfEngineToApi.toTimestamp(commands.ledgerEffectiveTime.toInstant)),
          Some(LfEngineToApi.toTimestamp(commands.ledgerEffectiveTime.toInstant.plusSeconds(30L))),
          commands
        )

  private val apiScenarioTransform = for (ledgerId <- context.ledgerId)
    yield new ApiScenarioTransform(ledgerId, packages)

  private def apiTransactionToLfEvents(
      tree: TransactionTree): Future[Events[String, Value.AbsoluteContractId]] =
    for {
      transform <- apiScenarioTransform
      result = transform.eventsFromApiTransaction(tree)
      future <- result.fold(Future.failed, Future.successful)
    } yield future

  override type EventNodeId = String

  override def submit(
      party: Party,
      lfCommands: Commands,
      opDescription: String): Future[Event.Events[
    EventNodeId,
    Value.AbsoluteContractId,
    Value.VersionedValue[Value.AbsoluteContractId]]] =
    for {
      apiCommands <- lfCommandToApiCommand(party, lfCommands)
      command +: commands = apiCommands.commands.map(_.command)
      id <- bindings.submitAndWaitForTransactionId(
        party,
        context.applicationId,
        command,
        commands: _*)
      tree <- bindings.getTransactionById(id, parties.toSeq)
      events <- apiTransactionToLfEvents(tree)
    } yield events

  override def passTime(dtMicros: Long): Future[Unit] =
    bindings.passTime(dtMicros.micros)

  override def currentTime: Future[Time.Timestamp] =
    bindings.time.map(
      Time.Timestamp.fromInstant(_).fold(reason => throw new RuntimeException(reason), identity))

}
