// Copyright (c) 2019 The DAML Authors. All rights reserved.
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
import com.digitalasset.ledger.client.binding.Primitive
import com.digitalasset.platform.common.PlatformTypes.Events
import com.digitalasset.platform.participant.util.LfEngineToApi

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationLong

private[testtool] final class SemanticTesterLedger(
    ledger: LedgerTestContext,
    parties: Set[Ref.Party],
    packages: Map[Ref.PackageId, Ast.Package])(implicit ec: ExecutionContext)
    extends SemanticTester.GenericLedger {

  private def lfCommandToApiCommand(party: String, commands: Commands) =
    LfEngineToApi.lfCommandToApiCommand(
      party,
      ledger.id,
      commands.commandsReference,
      ledger.applicationId,
      Some(LfEngineToApi.toTimestamp(commands.ledgerEffectiveTime.toInstant)),
      Some(LfEngineToApi.toTimestamp(commands.ledgerEffectiveTime.toInstant.plusSeconds(30L))),
      commands
    )

  private val apiScenarioTransform = new ApiScenarioTransform(ledger.id, packages)

  private def apiTransactionToLfEvents(
      tree: TransactionTree): Future[Events[String, Value.AbsoluteContractId]] =
    apiScenarioTransform.eventsFromApiTransaction(tree).fold(Future.failed, Future.successful)

  override type EventNodeId = String

  override def submit(
      party: Party,
      lfCommands: Commands,
      opDescription: String): Future[Event.Events[
    EventNodeId,
    Value.AbsoluteContractId,
    Value.VersionedValue[Value.AbsoluteContractId]]] =
    for {
      request <- ledger.submitAndWaitRequest(
        Primitive.Party(party),
        lfCommandToApiCommand(party, lfCommands).commands: _*)
      id <- ledger.submitAndWaitForTransactionId(request)
      tree <- ledger.transactionTreeById(id, parties.toSeq.map(Primitive.Party(_: String)): _*)
      events <- apiTransactionToLfEvents(tree)
    } yield events

  override def passTime(dtMicros: Long): Future[Unit] =
    ledger.passTime(dtMicros.micros)

  override def currentTime: Future[Time.Timestamp] =
    ledger
      .time()
      .map(
        Time.Timestamp.fromInstant(_).fold(reason => throw new RuntimeException(reason), identity))

}
