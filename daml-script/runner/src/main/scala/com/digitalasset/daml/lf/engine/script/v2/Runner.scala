// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package script
package v2

import akka.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.lf.engine.free.Free
import com.daml.lf.engine.script.Runner.IdeLedgerContext
import com.daml.lf.engine.script.ledgerinteraction.{
  ScriptLedgerClient => UnversionedScriptLedgerClient
}
import com.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient
import com.daml.lf.scenario.{ScenarioLedger, ScenarioRunner}
import com.daml.lf.speedy.{SExpr, SValue, Speedy, Profile, TraceLog, WarningLog}
import com.daml.script.converter.ConverterException

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}

private[lf] class Runner(
    unversionedRunner: script.Runner,
    initialClients: Participants[UnversionedScriptLedgerClient],
    traceLog: TraceLog = Speedy.Machine.newTraceLog,
    warningLog: WarningLog = Speedy.Machine.newWarningLog,
    profile: Profile = Speedy.Machine.newProfile,
    canceled: () => Option[RuntimeException] = () => None,
) {
  import free.Result

  private val initialClientsV1 = initialClients.map(ScriptLedgerClient.realiseScriptLedgerClient)

  private val env =
    new ScriptF.Env(
      unversionedRunner.script.scriptIds,
      unversionedRunner.timeMode,
      initialClientsV1,
      unversionedRunner.extendedCompiledPackages,
    )

  private val ideLedgerContext: Option[IdeLedgerContext] =
    initialClientsV1.default_participant.collect {
      case ledgerClient: ledgerinteraction.IdeLedgerClient =>
        new IdeLedgerContext {
          override def currentSubmission: Option[ScenarioRunner.CurrentSubmission] =
            ledgerClient.currentSubmission
          override def ledger: ScenarioLedger = ledgerClient.ledger
        }
    }

  def remapQ[X](result: Result[X, Free.Question]): Result[X, ScriptF.Cmd] =
    result match {
      case Result.Final(x) => Result.Final(x)
      case Result.Interruption(resume) => Result.Interruption(() => remapQ(resume()))
      case Result.Question(
            Free.Question(name, version, payload, stackTrace),
            lfContinue,
            continue,
          ) =>
        ScriptF.parse(name, version, payload, stackTrace) match {
          case Left(value) => Result.failed(new ConverterException(value))
          case Right(value) => Result.Question(value, lfContinue, x => remapQ(continue(x)))
        }
    }

  def consume[X](
      result: Result[X, ScriptF.Cmd]
  )(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory, mat: Materializer): Future[X] = {
    canceled() match {
      case Some(err) => Future.failed(err)
      case None =>
        Future(result).flatMap {
          case Result.Final(x) => Future.fromTry(x.toTry)
          case Result.Interruption(resume) => consume(resume())
          case q @ Result.Question(p, _, _) =>
            p.executeWithRunner(env, this).transformWith {
              case Success(x) => consume(q.resume(Right(x)))
              case Failure(err: RuntimeException) => consume(q.resume(Left(err)))
              case Failure(err) => Future.failed(err)
            }
        }
    }
  }

  def run(expr: SExpr.SExpr)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[SValue] =
    consume(
      remapQ(
        Free
          .run(
            expr,
            unversionedRunner.extendedCompiledPackages,
            traceLog,
            warningLog,
            profile,
            Script.DummyLoggingContext,
          )
      )
    )

  def getResult()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): (Future[SValue], Option[IdeLedgerContext]) = {
    (run(unversionedRunner.script.expr), ideLedgerContext)
  }
}
