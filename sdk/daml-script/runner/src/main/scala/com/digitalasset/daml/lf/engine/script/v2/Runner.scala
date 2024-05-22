// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package script
package v2

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.lf.engine.free.Free
import com.daml.lf.engine.script.Runner.IdeLedgerContext
import com.daml.lf.engine.script.ledgerinteraction.{
  ScriptLedgerClient => UnversionedScriptLedgerClient
}
import com.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient
import com.daml.lf.language.LanguageVersionRangeOps._
import com.daml.lf.scenario.{ScenarioLedger, ScenarioRunner}
import com.daml.lf.speedy._
import com.daml.script.converter.ConverterException
import org.apache.pekko.stream.Materializer

import scala.concurrent.{ExecutionContext, Future}

private[lf] class Runner(
    unversionedRunner: script.Runner,
    initialClients: Participants[UnversionedScriptLedgerClient],
    traceLog: TraceLog = Speedy.Machine.newTraceLog,
    warningLog: WarningLog = Speedy.Machine.newWarningLog,
    profile: Profile = Speedy.Machine.newProfile,
    canceled: () => Option[RuntimeException] = () => None,
) {
  import Free.Result
  import SExpr.SExpr

  val scriptF = new ScriptF(
    unversionedRunner.compiledPackages.compilerConfig.allowedLanguageVersions.majorVersion
  )

  private val initialClientsV2 = initialClients.map(
    ScriptLedgerClient.realiseScriptLedgerClient(
      _,
      unversionedRunner.enableContractUpgrading,
      unversionedRunner.extendedCompiledPackages,
    )
  )

  private val env =
    new ScriptF.Env(
      unversionedRunner.script.scriptIds,
      unversionedRunner.timeMode,
      initialClientsV2,
      unversionedRunner.extendedCompiledPackages,
    )

  private val knownPackages = scriptF.KnownPackages(unversionedRunner.knownPackages)

  private val ideLedgerContext: Option[IdeLedgerContext] =
    initialClientsV2.default_participant.collect {
      case ledgerClient: ledgerinteraction.IdeLedgerClient =>
        new IdeLedgerContext {
          override def currentSubmission: Option[ScenarioRunner.CurrentSubmission] =
            ledgerClient.currentSubmission
          override def ledger: ScenarioLedger = ledgerClient.ledger
        }
    }

  def remapQ[X](result: Result[X, Free.Question, SExpr]): Result[X, scriptF.Cmd, SExpr] =
    result.remapQ { case Free.Question(name, version, payload, stackTrace) =>
      scriptF.parse(name, version, payload, knownPackages) match {
        case Right(cmd) =>
          Result.Ask(
            cmd,
            {
              case Right(value) =>
                Result.successful(value)
              case Left(
                    e @ (_: free.InterpretationError | script.Runner.CanceledByRequest |
                    script.Runner.TimedOut)
                  ) =>
                Result.failed(e)
              case Left(err) =>
                Result.failed(Script.FailedCmd(name, stackTrace, err))
            },
          )
        case Left(err) =>
          Result.failed(new ConverterException(err))
      }
    }

  def run(expr: SExpr.SExpr)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[SValue] =
    remapQ(
      Free.getResult(
        expr,
        unversionedRunner.extendedCompiledPackages,
        traceLog,
        warningLog,
        profile,
        Script.DummyLoggingContext,
      )
    ).runF[scriptF.Cmd, SExpr](
      _.executeWithRunner(env, this)
        .map(Result.successful)
        .recover { case err: RuntimeException => Result.failed(err) },
      canceled,
    )

  def getResult()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): (Future[SValue], Option[IdeLedgerContext]) =
    (run(unversionedRunner.script.expr), ideLedgerContext)
}
