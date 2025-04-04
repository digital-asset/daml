// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package script
package v2

import org.apache.pekko.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.daml.lf.engine.free.Free
import com.digitalasset.daml.lf.engine.script.Runner.IdeLedgerContext
import com.digitalasset.daml.lf.engine.script.ledgerinteraction.{
  ScriptLedgerClient => UnversionedScriptLedgerClient
}
import com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient
import com.digitalasset.daml.lf.script.{IdeLedger, IdeLedgerRunner}
import com.digitalasset.daml.lf.speedy.{Profile, SExpr, SValue, Speedy, TraceLog, WarningLog}
import com.daml.script.converter.ConverterException
import com.digitalasset.canton.logging.NamedLoggerFactory

import scala.concurrent.{ExecutionContext, Future}

private[lf] class Runner(
    unversionedRunner: script.Runner,
    initialClients: Participants[UnversionedScriptLedgerClient],
    traceLog: TraceLog = Speedy.Machine.newTraceLog,
    warningLog: WarningLog = Speedy.Machine.newWarningLog,
    profile: Profile = Speedy.Machine.newProfile,
    canceled: () => Option[RuntimeException] = () => None,
) {
  import Free.Result, SExpr.SExpr

  implicit val namedLoggerFactory: NamedLoggerFactory =
    NamedLoggerFactory("daml-script", profile.name)

  private val initialClientsV2 = initialClients.map(
    ScriptLedgerClient.realiseScriptLedgerClient(
      _,
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

  private val knownPackages = ScriptF.KnownPackages(unversionedRunner.knownPackages)

  private val ideLedgerContext: Option[IdeLedgerContext] =
    initialClientsV2.default_participant.collect {
      case ledgerClient: ledgerinteraction.IdeLedgerClient =>
        new IdeLedgerContext {
          override def currentSubmission: Option[IdeLedgerRunner.CurrentSubmission] =
            ledgerClient.currentSubmission
          override def ledger: IdeLedger = ledgerClient.ledger
        }
    }

  def remapQ[X](result: Result[X, Free.Question, SExpr]): Result[X, ScriptF.Cmd, SExpr] =
    result.remapQ { case Free.Question(name, version, payload, stackTrace) =>
      ScriptF.parse(name, version, payload, knownPackages) match {
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

  def run(expr: SExpr.SExpr, convertLegacyExceptions: Boolean = true)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[SValue] =
    for {
      freeExpr <-
        Free.getResultF(
          expr,
          unversionedRunner.extendedCompiledPackages,
          traceLog,
          warningLog,
          profile,
          Script.DummyLoggingContext,
          convertLegacyExceptions,
          canceled,
        )
      result <-
        remapQ(freeExpr).runF[ScriptF.Cmd, SExpr](
          _.executeWithRunner(env, this, convertLegacyExceptions)
            .map(Result.successful)
            .recover { case err: RuntimeException => Result.failed(err) },
          canceled,
        )
    } yield result

  def getResult()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): (Future[SValue], Option[IdeLedgerContext]) =
    if (unversionedRunner.script.scriptIds.isLegacy)
      (
        Future.failed(
          new ConverterException(
            "Legacy daml-script is not supported in daml 3.3, please recompile your script using a daml 3.3+ SDK"
          )
        ),
        ideLedgerContext,
      )
    else
      (run(unversionedRunner.script.expr), ideLedgerContext)
}
