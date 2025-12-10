// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package script
package v2

import org.apache.pekko.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.engine.free.Free
import com.digitalasset.daml.lf.engine.script.Runner.IdeLedgerContext
import com.digitalasset.daml.lf.engine.script.ledgerinteraction.{
  ScriptLedgerClient => UnversionedScriptLedgerClient
}
import com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient
import com.digitalasset.daml.lf.script.{IdeLedger, IdeLedgerRunner}
import com.digitalasset.daml.lf.speedy.{Profile, TraceLog, WarningLog}
import com.digitalasset.daml.lf.speedy.Speedy.Machine.{
  ExtendedValue,
  ExtendedValueClosureBlob,
  ExtendedValueComputationMode,
  runExtendedValueComputation,
  newTraceLog,
  newWarningLog,
  newProfile,
}
import com.digitalasset.daml.lf.value.Value._
import com.digitalasset.daml.lf.script.converter.ConverterException
import com.digitalasset.canton.logging.NamedLoggerFactory

import scala.concurrent.{ExecutionContext, Future}

private[lf] class Runner(
    unversionedRunner: script.Runner,
    initialClients: Participants[UnversionedScriptLedgerClient],
    traceLog: TraceLog = newTraceLog,
    warningLog: WarningLog = newWarningLog,
    profile: Profile = newProfile,
    canceled: () => Option[RuntimeException] = () => None,
) {
  import Free.Result

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

  def remapQ[X](
      result: Result[X, Free.Question, ExtendedValue]
  ): Result[X, ScriptF.Cmd, ExtendedValue] =
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

  // Takes a Script X and runs it
  def runResolved(scriptValue: ExtendedValue, convertLegacyExceptions: Boolean = true)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[ExtendedValue] =
    for {
      freeClosure <- scriptValue match {
        case ValueRecord(_, ImmArray((_, freeClosure: ExtendedValueClosureBlob), _)) =>
          Future.successful(freeClosure)
        case a => Future.failed(new RuntimeException(s"Expected Script a but got $a"))
      }
      freeExpr <-
        Free.getResultF(
          freeClosure,
          unversionedRunner.extendedCompiledPackages,
          traceLog,
          warningLog,
          profile,
          Script.DummyLoggingContext,
          convertLegacyExceptions,
          canceled,
        )
      result <-
        remapQ(freeExpr).runF[ScriptF.Cmd, ExtendedValue](
          _.executeWithRunner(env, this, convertLegacyExceptions)
            .map(Result.successful)
            .recover { case err: RuntimeException => Result.failed(err) }
        )
    } yield result

  // Takes something that resolves/computes to a Script X, then runs the script
  def run(comp: ExtendedValueComputationMode, convertLegacyExceptions: Boolean = true)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[ExtendedValue] =
    for {
      scriptValue <- runComputation(comp, convertLegacyExceptions)
      result <- runResolved(scriptValue, convertLegacyExceptions)
    } yield result

  def runComputation(
      comp: ExtendedValueComputationMode,
      convertLegacyExceptions: Boolean = true,
  ): Future[ExtendedValue] =
    runExtendedValueComputation(
      comp,
      canceled,
      unversionedRunner.extendedCompiledPackages,
      iterationsBetweenInterruptions = 100000,
      traceLog,
      warningLog,
      profile,
      convertLegacyExceptions,
    )(Script.DummyLoggingContext).fold(
      err => Future.failed(err.fold(identity, script.Runner.InterpretationError(_))),
      Future.successful(_),
    )

  def getResult()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): (Future[ExtendedValue], Option[IdeLedgerContext]) =
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
      (
        unversionedRunner.script match {
          case ScriptAction.NoParam(id, _) =>
            run(ExtendedValueComputationMode.ByIdentifier(id))
          case ScriptAction.Param(id, paramType, Some(param), _) =>
            run(ExtendedValueComputationMode.ByIdentifier(id, Some(List(param))))
          case _ =>
            Future.failed(
              new RuntimeException("impossible")
            ) // This case is caught by script.Runner, when a Param ScriptAction is called without a param
        },
        ideLedgerContext,
      )
}
