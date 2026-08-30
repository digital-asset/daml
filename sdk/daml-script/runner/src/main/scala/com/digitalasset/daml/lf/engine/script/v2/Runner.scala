// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package script
package v2

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.engine.ScriptEngine.{
  ExtendedValue,
  ExtendedValueClosureBlob,
  ExtendedValueComputationMode,
  runExtendedValueComputation,
}
import com.digitalasset.daml.lf.engine.free.Free
import com.digitalasset.daml.lf.engine.script.Runner.IdeLedgerContext
import com.digitalasset.daml.lf.engine.script.ledgerinteraction.{
  ScriptLedgerClient => UnversionedScriptLedgerClient
}
import com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.script.IdeLedger
import com.digitalasset.daml.lf.script.converter.ConverterException
import com.digitalasset.daml.lf.speedy.{MachineLogger, SError}
import com.digitalasset.daml.lf.transaction.{NextGenContractStateMachine => ContractStateMachine}
import com.digitalasset.daml.lf.value.Value
import org.apache.pekko.stream.Materializer

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

private[lf] class Runner(
    unversionedRunner: script.Runner,
    initialClients: Participants[UnversionedScriptLedgerClient],
    machineLogger: MachineLogger = ScriptMachineLogger(),
    canceled: () => Option[RuntimeException] = () => None,
    csmMode: ContractStateMachine.Mode = ContractStateMachine.Mode.Key,
) {
  import Free.Result

  private val loggerFactory: NamedLoggerFactory =
    NamedLoggerFactory("daml-script", "Daml Script")

  private val initialClientsV2 = initialClients.map(
    ScriptLedgerClient.realiseScriptLedgerClient(
      _,
      unversionedRunner.extendedCompiledPackages,
      loggerFactory,
      csmMode,
    )
  )

  private val env =
    new ScriptF.Env(
      unversionedRunner.script.scriptIds,
      unversionedRunner.timeMode,
      initialClientsV2,
      unversionedRunner.extendedCompiledPackages,
      loggerFactory,
      traceContext = TraceContext.empty,
    )

  private val knownPackages = ScriptF.KnownPackages(unversionedRunner.knownPackages)

  private val ideLedgerContext: Option[IdeLedgerContext] =
    initialClientsV2.default_participant.collect {
      case ledgerClient: ledgerinteraction.IdeLedgerClient =>
        new IdeLedgerContext {
          override def currentSubmission: Option[CurrentSubmission] = ledgerClient.currentSubmission
          override def ledger: IdeLedger = ledgerClient.ledger
        }
    }

  def remapQ[X](
      result: Result[X, Free.Question, ExtendedValue]
  ): Result[X, ScriptF.Cmd, ExtendedValue] =
    result.remapQ { case Free.Question(name, version, payload, stackTrace) =>
      ScriptF.parse(name, version, payload, knownPackages, env) match {
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
  def runResolved(scriptValue: ExtendedValue, convertLegacyExceptions: Boolean)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[ExtendedValue] = handleLegacyExceptions(convertLegacyExceptions)(
    for {
      freeClosure <- scriptValue match {
        case Value.ValueRecord(_, ImmArray((_, freeClosure: ExtendedValueClosureBlob), _)) =>
          Future.successful(freeClosure)
        case a => Future.failed(new RuntimeException(s"Expected Script a but got $a"))
      }
      freeExpr <-
        Free.getResultF(
          freeClosure,
          unversionedRunner.extendedCompiledPackages,
          machineLogger,
          canceled,
        )
      result <-
        remapQ(freeExpr).runF[ScriptF.Cmd, ExtendedValue](
          _.executeWithRunner(env, this, convertLegacyExceptions)
            .map(Result.successful)
            .recover { case err: RuntimeException => Result.failed(err) }
        )
    } yield result
  )

  // Takes something that resolves/computes to a Script X, then runs the script
  def run(comp: ExtendedValueComputationMode, convertLegacyExceptions: Boolean)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[ExtendedValue] =
    for {
      scriptValue <- handleLegacyExceptions(convertLegacyExceptions)(runComputation(comp))
      result <- runResolved(scriptValue, convertLegacyExceptions)
    } yield result

  def runComputation(
      comp: ExtendedValueComputationMode
  )(implicit ec: ExecutionContext): Future[ExtendedValue] =
    Future {
      runExtendedValueComputation(
        comp,
        canceled,
        unversionedRunner.extendedCompiledPackages,
        machineLogger,
        iterationsBetweenInterruptions = 100000,
        convertLegacyExceptions = false,
      ).fold(
        err => throw err.fold(identity, free.InterpretationError(_)),
        identity,
      )
    }

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
            run(ExtendedValueComputationMode.ByIdentifier(id), convertLegacyExceptions = true)
          case ScriptAction.Param(id, paramType, Some(param), _) =>
            run(
              ExtendedValueComputationMode.ByIdentifier(id, Some(List(param))),
              convertLegacyExceptions = true,
            )
          case _ =>
            Future.failed(
              new RuntimeException("impossible")
            ) // This case is caught by script.Runner, when a Param ScriptAction is called without a param
        },
        ideLedgerContext,
      )

  def makeFailureStatus(excpType: Ref.TypeConId, msg: String) =
    free.InterpretationError(
      SError.SErrorDamlException(
        IE.FailureStatus(
          "UNHANDLED_EXCEPTION/" + excpType.qualifiedName.toString,
          Ast.FCInvalidGivenCurrentSystemStateOther.cantonCategoryId,
          msg,
          Map(),
        )
      )
    )

  def handleLegacyExceptions[X](
      convertLegacyExceptions: Boolean
  )(x: Future[X])(implicit ec: ExecutionContext) =
    x.recoverWith {
      case free.InterpretationError(
            SError.SErrorDamlException(IE.UnhandledException(Ast.TTyCon(excpType), value))
          ) if convertLegacyExceptions =>
        convertLegacyException(excpType, value)
    }

  def convertLegacyException(excpType: Ref.TypeConId, value: ExtendedValue)(implicit
      ec: ExecutionContext
  ): Future[Nothing] = {
    runComputation(
      ExtendedValueComputationMode.ByExceptionMessage(excpType, value)
    ).transform { result =>
      val error = result match {
        case Success(Value.ValueText(msg)) =>
          makeFailureStatus(excpType, msg)
        case Success(_) =>
          new RuntimeException(s"Message computation for exception $excpType did not give Text")
        case Failure(
              free.InterpretationError(
                SError.SErrorDamlException(
                  IE.UnhandledException(Ast.TTyCon(messageExceptionName), _)
                )
              )
            ) =>
          makeFailureStatus(
            excpType,
            s"<Failed to calculate message as ${messageExceptionName.qualifiedName.toString} was thrown during conversion>",
          )
        case Failure(error) => error
      }
      Failure(error)
    }
  }
}
