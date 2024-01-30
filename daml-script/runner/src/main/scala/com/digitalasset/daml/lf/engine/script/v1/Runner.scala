// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package script
package v1

import org.apache.pekko.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.lf.engine.script.Runner.IdeLedgerContext
import com.daml.lf.engine.script.ledgerinteraction.{
  ScriptLedgerClient => UnversionedScriptLedgerClient
}
import com.daml.lf.engine.script.v1.ledgerinteraction.ScriptLedgerClient
import com.daml.lf.interpretation.{Error => IE}
import com.daml.lf.scenario.{ScenarioLedger, ScenarioRunner}
import com.daml.lf.speedy.SBuiltin.SBToAny
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.{ArrayList, Profile, SError, SValue, Speedy, TraceLog, WarningLog}
import com.daml.script.converter.Converter.unrollFree
import com.daml.script.converter.ConverterException
import com.digitalasset.canton.logging.NamedLoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

private[lf] class Runner(
    unversionedRunner: script.Runner
) {
  def runWithClients(
      initialClients: Participants[UnversionedScriptLedgerClient],
      traceLog: TraceLog = Speedy.Machine.newTraceLog,
      warningLog: WarningLog = Speedy.Machine.newWarningLog,
      profile: Profile = Speedy.Machine.newProfile,
      canceled: () => Option[RuntimeException] = () => None,
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): (Future[SValue], Option[IdeLedgerContext]) = {
    val machine =
      Speedy.Machine.fromPureSExpr(
        unversionedRunner.extendedCompiledPackages,
        unversionedRunner.script.expr,
        iterationsBetweenInterruptions = 100000,
        traceLog = traceLog,
        warningLog = warningLog,
        profile = profile,
      )(Script.DummyLoggingContext)

    @scala.annotation.tailrec
    def stepToValue(): Either[RuntimeException, SValue] = {
      val result = machine.run()
      canceled() match {
        case Some(err) => Left(err)
        case None =>
          result match {
            case SResultInterruption =>
              stepToValue()
            case SResultFinal(v) =>
              Right(v)
            case SResultError(err) =>
              Left(Runner.InterpretationError(err))
            case res =>
              Left(new IllegalStateException(s"Internal error: Unexpected speedy result $res"))
          }
      }
    }

    implicit val namedLoggerFactory: NamedLoggerFactory =
      NamedLoggerFactory("daml-script", profile.name)

    val initialClientsV1 = initialClients.map(ScriptLedgerClient.realiseScriptLedgerClient)

    val env =
      new ScriptF.Env(
        unversionedRunner.script.scriptIds,
        unversionedRunner.timeMode,
        initialClientsV1,
        machine,
      )

    def run(
        expr: SExpr
    ): Future[SValue] = {
      machine.setExpressionToEvaluate(expr)
      stepToValue()
        .fold(Future.failed, Future.successful)
        .flatMap(fsu => Converter toFuture unrollFree(fsu))
        .flatMap {
          case Right((vv, v)) =>
            Converter
              .toFuture(
                ScriptF.parse(
                  ScriptF.Ctx(
                    unversionedRunner.knownPackages,
                    unversionedRunner.extendedCompiledPackages,
                  ),
                  vv,
                  v,
                )
              )
              .flatMap { scriptF =>
                scriptF match {
                  case ScriptF.Catch(act, handle, continue) =>
                    run(SEAppAtomic(SEValue(act), Array(SEValue(SUnit)))).transformWith {
                      case Success(v) => Future.successful(SEApp(SEValue(continue), Array(v)))
                      case Failure(
                            exce @ Runner.InterpretationError(
                              SError.SErrorDamlException(IE.UnhandledException(typ, value))
                            )
                          ) =>
                        val e =
                          SELet1(
                            SEImportValue(typ, value),
                            SELet1(
                              SEAppAtomic(SEBuiltin(SBToAny(typ)), Array(SELocS(1))),
                              SEAppAtomic(SEValue(handle), Array(SELocS(1))),
                            ),
                          )
                        machine.setExpressionToEvaluate(e)
                        stepToValue()
                          .fold(Future.failed, Future.successful)
                          .flatMap {
                            case SOptional(None) =>
                              Future.failed(exce)
                            case SOptional(Some(free)) =>
                              Future.successful(SEApp(SEValue(continue), Array(free)))
                            case e =>
                              Future.failed(
                                new ConverterException(s"Expected SOptional but got $e")
                              )
                          }
                      case Failure(e) => Future.failed(e)
                    }
                  case ScriptF.Throw(SAny(ty, value)) =>
                    Future.failed(
                      Runner.InterpretationError(
                        SError
                          .SErrorDamlException(IE.UnhandledException(ty, value.toUnnormalizedValue))
                      )
                    )
                  case cmd: ScriptF.Cmd =>
                    cmd.execute(env).transform {
                      case f @ Failure(Runner.CanceledByRequest) => f
                      case f @ Failure(Runner.TimedOut) => f
                      case Failure(exception) =>
                        Failure(Script.FailedCmd(cmd.description, cmd.stackTrace, exception))
                      case Success(value) =>
                        Success(value)
                    }
                }
              }
              .flatMap(run(_))
          case Left(v) =>
            v match {
              case SRecord(_, _, ArrayList(newState, _)) => {
                // Unwrap the Tuple2 we get from the inlined StateT.
                Future { newState }
              }
              case _ => Future.failed(new ConverterException(s"Expected Tuple2 but got $v"))
            }
        }
    }

    val ideLedgerContext: Option[IdeLedgerContext] =
      initialClientsV1.default_participant match {
        case Some(ledgerClient: ledgerinteraction.IdeLedgerClient) =>
          Some(new IdeLedgerContext {
            override def currentSubmission: Option[ScenarioRunner.CurrentSubmission] =
              ledgerClient.currentSubmission
            override def ledger: ScenarioLedger = ledgerClient.ledger
          })
        case _ => None
      }

    val resultF = for {
      _ <- Future.unit // We want the evaluation of following stepValue() to happen in a future.
      result <- stepToValue().fold(Future.failed, Future.successful)
      expr <- result match {
        // Unwrap Script type and apply to ()
        // For backwards-compatibility we support the 1 and the 2-field versions.
        case SRecord(_, _, vals) if vals.size == 1 || vals.size == 2 => {
          vals.get(0) match {
            case SPAP(_, _, _) =>
              Future(SEAppAtomic(SEValue(vals.get(0)), Array(SEValue(SUnit))))
            case _ =>
              Future.failed(
                new ConverterException(
                  "Mismatch in structure of Script type. " +
                    "This probably means that you tried to run a script built against an " +
                    "SDK <= 0.13.55-snapshot.20200304.3329.6a1c75cf with a script runner from a newer SDK."
                )
              )
          }
        }
        case v => Future.failed(new ConverterException(s"Expected record with 1 field but got $v"))
      }
      v <- run(expr)
    } yield v
    (resultF, ideLedgerContext)
  }
}
