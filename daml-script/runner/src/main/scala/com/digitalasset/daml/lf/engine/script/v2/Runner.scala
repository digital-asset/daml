// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package script
package v2

import akka.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.lf.engine.script.Runner.IdeLedgerContext
import com.daml.lf.engine.script.ledgerinteraction.{
  ScriptLedgerClient => UnversionedScriptLedgerClient
}
import com.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient
import com.daml.lf.scenario.{ScenarioLedger, ScenarioRunner}
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.{ArrayList, SValue, Speedy, TraceLog, WarningLog}
import com.daml.script.converter.ConverterException

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

// linking magic
import com.daml.lf.archive.DarDecoder
import com.daml.lf.speedy.SDefinition
import com.daml.lf.language.PackageInterface
import com.daml.lf.data.Ref._
import java.util.zip.ZipInputStream

private[lf] class Runner(
    unversionedRunner: script.Runner,
    initialClients: Participants[UnversionedScriptLedgerClient],
    traceLog: TraceLog = Speedy.Machine.newTraceLog,
    warningLog: WarningLog = Speedy.Machine.newWarningLog,
    canceled: () => Option[RuntimeException] = () => None,
) {
  // Dynamically link in the daml3-script library
  private val linkedExtendedCompiledPackages = {
    // Most recent Dar of the frontend daml3-script library
    val mostRecentScript =
      PureCompiledPackages.assertBuild(
        DarDecoder
          .readArchive(
            "daml3-script",
            new ZipInputStream(getClass.getResourceAsStream("/daml-script/daml3/daml3-script.dar")),
          )
          .toOption
          .get // `get` is safe here because the dar is a build artefact that we can't obtain if its not valid.
          .all
          .toMap,
        script.Runner.compilerConfig,
      )
    val mostRecentScriptPackageId =
      mostRecentScript.packageIds
        .find(
          (
              pkgId =>
                mostRecentScript.pkgInterface
                  .lookupPackage(pkgId)
                  .toOption
                  .flatMap(_.metadata)
                  .map(_.name) == Some("daml3-script")
          )
        )
        .getOrElse(throw new IllegalArgumentException("Own Daml Script had no metadata"))
    val injectLinkedDamlScript: (PackageId => PackageId) = pkgId =>
      if (pkgId == unversionedRunner.script.scriptIds.scriptPackageId)
        mostRecentScriptPackageId
      else
        pkgId

    // Daml script does not define any templates, so only `LfDefRef`s need to be injected
    val injectLinkedDamlScriptSDefinition: (SDefinitionRef => SDefinitionRef) = dref =>
      dref match {
        case LfDefRef(Identifier(pkgId, name)) =>
          LfDefRef(Identifier(injectLinkedDamlScript(pkgId), name))
        case _ => dref
      }

    // Plug in our dangerous cast
    val dangerousCastDef: PartialFunction[SDefinitionRef, SDefinition] = {
      // Generalised version of the various unsafe casts we need in daml scripts,
      // casting various types involving LedgerValue to/from their real types.
      case LfDefRef(id)
          if id == (new ScriptIds(mostRecentScriptPackageId)).damlScriptModule(
            "Daml.Script.Internal",
            "dangerousCast",
          ) =>
        SDefinition(SEMakeClo(Array(), 1, SELocA(0)))
    }

    new CompiledPackages(script.Runner.compilerConfig) {
      import PartialFunction._
      override def getDefinition(dref: SDefinitionRef): Option[SDefinition] = {
        val injectedDRef = injectLinkedDamlScriptSDefinition(dref)
        dangerousCastDef.lift(injectedDRef) orElse
          (mostRecentScript.getDefinition(injectedDRef) orElse
            unversionedRunner.extendedCompiledPackages.getDefinition(dref))
      }

      override def pkgInterface: PackageInterface =
        new PackageInterface(
          (fromFunction(
            injectLinkedDamlScript
          ) andThen mostRecentScript.pkgInterface.signatures) orElse unversionedRunner.extendedCompiledPackages.pkgInterface.signatures
        )

      override def packageIds: collection.Set[PackageId] =
        unversionedRunner.extendedCompiledPackages.packageIds ++ mostRecentScript.packageIds

      override def definitions: PartialFunction[SDefinitionRef, SDefinition] =
        fromFunction(
          injectLinkedDamlScriptSDefinition
        ) andThen (dangerousCastDef orElse (mostRecentScript.definitions orElse unversionedRunner.extendedCompiledPackages.definitions))
    }
  }
  private val machine =
    Speedy.Machine.fromPureSExpr(
      linkedExtendedCompiledPackages,
      unversionedRunner.script.expr,
      iterationsBetweenInterruptions = 100000,
      traceLog = traceLog,
      warningLog = warningLog,
    )(Script.DummyLoggingContext)

  @scala.annotation.tailrec
  final def stepToValue(): Either[RuntimeException, SValue] = {
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
            Left(script.Runner.InterpretationError(err))
          case res =>
            Left(new IllegalStateException(s"Internal error: Unexpected speedy result $res"))
        }
    }
  }

  private val initialClientsV1 = initialClients.map(ScriptLedgerClient.realiseScriptLedgerClient)

  private val env =
    new ScriptF.Env(
      unversionedRunner.script.scriptIds,
      unversionedRunner.timeMode,
      initialClientsV1,
      machine,
    )

  private val ctx =
    ScriptF.Ctx(
      unversionedRunner.knownPackages,
      linkedExtendedCompiledPackages,
    )

  // Wraps the result of executing a command in a call to continue, using the following form
  // let compute = \() -> resultExpr in let result = compute () in continue result
  // Note we defer to a lambda in order to maintain ANF
  private def runCmdQuestion(
      q: Converter.Question[ScriptF.Cmd]
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[SExpr] =
    for {
      resultExpr <- q.payload.executeWithRunner(env, this)
    } yield SELet1(
      SEMakeClo(Array(), 1, resultExpr),
      SELet1(
        SEAppAtomic(SELocS(1), Array(SEValue(SUnit))),
        SEAppAtomic(SEValue(q.continue), Array(SELocS(1))),
      ),
    )

  private[lf] def runExpr(
      expr: SExpr
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[SValue] = {
    machine.setExpressionToEvaluate(expr)
    stepToValue()
      .fold(Future.failed, Future.successful)
      .flatMap(fsu => Converter.toFuture(Converter.unrollFree(ctx, fsu)))
      .flatMap {
        case Right(question) =>
          Converter
            .toFuture(
              ScriptF.parse(
                question.name,
                question.version,
                question.payload,
                question.stackTrace,
              )
            )
            .map(cmd => question.copy(payload = cmd))
            .flatMap { cmdQuestion =>
              runCmdQuestion(cmdQuestion).transform {
                case f @ Failure(script.Runner.CanceledByRequest) => f
                case f @ Failure(script.Runner.TimedOut) => f
                case f @ Failure(script.Runner.InterpretationError(_)) => f
                case Failure(exception) =>
                  Failure(new Script.FailedCmd(cmdQuestion, exception))
                case Success(value) =>
                  Success(value)
              }
            }
            .flatMap(runExpr(_))
        case Left(v) =>
          v match {
            case SRecord(_, _, ArrayList(result, _)) => {
              // Unwrap the Tuple2 we get from the inlined StateT.
              Future { result }
            }
            case _ => Future.failed(new ConverterException(s"Expected Tuple2 but got $v"))
          }
      }
  }

  private val ideLedgerContext: Option[IdeLedgerContext] =
    initialClientsV1.default_participant.collect {
      case ledgerClient: ledgerinteraction.IdeLedgerClient =>
        new IdeLedgerContext {
          override def currentSubmission: Option[ScenarioRunner.CurrentSubmission] =
            ledgerClient.currentSubmission
          override def ledger: ScenarioLedger = ledgerClient.ledger
        }
    }

  def getResult()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): (Speedy.PureMachine, Future[SValue], Option[IdeLedgerContext]) = {
    val resultF = for {
      _ <- Future.unit // We want the evaluation of following stepValue() to happen in a future.
      result <- stepToValue().fold(Future.failed, Future.successful)
      expr <- result match {
        // Unwrap Script type and apply to ()
        // Second value in record is dummy unit, ignored
        case SRecord(_, _, ArrayList(expr @ SPAP(_, _, _), _)) =>
          Future(SEAppAtomic(SEValue(expr), Array(SEValue(SUnit))))
        case v => Future.failed(new ConverterException(s"Expected record with 1 field but got $v"))
      }
      v <- runExpr(expr)
    } yield v
    (machine, resultF, ideLedgerContext)
  }
}
