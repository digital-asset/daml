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
import com.daml.lf.speedy.{SValue, SExpr, Profile, WarningLog, Speedy, TraceLog}
import com.daml.script.converter.ConverterException

import scala.concurrent.{ExecutionContext, Future}

// linking magic
import com.daml.lf.archive.{DarReaderAllowFixed, Decode, Dar, ArchivePayload}
import com.daml.lf.speedy.SDefinition
import java.util.zip.ZipInputStream
import com.daml.lf.engine.script.Runner.{LinkingBehaviour, TypeCheckingBehaviour}
import com.daml.lf.validation.Validation

private[lf] class Runner(
    unversionedRunner: script.Runner,
    initialClients: Participants[UnversionedScriptLedgerClient],
    traceLog: TraceLog = Speedy.Machine.newTraceLog,
    warningLog: WarningLog = Speedy.Machine.newWarningLog,
    profile: Profile = Speedy.Machine.newProfile,
    canceled: () => Option[RuntimeException] = () => None,
    linkingBehaviour: LinkingBehaviour = LinkingBehaviour.LinkRecent,
    typeCheckingBehaviour: TypeCheckingBehaviour = TypeCheckingBehaviour.NoTypeChecking,
) {
  import Free.Result, SExpr.SExpr
  if (unversionedRunner.script.scriptIds.scriptPackageId.toString != "daml3scriptlabel")
    throw new IllegalArgumentException(
      s"""Expected daml3-script library to have package id 'daml3scriptlabel', but instead got '${unversionedRunner.script.scriptIds.scriptPackageId}'.
         |Be sure to compile daml3-script library with the '--override-package-id daml3scriptlabel' option.
         """.stripMargin
    )

  private def buildLinkedPackages(scriptDar: Dar[ArchivePayload]): CompiledPackages = {
    val mostRecentScriptDar =
      scriptDar
        .copy(main =
          scriptDar.main.copy(pkgId = unversionedRunner.script.scriptIds.scriptPackageId)
        )
        .all
        .map(Decode.assertDecodeArchivePayload(_))
        .toMap

    val mostRecentScriptCompiledPackages =
      PureCompiledPackages.assertBuild(mostRecentScriptDar, script.Runner.compilerConfig)

    typeCheckingBehaviour match {
      case TypeCheckingBehaviour.TypeChecking(dar) =>
        Validation
          .checkPackages(dar.all.toMap ++ mostRecentScriptDar)
          .fold(err => throw err, identity)
      case _ =>
    }

    mostRecentScriptCompiledPackages orElse unversionedRunner.compiledPackages
  }

  // Dynamically link in the daml3-script library
  private val linkedCompiledPackages =
    linkingBehaviour match {
      case LinkingBehaviour.NoLinking => {
        typeCheckingBehaviour match {
          case TypeCheckingBehaviour.TypeChecking(dar) =>
            Validation.checkPackages(dar.all.toMap).fold(err => throw err, identity)
          case _ =>
        }
        unversionedRunner.compiledPackages
      }
      case LinkingBehaviour.LinkRecent =>
        buildLinkedPackages(
          DarReaderAllowFixed
            .readArchive(
              "daml3-script",
              new ZipInputStream(
                getClass.getResourceAsStream("/daml-script/daml3/daml3-script.dar")
              ),
            )
            .toOption
            .get
        )
      case LinkingBehaviour.LinkSpecific(scriptDar) => buildLinkedPackages(scriptDar)
    }

  val linkedExtendedCompiledPackages =
    linkedCompiledPackages.overrideDefinitions({
      // Generalised version of the various unsafe casts we need in daml scripts,
      // casting various types involving LedgerValue to/from their real types.
      case SExpr.LfDefRef(id)
          if id == unversionedRunner.script.scriptIds.damlScriptModule(
            "Daml.Script.Internal",
            "dangerousCast",
          ) =>
        SDefinition(SExpr.SEMakeClo(Array(), 1, SExpr.SELocA(0)))
    })

  private val initialClientsV1 = initialClients.map(ScriptLedgerClient.realiseScriptLedgerClient)

  private val env =
    new ScriptF.Env(
      unversionedRunner.script.scriptIds,
      unversionedRunner.timeMode,
      initialClientsV1,
      linkedExtendedCompiledPackages,
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

  def remapQ[X](result: Result[X, Free.Question, SExpr]): Result[X, ScriptF.Cmd, SExpr] =
    result.remapQ { case Free.Question(name, version, payload, stackTrace) =>
      ScriptF.parse(name, version, payload, stackTrace) match {
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
        linkedExtendedCompiledPackages,
        traceLog,
        warningLog,
        profile,
        Script.DummyLoggingContext,
      )
    ).runF[ScriptF.Cmd, SExpr](
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
