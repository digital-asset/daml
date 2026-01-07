// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion}
import com.digitalasset.daml.lf.speedy._
import com.digitalasset.daml.lf.speedy.Compiler.{CompilationError, PackageNotFound}
import com.digitalasset.daml.lf.speedy.SError.SError
import com.digitalasset.daml.lf.speedy.SExpr.{
  SEApp,
  SEBuiltinFun,
  SEValue,
  SExpr,
  SEAppAtomicGeneral,
  SDefinitionRef,
  LfDefRef,
  ExceptionMessageDefRef,
  SEMakeClo,
  SELocA,
}
import com.digitalasset.daml.lf.speedy.SBuiltinFun.SBViewInterface
import com.digitalasset.daml.lf.speedy.Speedy.Machine
import com.digitalasset.daml.lf.speedy.Speedy.PureMachine
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.speedy.SValue.{SPAP, SAny}
import com.digitalasset.daml.lf.value.GenValue
import scala.annotation.{nowarn, tailrec}
import scala.collection.immutable.ArraySeq

object ScriptEngine {
  type ExtendedValueClosureBlob = GenValue.Blob[SPAP]
  val ExtendedValueClosureBlob: GenValue.Blob.type = GenValue.Blob
  type ExtendedValueAny = GenValue.Any[SPAP]
  val ExtendedValueAny: GenValue.Any.type = GenValue.Any
  type ExtendedValueTypeRep = GenValue.TypeRep[SPAP]
  val ExtendedValueTypeRep: GenValue.TypeRep.type = GenValue.TypeRep
  type ExtendedValue = GenValue[GenValue.Extension[SPAP]]

  type TraceLog = speedy.TraceLog
  type WarningLog = speedy.WarningLog

  def newTraceLog = Machine.newTraceLog
  def newWarningLog = Machine.newWarningLog

  val defaultCompilerConfig: Compiler.Config = {
    import Compiler._
    Config(
      allowedLanguageVersions = LanguageVersion.allLfVersionsRange,
      packageValidation = FullPackageValidation,
      profiling = NoProfile,
      stacktracing = FullStackTrace,
    )
  }

  sealed trait ExtendedValueComputationMode {
    def buildSExpr(
        compiledPackages: CompiledPackages,
        translator: ExtendedValueTranslator,
    ): Either[RuntimeException, SExpr]
  }
  object ExtendedValueComputationMode {
    final case class ByClosure(f: ExtendedValueClosureBlob, args: List[ExtendedValue])
        extends ExtendedValueComputationMode {
      override def buildSExpr(
          compiledPackages: CompiledPackages,
          translator: ExtendedValueTranslator,
      ): Either[RuntimeException, SExpr] = {
        import scalaz.syntax.traverse._
        import scalaz.std.list._
        import scalaz.std.either._
        args
          .traverse(v => translator.translateExtendedValue(v).map(SEValue(_)))
          .map(sArgs => SEAppAtomicGeneral(SEValue(f.getContent), ArraySeq.from(sArgs)))
      }
    }
    object ByIdentifier {
      def apply(
          id: Identifier,
          oArgs: Option[List[ExtendedValue]] = None,
      ): ExtendedValueComputationMode =
        BySDefinitionRef(LfDefRef(id), oArgs)
    }
    object ByExceptionMessage {
      def apply(
          id: Identifier,
          exceptionValue: ExtendedValue,
      ): ExtendedValueComputationMode =
        BySDefinitionRef(ExceptionMessageDefRef(id), Some(List(exceptionValue)))
    }
    private final case class BySDefinitionRef(
        ref: SDefinitionRef,
        oArgs: Option[List[ExtendedValue]],
    ) extends ExtendedValueComputationMode {
      override def buildSExpr(
          compiledPackages: CompiledPackages,
          translator: ExtendedValueTranslator,
      ): Either[RuntimeException, SExpr] = {
        import scalaz.syntax.traverse._
        import scalaz.std.list._
        import scalaz.std.either._
        import scalaz.std.option._
        for {
          sExpr <-
            compiledPackages.getDefinition(ref) match {
              case None => Left(new RuntimeException(s"Failed to find ref $ref in package"))
              case Some(SDefinition(sExpr)) => Right(sExpr)
            }
          oSValueArgs <-
            oArgs.traverse(args => args.traverse(v => translator.translateExtendedValue(v)))
        } yield oSValueArgs.fold(sExpr)(sValueArgs => SEApp(sExpr, sValueArgs.to(ArraySeq)))
      }
    }
    final case class ByInterfaceView(
        templateId: Identifier,
        interfaceId: Identifier,
        argument: ExtendedValue,
    ) extends ExtendedValueComputationMode {
      override def buildSExpr(
          compiledPackages: CompiledPackages,
          translator: ExtendedValueTranslator,
      ): Either[RuntimeException, SExpr] =
        for {
          sValueArgument <- translator.translateExtendedValue(argument)
        } yield SEApp(
          SEBuiltinFun(SBViewInterface(interfaceId)),
          ArraySeq(SAny(Ast.TTyCon(templateId), sValueArgument)),
        )
    }
  }

  @throws[PackageNotFound]
  @throws[CompilationError]
  // Returns a value with blackboxes
  // Arguments cannot contain blackboxes
  def runExtendedValueComputation(
      computationMode: ExtendedValueComputationMode,
      cancelled: () => Option[RuntimeException],
      compiledPackages: CompiledPackages,
      iterationsBetweenInterruptions: Long = Long.MaxValue,
      traceLog: TraceLog = newTraceLog,
      warningLog: WarningLog = newWarningLog,
      convertLegacyExceptions: Boolean = true,
  )(implicit
      loggingContext: LoggingContext
  ): Either[Either[RuntimeException, SError], ExtendedValue] = {
    val translator = new ExtendedValueTranslator(compiledPackages.pkgInterface)
    @nowarn("msg=dead code following this construct")
    @tailrec
    def runMachine(machine: PureMachine): Either[Either[RuntimeException, SError], SValue] =
      machine.run() match {
        case SResultError(err) => Left(Right(err))
        case SResultFinal(v) => Right(v)
        case SResultInterruption =>
          cancelled() match {
            case Some(err) => Left(Left(err))
            case None => runMachine(machine)
          }
        case SResultQuestion(nothing) => nothing
      }
    for {
      sExpr <- computationMode.buildSExpr(compiledPackages, translator).left.map(Left(_))
      machine = Machine.fromPureSExpr(
        compiledPackages,
        sExpr,
        iterationsBetweenInterruptions,
        traceLog,
        warningLog,
        Machine.newProfile,
        convertLegacyExceptions,
      )
      sRes <- runMachine(machine)
    } yield sRes.toUnnormalizedExtendedValue
  }

  def makeUnsafeCoerce(
      compiledPackages: CompiledPackages,
      unsafeCoerceName: Identifier,
  ): CompiledPackages =
    new CompiledPackages(
      compiledPackages.compilerConfig
    ) {
      override def signatures = compiledPackages.signatures
      override def getDefinition(ref: SDefinitionRef): Option[SDefinition] =
        if (ref == LfDefRef(unsafeCoerceName))
          Some(SDefinition(SEMakeClo(ArraySeq.empty, 1, SELocA(0))))
        else compiledPackages.getDefinition(ref)
    }
}
