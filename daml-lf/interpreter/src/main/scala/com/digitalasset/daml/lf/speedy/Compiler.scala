// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import java.util
import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, Numeric, Ref, Struct, Time}
import com.daml.lf.language.Ast._
import com.daml.lf.language.{LanguageVersion, LookupError, PackageInterface, StablePackages}
import com.daml.lf.speedy.Anf.flattenToAnf
import com.daml.lf.speedy.Profile.LabelModule
import com.daml.lf.speedy.SBuiltin._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.validation.{EUnknownDefinition, Validation, ValidationError}
import com.daml.scalautil.Statement.discard
import com.daml.nameof.NameOf
import org.slf4j.LoggerFactory

import scala.annotation.{nowarn, tailrec}
import scala.reflect.ClassTag

/** Compiles LF expressions into Speedy expressions.
  * This includes:
  *  - Writing variable references into de Bruijn indices.
  *  - Closure conversion: EAbs turns into SEMakeClo, which creates a closure by copying free variables into a closure object.
  *   - Rewriting of update and scenario actions into applications of builtin functions that take an "effect" token.
  *
  * If you're working on the code here note that there's
  * a pretty-printer defined in lf.speedy.Pretty, which
  * is exposed via ':speedy' command in the REPL.
  */
private[lf] object Compiler {

  case class CompilationError(error: String) extends RuntimeException(error, null, true, false)
  case class LanguageVersionError(
      packageId: Ref.PackageId,
      languageVersion: language.LanguageVersion,
      allowedLanguageVersions: VersionRange[language.LanguageVersion],
  ) extends RuntimeException(s"Disallowed language version $languageVersion", null, true, false)
  case class PackageNotFound(pkgId: PackageId, context: language.Reference)
      extends RuntimeException(
        language.LookupError.MissingPackage.pretty(pkgId, context),
        null,
        true,
        false,
      )

  // NOTE(MH): We make this an enum type to avoid boolean blindness. In fact,
  // other profiling modes like "only trace the ledger interactions" might also
  // be useful.
  sealed abstract class ProfilingMode extends Product with Serializable
  case object NoProfile extends ProfilingMode
  case object FullProfile extends ProfilingMode

  sealed abstract class StackTraceMode extends Product with Serializable
  case object NoStackTrace extends StackTraceMode
  case object FullStackTrace extends StackTraceMode

  sealed abstract class PackageValidationMode extends Product with Serializable
  case object NoPackageValidation extends PackageValidationMode
  case object FullPackageValidation extends PackageValidationMode

  case class Config(
      allowedLanguageVersions: VersionRange[LanguageVersion],
      packageValidation: PackageValidationMode,
      profiling: ProfilingMode,
      stacktracing: StackTraceMode,
  )

  object Config {
    val Default = Config(
      allowedLanguageVersions = LanguageVersion.StableVersions,
      packageValidation = FullPackageValidation,
      profiling = NoProfile,
      stacktracing = NoStackTrace,
    )
    val Dev = Config(
      allowedLanguageVersions = LanguageVersion.DevVersions,
      packageValidation = FullPackageValidation,
      profiling = NoProfile,
      stacktracing = NoStackTrace,
    )
  }

  private val SEGetTime = SEBuiltin(SBGetTime)

  private def SBCompareNumeric(b: SBuiltinPure) =
    SEAbs(3, SEApp(SEBuiltin(b), Array(SEVar(2), SEVar(1))))
  private val SBLessNumeric = SBCompareNumeric(SBLess)
  private val SBLessEqNumeric = SBCompareNumeric(SBLessEq)
  private val SBGreaterNumeric = SBCompareNumeric(SBGreater)
  private val SBGreaterEqNumeric = SBCompareNumeric(SBGreaterEq)
  private val SBEqualNumeric = SBCompareNumeric(SBEqual)

  private val SBEToTextNumeric = SEAbs(1, SEBuiltin(SBToText))

  private val SENat: Numeric.Scale => Some[SEValue] =
    Numeric.Scale.values.map(n => Some(SEValue(STNat(n))))

  /** Validates and Compiles all the definitions in the packages provided. Returns them in a Map.
    *
    * The packages do not need to be in any specific order, as long as they and all the packages
    * they transitively reference are in the [[packages]] in the compiler.
    */
  def compilePackages(
      interface: PackageInterface,
      packages: Map[PackageId, Package],
      compilerConfig: Compiler.Config,
  ): Either[String, Map[SDefinitionRef, SDefinition]] = {
    val compiler = new Compiler(interface, compilerConfig)
    try {
      Right(packages.foldLeft(Map.empty[SDefinitionRef, SDefinition]) { case (acc, (pkgId, pkg)) =>
        acc ++ compiler.unsafeCompilePackage(pkgId, pkg)
      })
    } catch {
      case CompilationError(msg) => Left(s"Compilation Error: $msg")
      case PackageNotFound(pkgId, context) =>
        Left(LookupError.MissingPackage.pretty(pkgId, context))
      case e: ValidationError => Left(e.pretty)
    }
  }

  // Hand-implemented `map` uses less stack.
  private def mapToArray[A, B: ClassTag](input: ImmArray[A])(f: A => B): Array[B] = {
    val output = Array.ofDim[B](input.length)
    var i = 0
    input.foreach { value =>
      output(i) = f(value)
      i += 1
    }
    output
  }

}

private[lf] final class Compiler(
    interface: PackageInterface,
    config: Compiler.Config,
) {

  import Compiler._

  private[this] def handleLookup[X](location: String, x: Either[LookupError, X]) =
    x match {
      case Right(value) => value
      case Left(err) => throw SError.SErrorCrash(location, err.pretty)
    }

  // Stack-trace support is disabled by avoiding the construction of SELocation nodes.
  private[this] def maybeSELocation(loc: Location, sexp: SExpr): SExpr = {
    config.stacktracing match {
      case NoStackTrace => sexp
      case FullStackTrace => SELocation(loc, sexp)
    }
  }

  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  private[this] abstract class VarRef { def name: Name }
  // corresponds to Daml-LF expression variable.
  private[this] case class EVarRef(name: ExprVarName) extends VarRef
  // corresponds to Daml-LF type variable.
  private[this] case class TVarRef(name: TypeVarName) extends VarRef

  case class Position(idx: Int)

  private[this] object Env {
    val Empty = Env(0, Map.empty)
  }

  private[this] case class Env(
      position: Int,
      varIndices: Map[VarRef, Position],
  ) {

    def toSEVar(p: Position): SEVar = SEVar(position - p.idx)

    def nextPosition = Position(position)

    def pushVar: Env = copy(position = position + 1)

    private[this] def bindVar(ref: VarRef, p: Position) =
      copy(varIndices = varIndices.updated(ref, p))

    def pushVar(ref: VarRef): Env =
      bindVar(ref, nextPosition).pushVar

    def pushExprVar(name: ExprVarName): Env =
      pushVar(EVarRef(name))

    def pushExprVar(maybeName: Option[ExprVarName]): Env =
      maybeName match {
        case Some(name) => pushExprVar(name)
        case None => pushVar
      }

    def pushTypeVar(name: ExprVarName): Env =
      pushVar(TVarRef(name))

    def hideTypeVar(name: TypeVarName): Env =
      copy(varIndices = varIndices - TVarRef(name))

    def bindExprVar(name: ExprVarName, p: Position): Env =
      bindVar(EVarRef(name), p)

    private[this] def vars: List[VarRef] = varIndices.keys.toList

    private[this] def lookupVar(varRef: VarRef): Option[SEVar] =
      varIndices.get(varRef).map(toSEVar)

    def lookupExprVar(name: ExprVarName): SEVar =
      lookupVar(EVarRef(name))
        .getOrElse(throw CompilationError(s"Unknown variable: $name. Known: ${vars.mkString(",")}"))

    def lookupTypeVar(name: TypeVarName): Option[SEVar] =
      lookupVar(TVarRef(name))

  }

  private[this] val withLabel: (Profile.Label, SExpr) => SExpr =
    config.profiling match {
      case NoProfile => { (_, expr) =>
        expr
      }
      case FullProfile => { (label, expr) =>
        expr match {
          case SELabelClosure(_, expr1) => SELabelClosure(label, expr1)
          case _ => SELabelClosure(label, expr)
        }
      }
    }

  private[this] def withOptLabel[L: Profile.LabelModule.Allowed](
      optLabel: Option[L with AnyRef],
      expr: SExpr,
  ): SExpr =
    optLabel match {
      case Some(label) => withLabel(label, expr)
      case None => expr
    }

  private[this] def app(f: SExpr, a: SExpr) = SEApp(f, Array(a))

  private[this] def let(env: Env, bound: SExpr)(f: (Position, Env) => SExpr): SELet =
    f(env.nextPosition, env.pushVar) match {
      case SELet(bounds, body) =>
        SELet(bound :: bounds, body)
      case otherwise =>
        SELet(List(bound), otherwise)
    }

  private[this] def unaryFunction(env: Env)(f: (Position, Env) => SExpr): SEAbs =
    f(env.nextPosition, env.pushVar) match {
      case SEAbs(n, body) => SEAbs(n + 1, body)
      case otherwise => SEAbs(1, otherwise)
    }

  private[this] def labeledUnaryFunction[L: Profile.LabelModule.Allowed](
      label: L with AnyRef,
      env: Env,
  )(
      body: (Position, Env) => SExpr
  ): SExpr =
    unaryFunction(env)((positions, env) => withLabel(label, body(positions, env)))

  private[this] def topLevelFunction[SDefRef <: SDefinitionRef: LabelModule.Allowed](
      ref: SDefRef
  )(
      body: SExpr
  ): (SDefRef, SDefinition) =
    ref -> SDefinition(unsafeClosureConvert(withLabel(ref, body)))

  private val Position1 = Env.Empty.nextPosition
  private val Env1 = Env.Empty.pushVar
  private val Position2 = Env1.nextPosition
  private val Env2 = Env1.pushVar
  private val Position3 = Env2.nextPosition
  private val Env3 = Env2.pushVar

  private[this] def topLevelFunction1[SDefRef <: SDefinitionRef: LabelModule.Allowed](ref: SDefRef)(
      body: (Position, Env) => SExpr
  ): (SDefRef, SDefinition) =
    topLevelFunction(ref)(SEAbs(1, body(Position1, Env1)))

  private[this] def topLevelFunction2[SDefRef <: SDefinitionRef: LabelModule.Allowed](ref: SDefRef)(
      body: (Position, Position, Env) => SExpr
  ): (SDefRef, SDefinition) =
    topLevelFunction(ref)(SEAbs(2, body(Position1, Position2, Env2)))

  private[this] def topLevelFunction3[SDefRef <: SDefinitionRef: LabelModule.Allowed](ref: SDefRef)(
      body: (Position, Position, Position, Env) => SExpr
  ): (SDefRef, SDefinition) =
    topLevelFunction(ref)(SEAbs(3, body(Position1, Position2, Position3, Env3)))

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompile(cmds: ImmArray[Command]): SExpr =
    validate(compilationPipeline(compileCommands(cmds)))

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompileForReinterpretation(cmd: Command): SExpr =
    validate(compilationPipeline(compileCommandForReinterpretation(cmd)))

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompile(expr: Expr): SExpr =
    validate(compilationPipeline(compile(Env.Empty, expr)))

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeClosureConvert(sexpr: SExpr): SExpr =
    validate(compilationPipeline(sexpr))

  // Run the compilation pipeline phases:
  // (1) closure conversion
  // (2) transform to ANF
  private[this] def compilationPipeline(sexpr: SExpr): SExpr =
    flattenToAnf(closureConvert(Map.empty, sexpr))

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompileModule(
      pkgId: PackageId,
      module: Module,
  ): Iterable[(SDefinitionRef, SDefinition)] = {
    val builder = Iterable.newBuilder[(SDefinitionRef, SDefinition)]
    def addDef(binding: (SDefinitionRef, SDefinition)) = discard(builder += binding)

    module.exceptions.foreach { case (defName, GenDefException(message)) =>
      val ref = ExceptionMessageDefRef(Identifier(pkgId, QualifiedName(module.name, defName)))
      builder += (ref -> SDefinition(withLabel(ref, unsafeCompile(message))))
    }

    module.definitions.foreach {
      case (defName, DValue(_, _, body, _)) =>
        val ref = LfDefRef(Identifier(pkgId, QualifiedName(module.name, defName)))
        builder += (ref -> SDefinition(withLabel(ref, unsafeCompile(body))))
      case _ =>
    }

    module.templates.foreach { case (tmplName, tmpl) =>
      val identifier = Identifier(pkgId, QualifiedName(module.name, tmplName))
      addDef(compileCreate(identifier, tmpl))
      addDef(compileFetch(identifier, tmpl))
      addDef(compileKey(identifier, tmpl))
      addDef(compileSignatories(identifier, tmpl))
      addDef(compileObservers(identifier, tmpl))
      tmpl.implements.values.foreach { impl =>
        addDef(compileImplements(identifier, impl.interface))
        impl.methods.values.foreach(method =>
          addDef(compileImplementsMethod(identifier, impl.interface, method))
        )
      }

      tmpl.choices.values.foreach(x => addDef(compileChoice(identifier, tmpl, x)))

      tmpl.key.foreach { tmplKey =>
        addDef(compileFetchByKey(identifier, tmpl, tmplKey))
        addDef(compileLookupByKey(identifier, tmplKey))
        tmpl.choices.values.foreach(x => addDef(compileChoiceByKey(identifier, tmpl, tmplKey, x)))
      }
    }

    module.interfaces.foreach { case (ifaceName, iface) =>
      val identifier = Identifier(pkgId, QualifiedName(module.name, ifaceName))
      addDef(compileFetchInterface(identifier))
      iface.fixedChoices.values.foreach(
        builder += compileFixedChoice(identifier, iface.param, _)
      )
      iface.virtualChoices.values.foreach(
        builder += compileVirtualChoice(identifier, _)
      )
    }

    builder.result()
  }

  /** Validates and compiles all the definitions in the package provided.
    *
    * Fails with [[PackageNotFound]] if the package or any of the packages it refers
    * to are not in the [[interface]].
    *
    * @throws ValidationError if the package does not pass validations.
    */
  @throws[PackageNotFound]
  @throws[CompilationError]
  @throws[ValidationError]
  def unsafeCompilePackage(
      pkgId: PackageId,
      pkg: Package,
  ): Iterable[(SDefinitionRef, SDefinition)] = {
    logger.trace(s"compilePackage: Compiling $pkgId...")

    val t0 = Time.Timestamp.now()

    interface.lookupPackage(pkgId) match {
      case Right(pkg)
          if !StablePackages.Ids.contains(pkgId) && !config.allowedLanguageVersions
            .contains(pkg.languageVersion) =>
        throw LanguageVersionError(pkgId, pkg.languageVersion, config.allowedLanguageVersions)
      case _ =>
    }

    config.packageValidation match {
      case Compiler.NoPackageValidation =>
      case Compiler.FullPackageValidation =>
        Validation.checkPackage(interface, pkgId, pkg).left.foreach {
          case EUnknownDefinition(_, LookupError.MissingPackage(pkgId_, context)) =>
            logger.trace(s"compilePackage: Missing $pkgId_, requesting it...")
            throw PackageNotFound(pkgId_, context)
          case e =>
            throw e
        }
    }

    val t1 = Time.Timestamp.now()

    val result = pkg.modules.values.flatMap(unsafeCompileModule(pkgId, _))

    val t2 = Time.Timestamp.now()
    logger.trace(
      s"compilePackage: $pkgId ready, typecheck=${(t1.micros - t0.micros) / 1000}ms, compile=${(t2.micros - t1.micros) / 1000}ms"
    )

    result
  }

  private[this] def patternNArgs(pat: SCasePat): Int = pat match {
    case _: SCPEnum | _: SCPPrimCon | SCPNil | SCPDefault | SCPNone => 0
    case _: SCPVariant | SCPSome => 1
    case SCPCons => 2
  }

  private[this] def compile(env: Env, expr0: Expr): SExpr =
    expr0 match {
      case EVar(name) =>
        env.lookupExprVar(name)
      case EVal(ref) =>
        SEVal(LfDefRef(ref))
      case EBuiltin(bf) =>
        compileBuiltin(bf)
      case EPrimCon(con) =>
        compilePrimCon(con)
      case EPrimLit(lit) =>
        compilePrimLit(lit)
      case EAbs(_, _, _) | ETyAbs(_, _) =>
        compileAbss(env, expr0)
      case EApp(_, _) | ETyApp(_, _) =>
        compileApps(env, expr0)
      case ERecCon(tApp, fields) =>
        compileERecCon(env, tApp, fields)
      case ERecProj(tapp, field, record) =>
        SBRecProj(
          tapp.tycon,
          handleLookup(
            NameOf.qualifiedNameOfCurrentFunc,
            interface.lookupRecordFieldInfo(tapp.tycon, field),
          ).index,
        )(compile(env, record))
      case erecupd: ERecUpd =>
        compileERecUpd(env, erecupd)
      case EStructCon(fields) =>
        val fieldsInputOrder =
          Struct.assertFromSeq(fields.iterator.map(_._1).zipWithIndex.toSeq)
        SEApp(
          SEBuiltin(SBStructCon(fieldsInputOrder)),
          mapToArray(fields) { case (_, e) => compile(env, e) },
        )
      case structProj: EStructProj =>
        structProj.fieldIndex match {
          case None => SBStructProjByName(structProj.field)(compile(env, structProj.struct))
          case Some(index) => SBStructProj(index)(compile(env, structProj.struct))
        }
      case structUpd: EStructUpd =>
        structUpd.fieldIndex match {
          case None =>
            SBStructUpdByName(structUpd.field)(
              compile(env, structUpd.struct),
              compile(env, structUpd.update),
            )
          case Some(index) =>
            SBStructUpd(index)(compile(env, structUpd.struct), compile(env, structUpd.update))
        }
      case ECase(scrut, alts) =>
        compileECase(env, scrut, alts)
      case ENil(_) =>
        SEValue.EmptyList
      case ECons(_, front, tail) =>
        // TODO(JM): Consider emitting SEValue(SList(...)) for
        // constant lists?
        val args = (front.iterator.map(compile(env, _)) ++ Seq(compile(env, tail))).toArray
        if (front.length == 1) {
          SEApp(SEBuiltin(SBCons), args)
        } else {
          SEApp(SEBuiltin(SBConsMany(front.length)), args)
        }
      case ENone(_) =>
        SEValue.None
      case ESome(_, body) =>
        SBSome(compile(env, body))
      case EEnumCon(tyCon, consName) =>
        val rank = handleLookup(
          NameOf.qualifiedNameOfCurrentFunc,
          interface.lookupEnumConstructor(tyCon, consName),
        )
        SEValue(SEnum(tyCon, consName, rank))
      case EVariantCon(tapp, variant, arg) =>
        val rank = handleLookup(
          NameOf.qualifiedNameOfCurrentFunc,
          interface.lookupVariantConstructor(tapp.tycon, variant),
        ).rank
        SBVariantCon(tapp.tycon, variant, rank)(compile(env, arg))
      case let: ELet =>
        compileELet(env, let)
      case EUpdate(upd) =>
        compileEUpdate(env, upd)
      case ELocation(loc, EScenario(scen)) =>
        maybeSELocation(loc, compileScenario(env, scen, Some(loc)))
      case EScenario(scen) =>
        compileScenario(env, scen, None)
      case ELocation(loc, e) =>
        maybeSELocation(loc, compile(env, e))
      case EToAny(ty, e) =>
        SBToAny(ty)(compile(env, e))
      case EFromAny(ty, e) =>
        SBFromAny(ty)(compile(env, e))
      case ETypeRep(typ) =>
        SEValue(STypeRep(typ))
      case EToAnyException(ty, e) =>
        SBToAny(ty)(compile(env, e))
      case EFromAnyException(ty, e) =>
        SBFromAny(ty)(compile(env, e))
      case EThrow(_, ty, e) =>
        SBThrow(SBToAny(ty)(compile(env, e)))
      case EToInterface(iface @ _, tpl @ _, e) =>
        compile(env, e) // interfaces have the same representation as underlying template
      case EFromInterface(iface @ _, tpl, e) =>
        SBFromInterface(tpl)(compile(env, e))
      case ECallInterface(iface, methodName, e) =>
        SBCallInterface(iface, methodName)(compile(env, e))
      case EExperimental(name, _) =>
        SBExperimental(name)

    }

  @inline
  private[this] def compileBuiltin(bf: BuiltinFunction): SExpr =
    bf match {
      case BEqualList =>
        val ref = SEBuiltinRecursiveDefinition.EqualList
        withLabel(ref.ref, ref)
      case BCoerceContractId => SEAbs.identity
      // Numeric Comparisons
      case BLessNumeric => SBLessNumeric
      case BLessEqNumeric => SBLessEqNumeric
      case BGreaterNumeric => SBGreaterNumeric
      case BGreaterEqNumeric => SBGreaterEqNumeric
      case BEqualNumeric => SBEqualNumeric
      case BNumericToText => SBEToTextNumeric

      case BTextMapEmpty => SEValue.EmptyTextMap
      case BGenMapEmpty => SEValue.EmptyGenMap
      case _ =>
        SEBuiltin(bf match {
          case BTrace => SBTrace

          // Decimal arithmetic
          case BAddNumeric => SBAddNumeric
          case BSubNumeric => SBSubNumeric
          case BMulNumeric => SBMulNumeric
          case BDivNumeric => SBDivNumeric
          case BRoundNumeric => SBRoundNumeric
          case BCastNumeric => SBCastNumeric
          case BShiftNumeric => SBShiftNumeric

          // Int64 arithmetic
          case BAddInt64 => SBAddInt64
          case BSubInt64 => SBSubInt64
          case BMulInt64 => SBMulInt64
          case BModInt64 => SBModInt64
          case BDivInt64 => SBDivInt64
          case BExpInt64 => SBExpInt64

          // Conversions
          case BInt64ToNumeric => SBInt64ToNumeric
          case BNumericToInt64 => SBNumericToInt64
          case BDateToUnixDays => SBDateToUnixDays
          case BUnixDaysToDate => SBUnixDaysToDate
          case BTimestampToUnixMicroseconds => SBTimestampToUnixMicroseconds
          case BUnixMicrosecondsToTimestamp => SBUnixMicrosecondsToTimestamp

          // Text functions
          case BExplodeText => SBExplodeText
          case BImplodeText => SBImplodeText
          case BAppendText => SBAppendText

          case BInt64ToText => SBToText
          case BTextToText => SBToText
          case BTimestampToText => SBToText
          case BPartyToText => SBToText
          case BDateToText => SBToText
          case BContractIdToText => SBContractIdToText
          case BPartyToQuotedText => SBPartyToQuotedText
          case BCodePointsToText => SBCodePointsToText
          case BTextToParty => SBTextToParty
          case BTextToInt64 => SBTextToInt64
          case BTextToNumeric => SBTextToNumeric
          case BTextToCodePoints => SBTextToCodePoints

          case BSHA256Text => SBSHA256Text

          // List functions
          case BFoldl => SBFoldl
          case BFoldr => SBFoldr

          // Errors
          case BError => SBError

          // Comparison
          case BEqualContractId => SBEqual
          case BEqual => SBEqual
          case BLess => SBLess
          case BLessEq => SBLessEq
          case BGreater => SBGreater
          case BGreaterEq => SBGreaterEq

          // TextMap

          case BTextMapInsert => SBMapInsert
          case BTextMapLookup => SBMapLookup
          case BTextMapDelete => SBMapDelete
          case BTextMapToList => SBMapToList
          case BTextMapSize => SBMapSize

          // GenMap

          case BGenMapInsert => SBMapInsert
          case BGenMapLookup => SBMapLookup
          case BGenMapDelete => SBMapDelete
          case BGenMapKeys => SBMapKeys
          case BGenMapValues => SBMapValues
          case BGenMapSize => SBMapSize

          case BScaleBigNumeric => SBScaleBigNumeric
          case BPrecisionBigNumeric => SBPrecisionBigNumeric
          case BAddBigNumeric => SBAddBigNumeric
          case BSubBigNumeric => SBSubBigNumeric
          case BDivBigNumeric => SBDivBigNumeric
          case BMulBigNumeric => SBMulBigNumeric
          case BShiftRightBigNumeric => SBShiftRightBigNumeric
          case BNumericToBigNumeric => SBNumericToBigNumeric
          case BBigNumericToNumeric => SBBigNumericToNumeric
          case BBigNumericToText => SBToText

          // Unstable Text Primitives
          case BTextToUpper => SBTextToUpper
          case BTextToLower => SBTextToLower
          case BTextSlice => SBTextSlice
          case BTextSliceIndex => SBTextSliceIndex
          case BTextContainsOnly => SBTextContainsOnly
          case BTextReplicate => SBTextReplicate
          case BTextSplitOn => SBTextSplitOn
          case BTextIntercalate => SBTextIntercalate

          // Implemented using normal SExpr
          case BFoldl | BFoldr | BCoerceContractId | BEqual | BEqualList | BLessEq |
              BLess | BGreaterEq | BGreater | BLessNumeric | BLessEqNumeric | BGreaterNumeric |
              BGreaterEqNumeric | BEqualNumeric | BNumericToText | BTextMapEmpty | BGenMapEmpty =>
            throw CompilationError(s"unexpected $bf")

          case BAnyExceptionMessage => SBAnyExceptionMessage
        })
    }

  @inline
  private[this] def compilePrimCon(con: PrimCon): SExpr =
    con match {
      case PCTrue => SEValue.True
      case PCFalse => SEValue.False
      case PCUnit => SEValue.Unit
    }

  @inline
  private[this] def compilePrimLit(lit: PrimLit): SExpr =
    SEValue(lit match {
      case PLInt64(i) => SInt64(i)
      case PLNumeric(d) => SNumeric(d)
      case PLText(t) => SText(t)
      case PLTimestamp(ts) => STimestamp(ts)
      case PLParty(p) => SParty(p)
      case PLDate(d) => SDate(d)
      case PLRoundingMode(roundingMode) => SInt64(roundingMode.ordinal.toLong)
    })

  // ERecUpd(_, f2, ERecUpd(_, f1, e0, e1), e2) => (e0, [f1, f2], [e1, e2])
  @inline
  private[this] def collectRecUpds(expr: Expr): (Expr, List[Name], List[Expr]) = {
    @tailrec
    def go(expr: Expr, fields: List[Name], updates: List[Expr]): (Expr, List[Name], List[Expr]) =
      stripLocs(expr) match {
        case ERecUpd(_, field, record, update) =>
          go(record, field :: fields, update :: updates)
        case _ =>
          (expr, fields, updates)
      }
    go(expr, List.empty, List.empty)
  }

  private def noArgs = new util.ArrayList[SValue](0)

  @inline
  private[this] def compileERecCon(
      env: Env,
      tApp: TypeConApp,
      fields: ImmArray[(FieldName, Expr)],
  ): SExpr =
    if (fields.isEmpty)
      SEValue(SRecord(tApp.tycon, ImmArray.Empty, noArgs))
    else
      SEApp(
        SEBuiltin(SBRecCon(tApp.tycon, fields.map(_._1))),
        fields.iterator.map(f => compile(env, f._2)).toArray,
      )

  private[this] def compileERecUpd(env: Env, erecupd: ERecUpd): SExpr = {
    val tapp = erecupd.tycon
    val (record, fields, updates) = collectRecUpds(erecupd)
    if (fields.length == 1) {
      val index = handleLookup(
        NameOf.qualifiedNameOfCurrentFunc,
        interface.lookupRecordFieldInfo(tapp.tycon, fields.head),
      ).index
      SBRecUpd(tapp.tycon, index)(compile(env, record), compile(env, updates.head))
    } else {
      val indices =
        fields.map(name =>
          handleLookup(
            NameOf.qualifiedNameOfCurrentFunc,
            interface.lookupRecordFieldInfo(tapp.tycon, name),
          ).index
        )
      SBRecUpdMulti(tapp.tycon, indices.to(ImmArray))((record :: updates).map(compile(env, _)): _*)
    }
  }

  private[this] def compileECase(env: Env, scrut: Expr, alts: ImmArray[CaseAlt]): SExpr =
    SECase(
      compile(env, scrut),
      mapToArray(alts) { case CaseAlt(pat, expr) =>
        pat match {
          case CPVariant(tycon, variant, binder) =>
            val rank = handleLookup(
              NameOf.qualifiedNameOfCurrentFunc,
              interface.lookupVariantConstructor(tycon, variant),
            ).rank
            SCaseAlt(SCPVariant(tycon, variant, rank), compile(env.pushExprVar(binder), expr))

          case CPEnum(tycon, constructor) =>
            val rank = handleLookup(
              NameOf.qualifiedNameOfCurrentFunc,
              interface.lookupEnumConstructor(tycon, constructor),
            )
            SCaseAlt(SCPEnum(tycon, constructor, rank), compile(env, expr))

          case CPNil =>
            SCaseAlt(SCPNil, compile(env, expr))

          case CPCons(head, tail) =>
            SCaseAlt(SCPCons, compile(env.pushExprVar(head).pushExprVar(tail), expr))

          case CPPrimCon(pc) =>
            SCaseAlt(SCPPrimCon(pc), compile(env, expr))

          case CPNone =>
            SCaseAlt(SCPNone, compile(env, expr))

          case CPSome(body) =>
            SCaseAlt(SCPSome, compile(env.pushExprVar(body), expr))

          case CPDefault =>
            SCaseAlt(SCPDefault, compile(env, expr))
        }
      },
    )

  // Compile nested lets using constant stack.
  @tailrec
  private[this] def compileELet(
      env0: Env,
      eLet0: ELet,
      bounds0: List[SExpr] = List.empty,
  ): SELet = {
    val binding = eLet0.binding
    val bounds = withOptLabel(binding.binder, compile(env0, binding.bound)) :: bounds0
    val env1 = env0.pushExprVar(binding.binder)
    eLet0.body match {
      case eLet1: ELet =>
        compileELet(env1, eLet1, bounds)
      case body0 =>
        compile(env1, body0) match {
          case SELet(bounds1, body1) =>
            SELet(bounds.foldLeft(bounds1)((acc, b) => b :: acc), body1)
          case otherwise =>
            SELet(bounds.reverse, otherwise)
        }
    }
  }

  @inline
  private[this] def compileEUpdate(env: Env, update: Update): SExpr =
    update match {
      case UpdatePure(_, e) =>
        compilePure(env, e)
      case UpdateBlock(bindings, body) =>
        compileBlock(env, bindings, body)
      case UpdateFetch(tmplId, coidE) =>
        FetchDefRef(tmplId)(compile(env, coidE))
      case UpdateFetchInterface(ifaceId, coidE) =>
        FetchDefRef(ifaceId)(compile(env, coidE))
      case UpdateEmbedExpr(_, e) =>
        compileEmbedExpr(env, e)
      case UpdateCreate(tmplId, arg) =>
        CreateDefRef(tmplId)(compile(env, arg))
      case UpdateExercise(tmplId, chId, cidE, argE) =>
        ChoiceDefRef(tmplId, chId)(compile(env, cidE), compile(env, argE))
      case UpdateExerciseInterface(ifaceId, chId, cidE, argE) =>
        ChoiceDefRef(ifaceId, chId)(compile(env, cidE), compile(env, argE))
      case UpdateExerciseByKey(tmplId, chId, keyE, argE) =>
        ChoiceByKeyDefRef(tmplId, chId)(compile(env, keyE), compile(env, argE))
      case UpdateGetTime =>
        SEGetTime
      case UpdateLookupByKey(RetrieveByKey(templateId, key)) =>
        LookupByKeyDefRef(templateId)(compile(env, key))
      case UpdateFetchByKey(RetrieveByKey(templateId, key)) =>
        FetchByKeyDefRef(templateId)(compile(env, key))

      case UpdateTryCatch(_, body, binder, handler) =>
        unaryFunction(env) { (tokenPos, env0) =>
          SETryCatch(
            app(compile(env0, body), env0.toSEVar(tokenPos)), {
              val env1 = env0.pushExprVar(binder)
              SBTryHandler(
                compile(env1, handler),
                env1.lookupExprVar(binder),
                env1.toSEVar(tokenPos),
              )
            },
          )
        }
    }

  @tailrec
  private[this] def compileAbss(env: Env, expr0: Expr, arity: Int = 0): SExpr =
    expr0 match {
      case EAbs((binder, typ @ _), body, ref @ _) =>
        compileAbss(env.pushExprVar(binder), body, arity + 1)
      case ETyAbs((binder, KNat), body) =>
        compileAbss(env.pushTypeVar(binder), body, arity + 1)
      case ETyAbs((binder, _), body) =>
        compileAbss(env.hideTypeVar(binder), body, arity)
      case _ if arity == 0 =>
        compile(env, expr0)
      case _ =>
        withLabel(AnonymousClosure, SEAbs(arity, compile(env, expr0)))
    }

  @tailrec
  private[this] def compileApps(env: Env, expr0: Expr, args: List[SExpr] = List.empty): SExpr =
    expr0 match {
      case EApp(fun, arg) =>
        compileApps(env, fun, compile(env, arg) :: args)
      case ETyApp(fun, arg) =>
        compileApps(env, fun, translateType(env, arg).fold(args)(_ :: args))
      case _ if args.isEmpty =>
        compile(env, expr0)
      case _ =>
        SEApp(compile(env, expr0), args.toArray)
    }

  private[this] def translateType(env: Env, typ: Type): Option[SExpr] =
    typ match {
      case TNat(n) => SENat(n)
      case TVar(name) => env.lookupTypeVar(name)
      case _ => None
    }

  private[this] def compileScenario(env: Env, scen: Scenario, optLoc: Option[Location]): SExpr =
    scen match {
      case ScenarioPure(_, e) =>
        compilePure(env, e)
      case ScenarioBlock(bindings, body) =>
        compileBlock(env, bindings, body)
      case ScenarioCommit(partyE, updateE, _retType @ _) =>
        compileCommit(env, partyE, updateE, optLoc, mustFail = false)
      case ScenarioMustFailAt(partyE, updateE, _retType @ _) =>
        compileCommit(env, partyE, updateE, optLoc, mustFail = true)
      case ScenarioGetTime =>
        SEGetTime
      case ScenarioGetParty(e) =>
        compileGetParty(env, e)
      case ScenarioPass(relTime) =>
        compilePass(env, relTime)
      case ScenarioEmbedExpr(_, e) =>
        compileEmbedExpr(env, e)
    }

  @inline
  private[this] def compileCommit(
      env: Env,
      partyE: Expr,
      updateE: Expr,
      optLoc: Option[Location],
      mustFail: Boolean,
  ): SExpr =
    // let party = <partyE>
    //     update = <updateE>
    // in $submit(mustFail)(party, update)
    let(env, compile(env, partyE)) { (partyLoc, env) =>
      let(env, compile(env, updateE)) { (updateLoc, env) =>
        SBSSubmit(optLoc, mustFail)(env.toSEVar(partyLoc), env.toSEVar(updateLoc))
      }
    }

  @inline
  private[this] def compileGetParty(env: Env, expr: Expr): SExpr =
    labeledUnaryFunction(Profile.GetPartyLabel, env) { (tokenPos, env) =>
      SBSGetParty(compile(env, expr), env.toSEVar(tokenPos))
    }

  @inline
  private[this] def compilePass(env: Env, time: Expr): SExpr =
    labeledUnaryFunction(Profile.PassLabel, env) { (tokenPos, env) =>
      SBSPass(compile(env, time), env.toSEVar(tokenPos))
    }

  @inline
  private[this] def compileEmbedExpr(env: Env, expr: Expr): SExpr =
    // EmbedExpr's get wrapped into an extra layer of abstraction
    // to delay evaluation.
    // e.g.
    // embed (error "foo") => \token -> error "foo"
    unaryFunction(env) { (tokenPos, env) =>
      app(compile(env, expr), env.toSEVar(tokenPos))
    }

  private[this] def compilePure(env: Env, body: Expr): SExpr =
    // pure <E>
    // =>
    // ((\x token -> x) <E>)
    let(env, compile(env, body)) { (bodyPos, env) =>
      unaryFunction(env) { (tokenPos, env) =>
        SBSPure(env.toSEVar(bodyPos), env.toSEVar(tokenPos))
      }
    }

  private[this] def compileBlock(env: Env, bindings: ImmArray[Binding], body: Expr): SExpr =
    // do
    //   x <- f
    //   y <- g x
    //   z x y
    // =>
    // let f' = f
    // in \token ->
    //   let x = f' token
    //       y = g x token
    //   in z x y token
    let(env, compile(env, bindings.head.bound)) { (firstPos, env) =>
      unaryFunction(env) { (tokenPos, env) =>
        let(env, app(env.toSEVar(firstPos), env.toSEVar(tokenPos))) { (firstBoundPos, _env) =>
          val env = bindings.head.binder.fold(_env)(_env.bindExprVar(_, firstBoundPos))

          def loop(env: Env, list: List[Binding]): SExpr = list match {
            case Binding(binder, _, bound) :: tail =>
              let(env, app(compile(env, bound), env.toSEVar(tokenPos))) { (boundPos, _env) =>
                val env = binder.fold(_env)(_env.bindExprVar(_, boundPos))
                loop(env, tail)
              }
            case Nil =>
              app(compile(env, body), env.toSEVar(tokenPos))
          }

          loop(env, bindings.tail.toList)
        }
      }
    }

  private[this] val KeyWithMaintainersStruct =
    SBStructCon(Struct.assertFromSeq(List(keyFieldName, maintainersFieldName).zipWithIndex))

  private[this] def encodeKeyWithMaintainers(
      env: Env,
      keyPos: Position,
      tmplKey: TemplateKey,
  ): SExpr =
    KeyWithMaintainersStruct(
      env.toSEVar(keyPos),
      app(compile(env, tmplKey.maintainers), env.toSEVar(keyPos)),
    )

  private[this] def compileKeyWithMaintainers(env: Env, maybeTmplKey: Option[TemplateKey]): SExpr =
    maybeTmplKey match {
      case None => SEValue.None
      case Some(tmplKey) =>
        let(env, compile(env, tmplKey.body)) { (keyPos, env) =>
          SBSome(encodeKeyWithMaintainers(env, keyPos, tmplKey))
        }
    }

  private[this] def compileChoiceBody(
      env: Env,
      tmplId: TypeConName,
      tmpl: Template,
      choice: TemplateChoice,
  )(
      choiceArgPos: Position,
      cidPos: Position,
      mbKey: Option[Position], // defined for byKey operation
      tokenPos: Position,
  ) =
    let(
      env,
      SBUFetch(
        tmplId
      )(env.toSEVar(cidPos), mbKey.fold(SEValue.None: SExpr)(pos => SBSome(env.toSEVar(pos)))),
    ) { (tmplArgPos, _env) =>
      val env =
        _env.bindExprVar(tmpl.param, tmplArgPos).bindExprVar(choice.argBinder._1, choiceArgPos)
      let(
        env,
        SBUBeginExercise(tmplId, choice.name, choice.consuming, byKey = mbKey.isDefined)(
          env.toSEVar(choiceArgPos),
          env.toSEVar(cidPos),
          compile(env, choice.controllers),
          choice.choiceObservers match {
            case Some(observers) => compile(env, observers)
            case None => SEValue.EmptyList
          },
        ),
      ) { (_, _env) =>
        val env = _env.bindExprVar(choice.selfBinder, cidPos)
        SEScopeExercise(
          app(compile(env, choice.update), env.toSEVar(tokenPos))
        )
      }
    }

  // TODO https://github.com/digital-asset/daml/issues/10810:
  //   Try to factorise this with compileChoiceBody above.
  private[this] def compileFixedChoiceBody(
      env: Env,
      ifaceId: TypeConName,
      param: ExprVarName,
      choice: TemplateChoice,
  )(
      choiceArgPos: Position,
      cidPos: Position,
      tokenPos: Position,
  ) =
    let(env, SBUFetchInterface(ifaceId)(env.toSEVar(cidPos))) { (payloadPos, _env) =>
      val env = _env.bindExprVar(param, payloadPos).bindExprVar(choice.argBinder._1, choiceArgPos)
      let(
        env,
        SBResolveSBUBeginExercise(choice.name, choice.consuming, byKey = false)(
          env.toSEVar(payloadPos),
          env.toSEVar(choiceArgPos),
          env.toSEVar(cidPos),
          compile(env, choice.controllers),
          choice.choiceObservers match {
            case Some(observers) => compile(env, observers)
            case None => SEValue.EmptyList
          },
        ),
      ) { (_, _env) =>
        val env = _env.bindExprVar(choice.selfBinder, cidPos)
        SEScopeExercise(app(compile(env, choice.update), env.toSEVar(tokenPos)))
      }
    }

  // TODO https://github.com/digital-asset/daml/issues/10810:
  //  Here we fetch twice, once by interface Id once by template Id. Try to bypass the second fetch.
  private[this] def compileVirtualChoice(
      ifaceId: TypeConName,
      choice: InterfaceChoice,
  ): (SDefinitionRef, SDefinition) =
    topLevelFunction3(ChoiceDefRef(ifaceId, choice.name)) { (cidPos, choiceArgPos, tokenPos, env) =>
      let(env, SBUFetchInterface(ifaceId)(env.toSEVar(cidPos))) { (payloadPos, env) =>
        SBResolveVirtualChoice(choice.name)(
          env.toSEVar(payloadPos),
          env.toSEVar(cidPos),
          env.toSEVar(choiceArgPos),
          env.toSEVar(tokenPos),
        )
      }
    }

  private[this] def compileFixedChoice(
      ifaceId: TypeConName,
      param: ExprVarName,
      choice: TemplateChoice,
  ): (SDefinitionRef, SDefinition) =
    topLevelFunction3(ChoiceDefRef(ifaceId, choice.name)) { (cidPos, choiceArgPos, tokenPos, env) =>
      compileFixedChoiceBody(env, ifaceId, param, choice)(
        choiceArgPos,
        cidPos,
        tokenPos,
      )
    }

  private[this] def compileChoice(
      tmplId: TypeConName,
      tmpl: Template,
      choice: TemplateChoice,
  ): (SDefinitionRef, SDefinition) =
    // Compiles a choice into:
    // ChoiceDefRef(SomeTemplate, SomeChoice) = \<actors> <cid> <choiceArg> <token> ->
    //   let targ = fetch(tmplId) <cid>
    //       _ = $beginExercise(tmplId, choice.name, choice.consuming, false) <choiceArg> <cid> <actors> [tmpl.signatories] [tmpl.observers] [choice.controllers] [tmpl.key]
    //       <retValue> = [update] <token>
    //       _ = $endExercise[tmplId] <retValue>
    //   in <retValue>
    topLevelFunction3(ChoiceDefRef(tmplId, choice.name)) { (cidPos, choiceArgPos, tokenPos, env) =>
      compileChoiceBody(env, tmplId, tmpl, choice)(
        choiceArgPos,
        cidPos,
        None,
        tokenPos,
      )
    }

  /** Compile a choice into a top-level function for exercising that choice */
  private[this] def compileChoiceByKey(
      tmplId: TypeConName,
      tmpl: Template,
      tmplKey: TemplateKey,
      choice: TemplateChoice,
  ): (SDefinitionRef, SDefinition) =
    // Compiles a choice into:
    // ChoiceByKeyDefRef(SomeTemplate, SomeChoice) = \ <actors> <key> <choiceArg> <token> ->
    //    let <keyWithM> = { key = <key> ; maintainers = [tmpl.maintainers] <key> }
    //        <cid> = $fecthKey(tmplId) <keyWithM>
    //        targ = fetch <cid>
    //       _ = $beginExercise[tmplId,  choice.name, choice.consuming, true] <choiceArg> <cid> <actors> [tmpl.signatories] [tmpl.observers] [choice.controllers] (Some <keyWithM>)
    //       <retValue> = <updateE> <token>
    //       _ = $endExercise[tmplId] <retValue>
    //   in  <retValue>
    topLevelFunction3(ChoiceByKeyDefRef(tmplId, choice.name)) {
      (keyPos, choiceArgPos, tokenPos, env) =>
        let(env, encodeKeyWithMaintainers(env, keyPos, tmplKey)) { (keyWithMPos, env) =>
          let(env, SBUFetchKey(tmplId)(env.toSEVar(keyWithMPos))) { (cidPos, env) =>
            compileChoiceBody(env, tmplId, tmpl, choice)(
              choiceArgPos,
              cidPos,
              Some(keyWithMPos),
              tokenPos,
            )
          }
        }
    }

  @tailrec
  private[this] def stripLocs(expr: Expr): Expr =
    expr match {
      case ELocation(_, expr1) => stripLocs(expr1)
      case _ => expr
    }

  /** Convert abstractions in a speedy expression into
    * explicit closure creations.
    * This step computes the free variables in an abstraction
    * body, then translates the references in the body into
    * references to the immediate top of the argument stack,
    * and changes the abstraction into a closure creation node
    * describing the free variables that need to be captured.
    *
    * For example:
    *   SELet(..two-bindings..) in
    *     SEAbs(2,
    *       SEVar(4) ..             [reference to first let-bound variable]
    *       SEVar(2))               [reference to first function-arg]
    * =>
    *   SELet(..two-bindings..) in
    *     SEMakeClo(
    *       Array(SELocS(2)),       [capture the first let-bound variable, from the stack]
    *       2,
    *       SELocF(0) ..            [reference the first let-bound variable via the closure]
    *       SELocA(0))              [reference the first function arg]
    */
  private[this] def closureConvert(remaps: Map[Int, SELoc], expr: SExpr): SExpr = {
    // remaps is a function which maps the relative offset from variables (SEVar) to their runtime location
    // The Map must contain a binding for every variable referenced.
    // The Map is consulted when translating variable references (SEVar) and free variables of an abstraction (SEAbs)
    def remap(i: Int): SELoc =
      remaps.get(i) match {
        case Some(loc) => loc
        case None =>
          throw CompilationError(s"remap($i),remaps=$remaps")
      }
    expr match {
      case SEVar(i) => remap(i)
      case v: SEVal => v
      case be: SEBuiltin => be
      case pl: SEValue => pl
      case f: SEBuiltinRecursiveDefinition => f
      case SELocation(loc, body) =>
        SELocation(loc, closureConvert(remaps, body))

      case SEAbs(0, _) =>
        throw CompilationError("empty SEAbs")

      case SEAbs(arity, body) =>
        val fvs = freeVars(body, arity).toList.sorted
        val newRemapsF: Map[Int, SELoc] = fvs.zipWithIndex.map { case (orig, i) =>
          (orig + arity) -> SELocF(i)
        }.toMap
        val newRemapsA = (1 to arity).map { case i =>
          i -> SELocA(arity - i)
        }
        // The keys in newRemapsF and newRemapsA are disjoint
        val newBody = closureConvert(newRemapsF ++ newRemapsA, body)
        SEMakeClo(fvs.map(remap).toArray, arity, newBody)

      case SEAppGeneral(fun, args) =>
        val newFun = closureConvert(remaps, fun)
        val newArgs = args.map(closureConvert(remaps, _))
        SEApp(newFun, newArgs)

      case SEAppAtomicFun(fun, args) =>
        val newFun = closureConvert(remaps, fun)
        val newArgs = args.map(closureConvert(remaps, _))
        SEApp(newFun, newArgs)

      case SECase(scrut, alts) =>
        SECase(
          closureConvert(remaps, scrut),
          alts.map { case SCaseAlt(pat, body) =>
            val n = patternNArgs(pat)
            SCaseAlt(
              pat,
              closureConvert(shift(remaps, n), body),
            )
          },
        )

      case SELet(bounds, body) =>
        SELet(
          bounds.zipWithIndex.map { case (b, i) =>
            closureConvert(shift(remaps, i), b)
          },
          closureConvert(shift(remaps, bounds.length), body),
        )

      case SETryCatch(body, handler) =>
        SETryCatch(
          closureConvert(remaps, body),
          closureConvert(shift(remaps, 1), handler),
        )

      case SEScopeExercise(body) =>
        SEScopeExercise(closureConvert(remaps, body))

      case SELabelClosure(label, expr) =>
        SELabelClosure(label, closureConvert(remaps, expr))

      case SELet1General(bound, body) =>
        SELet1General(closureConvert(remaps, bound), closureConvert(shift(remaps, 1), body))

      case _: SELoc | _: SEMakeClo | _: SELet1Builtin | _: SELet1BuiltinArithmetic |
          _: SEDamlException | _: SEImportValue | _: SEAppAtomicGeneral |
          _: SEAppAtomicSaturatedBuiltin | _: SECaseAtomic =>
        throw CompilationError(s"closureConvert: unexpected $expr")
    }
  }

  // Modify/extend `remaps` to reflect when new values are pushed on the stack.  This
  // happens as we traverse into SELet and SECase bodies which have bindings which at
  // runtime will appear on the stack.
  // We must modify `remaps` because it is keyed by indexes relative to the end of the stack.
  // And any values in the map which are of the form SELocS must also be _shifted_
  // because SELocS indexes are also relative to the end of the stack.
  private[this] def shift(remaps: Map[Int, SELoc], n: Int): Map[Int, SELoc] = {

    // We must update both the keys of the map (the relative-indexes from the original SEVar)
    // And also any values in the map which are stack located (SELocS), which are also indexed relatively
    val m1 = remaps.map { case (k, loc) => (n + k, shiftLoc(loc, n)) }

    // And create mappings for the `n` new stack items
    val m2 = (1 to n).map(i => (i, SELocS(i)))

    m1 ++ m2
  }

  private[this] def shiftLoc(loc: SELoc, n: Int): SELoc = loc match {
    case SELocS(i) => SELocS(i + n)
    case SELocA(_) | SELocF(_) => loc
  }

  /** Compute the free variables in a speedy expression.
    * The returned free variables are de bruijn indices
    * adjusted to the stack of the caller.
    */
  private[this] def freeVars(expr: SExpr, initiallyBound: Int): Set[Int] = {
    def go(expr: SExpr, bound: Int, free: Set[Int]): Set[Int] =
      expr match {
        case SEVar(i) =>
          if (i > bound) free + (i - bound) else free /* adjust to caller's environment */
        case _: SEVal => free
        case _: SEBuiltin => free
        case _: SEValue => free
        case _: SEBuiltinRecursiveDefinition => free
        case SELocation(_, body) =>
          go(body, bound, free)
        case SEAppGeneral(fun, args) =>
          args.foldLeft(go(fun, bound, free))((acc, arg) => go(arg, bound, acc))
        case SEAppAtomicFun(fun, args) =>
          args.foldLeft(go(fun, bound, free))((acc, arg) => go(arg, bound, acc))
        case SEAbs(n, body) =>
          go(body, bound + n, free)
        case SECase(scrut, alts) =>
          alts.foldLeft(go(scrut, bound, free)) { case (acc, SCaseAlt(pat, body)) =>
            go(body, bound + patternNArgs(pat), acc)
          }
        case SELet(bounds, body) =>
          bounds.zipWithIndex.foldLeft(go(body, bound + bounds.length, free)) {
            case (acc, (expr, idx)) => go(expr, bound + idx, acc)
          }
        case SELabelClosure(_, expr) =>
          go(expr, bound, free)
        case SETryCatch(body, handler) =>
          go(body, bound, go(handler, 1 + bound, free))
        case SEScopeExercise(body) =>
          go(body, bound, free)

        case _: SELoc | _: SEMakeClo | _: SEDamlException | _: SEImportValue |
            _: SEAppAtomicGeneral | _: SEAppAtomicSaturatedBuiltin | _: SELet1General |
            _: SELet1Builtin | _: SELet1BuiltinArithmetic | _: SECaseAtomic =>
          throw CompilationError(s"freeVars: unexpected $expr")
      }

    go(expr, initiallyBound, Set.empty)
  }

  /** Validate variable references in a speedy expression */
  // validate that we correctly captured all free-variables, and so reference to them is
  // via the surrounding closure, instead of just finding them higher up on the stack
  private[this] def validate(expr0: SExpr): SExpr = {

    def goV(v: SValue): Unit =
      v match {
        case _: SPrimLit | STNat(_) | STypeRep(_) =>
        case SList(a) => a.iterator.foreach(goV)
        case SOptional(x) => x.foreach(goV)
        case SMap(_, entries) =>
          entries.foreach { case (k, v) =>
            goV(k)
            goV(v)
          }
        case SRecord(_, _, args) => args.forEach(goV)
        case SVariant(_, _, _, value) => goV(value)
        case SEnum(_, _, _) => ()
        case SAny(_, v) => goV(v)
        case _: SPAP | SToken | SStruct(_, _) =>
          throw CompilationError("validate: unexpected SEValue")
      }

    def goBody(maxS: Int, maxA: Int, maxF: Int): SExpr => Unit = {

      def goLoc(loc: SELoc) = loc match {
        case SELocS(i) =>
          if (i < 1 || i > maxS)
            throw CompilationError(s"validate: SELocS: index $i out of range ($maxS..1)")
        case SELocA(i) =>
          if (i < 0 || i >= maxA)
            throw CompilationError(s"validate: SELocA: index $i out of range (0..$maxA-1)")
        case SELocF(i) =>
          if (i < 0 || i >= maxF)
            throw CompilationError(s"validate: SELocF: index $i out of range (0..$maxF-1)")
      }

      def go(expr: SExpr): Unit = expr match {
        case loc: SELoc => goLoc(loc)
        case _: SEVal => ()
        case _: SEBuiltin => ()
        case _: SEBuiltinRecursiveDefinition => ()
        case SEValue(v) => goV(v)
        case SEAppAtomicGeneral(fun, args) =>
          go(fun)
          args.foreach(go)
        case SEAppAtomicSaturatedBuiltin(_, args) =>
          args.foreach(go)
        case SEAppGeneral(fun, args) =>
          go(fun)
          args.foreach(go)
        case SEAppAtomicFun(fun, args) =>
          go(fun)
          args.foreach(go)
        case SEMakeClo(fvs, n, body) =>
          fvs.foreach(goLoc)
          goBody(0, n, fvs.length)(body)
        case SECaseAtomic(scrut, alts) => go(SECase(scrut, alts))
        case SECase(scrut, alts) =>
          go(scrut)
          alts.foreach { case SCaseAlt(pat, body) =>
            val n = patternNArgs(pat)
            goBody(maxS + n, maxA, maxF)(body)
          }
        case SELet(bounds, body) =>
          bounds.zipWithIndex.foreach { case (rhs, i) =>
            goBody(maxS + i, maxA, maxF)(rhs)
          }
          goBody(maxS + bounds.length, maxA, maxF)(body)
        case _: SELet1General => goLets(maxS)(expr)
        case _: SELet1Builtin => goLets(maxS)(expr)
        case _: SELet1BuiltinArithmetic => goLets(maxS)(expr)
        case SELocation(_, body) =>
          go(body)
        case SELabelClosure(_, expr) =>
          go(expr)
        case SETryCatch(body, handler) =>
          go(body)
          goBody(maxS + 1, maxA, maxF)(handler)
        case SEScopeExercise(body) =>
          go(body)

        case _: SEVar | _: SEAbs | _: SEDamlException | _: SEImportValue =>
          throw CompilationError(s"validate: unexpected $expr")
      }
      @tailrec
      def goLets(maxS: Int)(expr: SExpr): Unit = {
        def go = goBody(maxS, maxA, maxF)
        expr match {
          case SELet1General(rhs, body) =>
            go(rhs)
            goLets(maxS + 1)(body)
          case SELet1Builtin(_, args, body) =>
            args.foreach(go)
            goLets(maxS + 1)(body)
          case SELet1BuiltinArithmetic(_, args, body) =>
            args.foreach(go)
            goLets(maxS + 1)(body)
          case expr =>
            go(expr)
        }
      }
      go
    }
    goBody(0, 0, 0)(expr0)
    expr0
  }

  @nowarn("msg=parameter value tokenPos in method compileFetchBody is never used")
  private[this] def compileFetchBody(env: Env, tmplId: Identifier, tmpl: Template)(
      cidPos: Position,
      mbKey: Option[Position], //defined for byKey operation
      tokenPos: Position,
  ) =
    let(
      env,
      SBUFetch(
        tmplId
      )(env.toSEVar(cidPos), mbKey.fold(SEValue.None: SExpr)(pos => SBSome(env.toSEVar(pos)))),
    ) { (tmplArgPos, _env) =>
      val env = _env.bindExprVar(tmpl.param, tmplArgPos)
      let(
        env,
        SBUInsertFetchNode(tmplId, byKey = mbKey.isDefined)(env.toSEVar(cidPos)),
      ) { (_, env) =>
        env.toSEVar(tmplArgPos)
      }
    }

  private[this] def compileFetch(
      tmplId: Identifier,
      tmpl: Template,
  ): (SDefinitionRef, SDefinition) =
    // compile a template to
    // FetchDefRef(tmplId) = \ <coid> <token> ->
    //   let <tmplArg> = $fetch(tmplId) <coid>
    //       _ = $insertFetch(tmplId, false) coid [tmpl.signatories] [tmpl.observers] [tmpl.key]
    //   in <tmplArg>
    topLevelFunction2(FetchDefRef(tmplId)) { (cidPos, tokenPos, env) =>
      compileFetchBody(env, tmplId, tmpl)(cidPos, None, tokenPos)
    }

  // TODO https://github.com/digital-asset/daml/issues/10810:
  //  Here we fetch twice, once by interface Id once by template Id. Try to bypass the second fetch.
  private[this] def compileFetchInterface(
      ifaceId: Identifier
  ): (SDefinitionRef, SDefinition) =
    topLevelFunction2(FetchDefRef(ifaceId)) { (cidPos, tokenPos, env) =>
      let(env, SBUFetchInterface(ifaceId)(env.toSEVar(cidPos))) { (payloadPos, env) =>
        SBResolveVirtualFetch(
          env.toSEVar(payloadPos),
          env.toSEVar(cidPos),
          env.toSEVar(tokenPos),
        )
      }
    }

  private[this] def compileKey(
      tmplId: Identifier,
      tmpl: Template,
  ): (SDefinitionRef, SDefinition) =
    topLevelFunction1(KeyDefRef(tmplId)) { (tmplArgPos, env) =>
      compileKeyWithMaintainers(env.bindExprVar(tmpl.param, tmplArgPos), tmpl.key)
    }

  private[this] def compileSignatories(
      tmplId: Identifier,
      tmpl: Template,
  ): (SDefinitionRef, SDefinition) =
    topLevelFunction1(SignatoriesDefRef(tmplId)) { (tmplArgPos, env) =>
      compile(env.bindExprVar(tmpl.param, tmplArgPos), tmpl.signatories)
    }

  private[this] def compileObservers(
      tmplId: Identifier,
      tmpl: Template,
  ): (SDefinitionRef, SDefinition) =
    topLevelFunction1(ObserversDefRef(tmplId)) { (tmplArgPos, env) =>
      compile(env.bindExprVar(tmpl.param, tmplArgPos), tmpl.observers)
    }

  // Turn a template value into an interface value. Since interfaces have a
  // toll-free representation (for now), this is just the identity function.
  // But the existence of ImplementsDefRef implies that the template implements
  // the interface, which is useful in itself.
  private[this] def compileImplements(
      tmplId: Identifier,
      ifaceId: Identifier,
  ): (SDefinitionRef, SDefinition) =
    ImplementsDefRef(tmplId, ifaceId) ->
      SDefinition(flattenToAnf(unsafeClosureConvert(SEAbs.identity)))

  // Compile the implementation of an interface method.
  private[this] def compileImplementsMethod(
      tmplId: Identifier,
      ifaceId: Identifier,
      method: TemplateImplementsMethod,
  ): (SDefinitionRef, SDefinition) = {
    val ref = ImplementsMethodDefRef(tmplId, ifaceId, method.name)
    ref -> SDefinition(withLabel(ref, unsafeCompile(method.value)))
  }

  private[this] def compileCreate(
      tmplId: Identifier,
      tmpl: Template,
  ): (SDefinitionRef, SDefinition) =
    // Translates 'create Foo with <params>' into:
    // CreateDefRef(tmplId) = \ <tmplArg> <token> ->
    //   let _ = $checkPreconf(tmplId)(<tmplArg> [tmpl.precond]
    //   in $create <tmplArg> [tmpl.agreementText] [tmpl.signatories] [tmpl.observers] [tmpl.key]
    topLevelFunction2(CreateDefRef(tmplId)) { (tmplArgPos, _, _env) =>
      val env = _env.bindExprVar(tmpl.param, tmplArgPos)
      // We check precondition in a separated builtin to prevent
      // further evaluation of agreement, signatories, observers and key
      // in case of failed precondition.
      let(env, SBCheckPrecond(tmplId)(env.toSEVar(tmplArgPos), compile(env, tmpl.precond))) {
        (_, env) =>
          SBUCreate(tmplId)(
            env.toSEVar(tmplArgPos),
            compile(env, tmpl.agreementText),
            compile(env, tmpl.signatories),
            compile(env, tmpl.observers),
            compileKeyWithMaintainers(env, tmpl.key),
          )
      }
    }

  private[this] def compileCreateAndExercise(
      tmplId: Identifier,
      createArg: SValue,
      choiceId: ChoiceName,
      choiceArg: SValue,
  ): SExpr =
    labeledUnaryFunction(Profile.CreateAndExerciseLabel(tmplId, choiceId), Env.Empty) {
      (tokenPos, env) =>
        let(env, CreateDefRef(tmplId)(SEValue(createArg), env.toSEVar(tokenPos))) { (cidPos, env) =>
          ChoiceDefRef(tmplId, choiceId)(
            env.toSEVar(cidPos),
            SEValue(choiceArg),
            env.toSEVar(tokenPos),
          )
        }
    }

  private[this] def compileLookupByKey(
      tmplId: Identifier,
      tmplKey: TemplateKey,
  ): (SDefinitionRef, SDefinition) =
    // compile a template with key into
    // LookupByKeyDefRef(tmplId) = \ <key> <token> ->
    //    let <keyWithM> = { key = <key> ; maintainers = [tmplKey.maintainers] <key> }
    //        <mbCid> = $lookupKey(tmplId) <keyWithM>
    //        _ = $insertLookup(tmplId> <keyWithM> <mbCid>
    //    in <mbCid>
    topLevelFunction2(LookupByKeyDefRef(tmplId)) { (keyPos, _, env) =>
      let(env, encodeKeyWithMaintainers(env, keyPos, tmplKey)) { (keyWithMPos, env) =>
        let(env, SBULookupKey(tmplId)(env.toSEVar(keyWithMPos))) { (maybeCidPos, env) =>
          let(
            env,
            SBUInsertLookupNode(tmplId)(env.toSEVar(keyWithMPos), env.toSEVar(maybeCidPos)),
          ) { (_, env) =>
            env.toSEVar(maybeCidPos)
          }
        }
      }
    }

  private[this] val FetchByKeyResult =
    SBStructCon(Struct.assertFromSeq(List(contractIdFieldName, contractFieldName).zipWithIndex))

  @inline
  private[this] def compileFetchByKey(
      tmplId: TypeConName,
      tmpl: Template,
      tmplKey: TemplateKey,
  ): (SDefinitionRef, SDefinition) =
    // compile a template with key into
    // FetchByKeyDefRef(tmplId) = \ <key> <token> ->
    //    let <keyWithM> = { key = <key> ; maintainers = [tmpl.maintainers] <key> }
    //        <coid> = $fetchKey(tmplId) <keyWithM>
    //        <contract> = $fetch(tmplId) <coid>
    //        _ = $insertFetch <coid> <signatories> <observers> (Some <keyWithM> )
    //    in { contractId: ContractId Foo, contract: Foo }
    topLevelFunction2(FetchByKeyDefRef(tmplId)) { (keyPos, tokenPos, env) =>
      let(env, encodeKeyWithMaintainers(env, keyPos, tmplKey)) { (keyWithMPos, env) =>
        let(env, SBUFetchKey(tmplId)(env.toSEVar(keyWithMPos))) { (cidPos, env) =>
          let(env, compileFetchBody(env, tmplId, tmpl)(cidPos, Some(keyWithMPos), tokenPos)) {
            (contractPos, env) =>
              FetchByKeyResult(env.toSEVar(cidPos), env.toSEVar(contractPos))
          }
        }
      }
    }

  private[this] def compileCommand(cmd: Command): SExpr = cmd match {
    case Command.Create(templateId, argument) =>
      CreateDefRef(templateId)(SEValue(argument))
    case Command.Exercise(templateId, contractId, choiceId, argument) =>
      ChoiceDefRef(templateId, choiceId)(SEValue(contractId), SEValue(argument))
    case Command.ExerciseByKey(templateId, contractKey, choiceId, argument) =>
      ChoiceByKeyDefRef(templateId, choiceId)(SEValue(contractKey), SEValue(argument))
    case Command.Fetch(templateId, coid) =>
      FetchDefRef(templateId)(SEValue(coid))
    case Command.FetchByKey(templateId, key) =>
      FetchByKeyDefRef(templateId)(SEValue(key))
    case Command.CreateAndExercise(templateId, createArg, choice, choiceArg) =>
      compileCreateAndExercise(
        templateId,
        createArg,
        choice,
        choiceArg,
      )
    case Command.LookupByKey(templateId, contractKey) =>
      LookupByKeyDefRef(templateId)(SEValue(contractKey))
  }

  private val SEUpdatePureUnit = unaryFunction(Env.Empty)((_, _) => SEValue.Unit)

  private[this] val handleEverything: SExpr = SBSome(SEUpdatePureUnit)

  private[this] def catchEverything(e: SExpr): SExpr =
    unaryFunction(Env.Empty) { (tokenPos, env0) =>
      SETryCatch(
        app(e, env0.toSEVar(tokenPos)), {
          val binderPos = env0.nextPosition
          val env1 = env0.pushVar
          SBTryHandler(handleEverything, env1.toSEVar(binderPos), env1.toSEVar(tokenPos))
        },
      )
    }

  private[this] def compileCommandForReinterpretation(cmd: Command): SExpr =
    catchEverything(compileCommand(cmd))

  private[this] def compileCommands(bindings: ImmArray[Command]): SExpr =
    // commands are compile similarly as update block
    // see compileBlock
    bindings.toList match {
      case Nil =>
        SEUpdatePureUnit
      case first :: rest =>
        let(Env.Empty, compileCommand(first)) { (firstPos, env) =>
          unaryFunction(env) { (tokenPos, env) =>
            let(env, app(env.toSEVar(firstPos), env.toSEVar(tokenPos))) { (_, _env) =>
              // we cannot process `rest` recursively without exposing ourselves to stack overflow.
              var env = _env
              val exprs = rest.map { cmd =>
                val expr = app(compileCommand(cmd), env.toSEVar(tokenPos))
                env = env.pushVar
                expr
              }
              SELet(exprs, SEValue.Unit)
            }
          }
        }
    }
}
