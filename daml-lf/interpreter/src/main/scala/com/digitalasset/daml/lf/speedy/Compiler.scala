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
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.{SExpr0 => s}
import com.daml.lf.speedy.{SExpr => t}
import com.daml.lf.speedy.ClosureConversion.closureConvert
import com.daml.lf.speedy.ValidateCompilation.validateCompilation
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

  private val SEGetTime = s.SEBuiltin(SBGetTime)

  private def SBCompareNumeric(b: SBuiltinPure) =
    s.SEAbs(3, s.SEApp(s.SEBuiltin(b), Array(s.SEVar(2), s.SEVar(1))))
  private val SBLessNumeric = SBCompareNumeric(SBLess)
  private val SBLessEqNumeric = SBCompareNumeric(SBLessEq)
  private val SBGreaterNumeric = SBCompareNumeric(SBGreater)
  private val SBGreaterEqNumeric = SBCompareNumeric(SBGreaterEq)
  private val SBEqualNumeric = SBCompareNumeric(SBEqual)

  private val SBEToTextNumeric = s.SEAbs(1, s.SEBuiltin(SBToText))

  private val SENat: Numeric.Scale => Some[s.SEValue] =
    Numeric.Scale.values.map(n => Some(s.SEValue(STNat(n))))

  /** Validates and Compiles all the definitions in the packages provided. Returns them in a Map.
    *
    * The packages do not need to be in any specific order, as long as they and all the packages
    * they transitively reference are in the [[packages]] in the compiler.
    */
  def compilePackages(
      interface: PackageInterface,
      packages: Map[PackageId, Package],
      compilerConfig: Compiler.Config,
  ): Either[String, Map[t.SDefinitionRef, SDefinition]] = {
    val compiler = new Compiler(interface, compilerConfig)
    try {
      Right(packages.foldLeft(Map.empty[t.SDefinitionRef, SDefinition]) {
        case (acc, (pkgId, pkg)) =>
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

  private[this] val stablePackageIds = StablePackages.ids(config.allowedLanguageVersions)

  private[this] def handleLookup[X](location: String, x: Either[LookupError, X]) =
    x match {
      case Right(value) => value
      case Left(err) => throw SError.SErrorCrash(location, err.pretty)
    }

  // Stack-trace support is disabled by avoiding the construction of SELocation nodes.
  private[this] def maybeSELocation(loc: Location, sexp: s.SExpr): s.SExpr = {
    config.stacktracing match {
      case NoStackTrace => sexp
      case FullStackTrace => s.SELocation(loc, sexp)
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

    def toSEVar(p: Position): s.SEVar = s.SEVar(position - p.idx)

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

    private[this] def lookupVar(varRef: VarRef): Option[s.SEVar] =
      varIndices.get(varRef).map(toSEVar)

    def lookupExprVar(name: ExprVarName): s.SEVar =
      lookupVar(EVarRef(name))
        .getOrElse(throw CompilationError(s"Unknown variable: $name. Known: ${vars.mkString(",")}"))

    def lookupTypeVar(name: TypeVarName): Option[s.SEVar] =
      lookupVar(TVarRef(name))

  }

  // We add labels before and after flattenning

  private[this] val withLabelS: (Profile.Label, s.SExpr) => s.SExpr =
    config.profiling match {
      case NoProfile => { (_, expr) =>
        expr
      }
      case FullProfile => { (label, expr) =>
        expr match {
          case s.SELabelClosure(_, expr1) => s.SELabelClosure(label, expr1)
          case _ => s.SELabelClosure(label, expr)
        }
      }
    }

  private[this] val withLabelT: (Profile.Label, t.SExpr) => t.SExpr =
    config.profiling match {
      case NoProfile => { (_, expr) =>
        expr
      }
      case FullProfile => { (label, expr) =>
        expr match {
          case t.SELabelClosure(_, expr1) => t.SELabelClosure(label, expr1)
          case _ => t.SELabelClosure(label, expr)
        }
      }
    }

  private[this] def withOptLabelS[L: Profile.LabelModule.Allowed](
      optLabel: Option[L with AnyRef],
      expr: s.SExpr,
  ): s.SExpr =
    optLabel match {
      case Some(label) => withLabelS(label, expr)
      case None => expr
    }

  private[this] def app(f: s.SExpr, a: s.SExpr) = s.SEApp(f, Array(a))

  private[this] def let(env: Env, bound: s.SExpr)(f: (Position, Env) => s.SExpr): s.SELet =
    f(env.nextPosition, env.pushVar) match {
      case s.SELet(bounds, body) =>
        s.SELet(bound :: bounds, body)
      case otherwise =>
        s.SELet(List(bound), otherwise)
    }

  private[this] def unaryFunction(env: Env)(f: (Position, Env) => s.SExpr): s.SEAbs =
    f(env.nextPosition, env.pushVar) match {
      case s.SEAbs(n, body) => s.SEAbs(n + 1, body)
      case otherwise => s.SEAbs(1, otherwise)
    }

  private[this] def labeledUnaryFunction[L: Profile.LabelModule.Allowed](
      label: L with AnyRef,
      env: Env,
  )(
      body: (Position, Env) => s.SExpr
  ): s.SExpr =
    unaryFunction(env)((positions, env) => withLabelS(label, body(positions, env)))

  private[this] def topLevelFunction[SDefRef <: t.SDefinitionRef: LabelModule.Allowed](
      ref: SDefRef
  )(
      body: s.SExpr
  ): (SDefRef, SDefinition) =
    ref -> SDefinition(unsafeClosureConvert(withLabelS(ref, body)))

  private val Position1 = Env.Empty.nextPosition
  private val Env1 = Env.Empty.pushVar
  private val Position2 = Env1.nextPosition
  private val Env2 = Env1.pushVar
  private val Position3 = Env2.nextPosition
  private val Env3 = Env2.pushVar
  private val Position4 = Env3.nextPosition
  private val Env4 = Env3.pushVar

  private[this] def topLevelFunction1[SDefRef <: t.SDefinitionRef: LabelModule.Allowed](
      ref: SDefRef
  )(
      body: (Position, Env) => s.SExpr
  ): (SDefRef, SDefinition) =
    topLevelFunction(ref)(s.SEAbs(1, body(Position1, Env1)))

  private[this] def topLevelFunction2[SDefRef <: t.SDefinitionRef: LabelModule.Allowed](
      ref: SDefRef
  )(
      body: (Position, Position, Env) => s.SExpr
  ): (SDefRef, SDefinition) =
    topLevelFunction(ref)(s.SEAbs(2, body(Position1, Position2, Env2)))

  private[this] def topLevelFunction3[SDefRef <: t.SDefinitionRef: LabelModule.Allowed](
      ref: SDefRef
  )(
      body: (Position, Position, Position, Env) => s.SExpr
  ): (SDefRef, SDefinition) =
    topLevelFunction(ref)(s.SEAbs(3, body(Position1, Position2, Position3, Env3)))

  private[this] def topLevelFunction4[SDefRef <: t.SDefinitionRef: LabelModule.Allowed](
      ref: SDefRef
  )(
      body: (Position, Position, Position, Position, Env) => s.SExpr
  ): (SDefRef, SDefinition) =
    topLevelFunction(ref)(s.SEAbs(4, body(Position1, Position2, Position3, Position4, Env4)))

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompile(cmds: ImmArray[Command]): t.SExpr =
    validateCompilation(compilationPipeline(compileCommands(cmds)))

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompileForReinterpretation(cmd: Command): t.SExpr =
    validateCompilation(compilationPipeline(compileCommandForReinterpretation(cmd)))

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompile(expr: Expr): t.SExpr =
    validateCompilation(compilationPipeline(compile(Env.Empty, expr)))

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeClosureConvert(sexpr: s.SExpr): t.SExpr =
    validateCompilation(compilationPipeline(sexpr))

  // Run the compilation pipeline phases:
  // (1) closure conversion
  // (2) transform to ANF
  private[this] def compilationPipeline(sexpr: s.SExpr): t.SExpr =
    flattenToAnf(closureConvert(sexpr))

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompileModule(
      pkgId: PackageId,
      module: Module,
  ): Iterable[(t.SDefinitionRef, SDefinition)] = {
    val builder = Iterable.newBuilder[(t.SDefinitionRef, SDefinition)]
    def addDef(binding: (t.SDefinitionRef, SDefinition)) = discard(builder += binding)

    module.exceptions.foreach { case (defName, GenDefException(message)) =>
      val ref = t.ExceptionMessageDefRef(Identifier(pkgId, QualifiedName(module.name, defName)))
      builder += (ref -> SDefinition(withLabelT(ref, unsafeCompile(message))))
    }

    module.definitions.foreach {
      case (defName, DValue(_, _, body, _)) =>
        val ref = t.LfDefRef(Identifier(pkgId, QualifiedName(module.name, defName)))
        builder += (ref -> SDefinition(withLabelT(ref, unsafeCompile(body))))
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
        addDef(compileCreateByInterface(identifier, tmpl, impl.interfaceId))
        addDef(compileImplements(identifier, impl.interfaceId))
        impl.methods.values.foreach(method =>
          addDef(compileImplementsMethod(identifier, impl.interfaceId, method))
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
      addDef(compileCreateInterface(identifier))
      addDef(compileFetchInterface(identifier))
      addDef(compileInterfacePrecond(identifier, iface.param, iface.precond))
      iface.fixedChoices.values.foreach { choice =>
        addDef(compileInterfaceChoice(identifier, iface.param, choice))
        addDef(compileInterfaceGuardedChoice(identifier, iface.param, choice))
      }
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
  ): Iterable[(t.SDefinitionRef, SDefinition)] = {
    logger.trace(s"compilePackage: Compiling $pkgId...")

    val t0 = Time.Timestamp.now()

    interface.lookupPackage(pkgId) match {
      case Right(pkg)
          if !stablePackageIds.contains(pkgId) && !config.allowedLanguageVersions
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

  private[this] def compile(env: Env, expr0: Expr): s.SExpr =
    expr0 match {
      case EVar(name) =>
        env.lookupExprVar(name)
      case EVal(ref) =>
        s.SEVal(t.LfDefRef(ref))
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
        s.SEApp(
          s.SEBuiltin(SBStructCon(fieldsInputOrder)),
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
        s.SEValue.EmptyList
      case ECons(_, front, tail) =>
        // TODO(JM): Consider emitting SEValue(SList(...)) for
        // constant lists?
        val args = (front.iterator.map(compile(env, _)) ++ Seq(compile(env, tail))).toArray
        if (front.length == 1) {
          s.SEApp(s.SEBuiltin(SBCons), args)
        } else {
          s.SEApp(s.SEBuiltin(SBConsMany(front.length)), args)
        }
      case ENone(_) =>
        s.SEValue.None
      case ESome(_, body) =>
        SBSome(compile(env, body))
      case EEnumCon(tyCon, consName) =>
        val rank = handleLookup(
          NameOf.qualifiedNameOfCurrentFunc,
          interface.lookupEnumConstructor(tyCon, consName),
        )
        s.SEValue(SEnum(tyCon, consName, rank))
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
        s.SEValue(STypeRep(typ))
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
  private[this] def compileBuiltin(bf: BuiltinFunction): s.SExpr =
    bf match {
      case BCoerceContractId => s.SEAbs.identity
      // Numeric Comparisons
      case BLessNumeric => SBLessNumeric
      case BLessEqNumeric => SBLessEqNumeric
      case BGreaterNumeric => SBGreaterNumeric
      case BGreaterEqNumeric => SBGreaterEqNumeric
      case BEqualNumeric => SBEqualNumeric
      case BNumericToText => SBEToTextNumeric

      case BTextMapEmpty => s.SEValue.EmptyTextMap
      case BGenMapEmpty => s.SEValue.EmptyGenMap
      case _ =>
        s.SEBuiltin(bf match {
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
          case BEqualList => SBEqualList

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

          case BCoerceContractId | BLessNumeric | BLessEqNumeric | BGreaterNumeric |
              BGreaterEqNumeric | BEqualNumeric | BNumericToText | BTextMapEmpty | BGenMapEmpty =>
            throw CompilationError(s"unexpected $bf")

          case BAnyExceptionMessage => SBAnyExceptionMessage
        })
    }

  @inline
  private[this] def compilePrimCon(con: PrimCon): s.SExpr =
    con match {
      case PCTrue => s.SEValue.True
      case PCFalse => s.SEValue.False
      case PCUnit => s.SEValue.Unit
    }

  @inline
  private[this] def compilePrimLit(lit: PrimLit): s.SExpr =
    s.SEValue(lit match {
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
  ): s.SExpr =
    if (fields.isEmpty)
      s.SEValue(SRecord(tApp.tycon, ImmArray.Empty, noArgs))
    else
      s.SEApp(
        s.SEBuiltin(SBRecCon(tApp.tycon, fields.map(_._1))),
        fields.iterator.map(f => compile(env, f._2)).toArray,
      )

  private[this] def compileERecUpd(env: Env, erecupd: ERecUpd): s.SExpr = {
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

  private[this] def compileECase(env: Env, scrut: Expr, alts: ImmArray[CaseAlt]): s.SExpr =
    s.SECase(
      compile(env, scrut),
      mapToArray(alts) { case CaseAlt(pat, expr) =>
        pat match {
          case CPVariant(tycon, variant, binder) =>
            val rank = handleLookup(
              NameOf.qualifiedNameOfCurrentFunc,
              interface.lookupVariantConstructor(tycon, variant),
            ).rank
            s.SCaseAlt(t.SCPVariant(tycon, variant, rank), compile(env.pushExprVar(binder), expr))

          case CPEnum(tycon, constructor) =>
            val rank = handleLookup(
              NameOf.qualifiedNameOfCurrentFunc,
              interface.lookupEnumConstructor(tycon, constructor),
            )
            s.SCaseAlt(t.SCPEnum(tycon, constructor, rank), compile(env, expr))

          case CPNil =>
            s.SCaseAlt(t.SCPNil, compile(env, expr))

          case CPCons(head, tail) =>
            s.SCaseAlt(t.SCPCons, compile(env.pushExprVar(head).pushExprVar(tail), expr))

          case CPPrimCon(pc) =>
            s.SCaseAlt(t.SCPPrimCon(pc), compile(env, expr))

          case CPNone =>
            s.SCaseAlt(t.SCPNone, compile(env, expr))

          case CPSome(body) =>
            s.SCaseAlt(t.SCPSome, compile(env.pushExprVar(body), expr))

          case CPDefault =>
            s.SCaseAlt(t.SCPDefault, compile(env, expr))
        }
      },
    )

  // Compile nested lets using constant stack.
  @tailrec
  private[this] def compileELet(
      env0: Env,
      eLet0: ELet,
      bounds0: List[s.SExpr] = List.empty,
  ): s.SELet = {
    val binding = eLet0.binding
    val bounds = withOptLabelS(binding.binder, compile(env0, binding.bound)) :: bounds0
    val env1 = env0.pushExprVar(binding.binder)
    eLet0.body match {
      case eLet1: ELet =>
        compileELet(env1, eLet1, bounds)
      case body0 =>
        compile(env1, body0) match {
          case s.SELet(bounds1, body1) =>
            s.SELet(bounds.foldLeft(bounds1)((acc, b) => b :: acc), body1)
          case otherwise =>
            s.SELet(bounds.reverse, otherwise)
        }
    }
  }

  @inline
  private[this] def compileEUpdate(env: Env, update: Update): s.SExpr =
    update match {
      case UpdatePure(_, e) =>
        compilePure(env, e)
      case UpdateBlock(bindings, body) =>
        compileBlock(env, bindings, body)
      case UpdateFetch(tmplId, coidE) =>
        t.FetchDefRef(tmplId)(compile(env, coidE))
      case UpdateFetchInterface(ifaceId, coidE) =>
        t.FetchDefRef(ifaceId)(compile(env, coidE))
      case UpdateEmbedExpr(_, e) =>
        compileEmbedExpr(env, e)
      case UpdateCreate(tmplId, arg) =>
        t.CreateDefRef(tmplId)(compile(env, arg))
      case UpdateCreateInterface(iface, arg) =>
        t.CreateDefRef(iface)(compile(env, arg))
      case UpdateExercise(tmplId, chId, cidE, argE) =>
        t.ChoiceDefRef(tmplId, chId)(compile(env, cidE), compile(env, argE))
      case UpdateExerciseInterface(ifaceId, chId, cidE, argE, None) =>
        t.ChoiceDefRef(ifaceId, chId)(compile(env, cidE), compile(env, argE))
      case UpdateExerciseInterface(ifaceId, chId, cidE, argE, Some(guardE)) =>
        t.GuardedChoiceDefRef(ifaceId, chId)(
          compile(env, cidE),
          compile(env, argE),
          compile(env, guardE),
        )
      case UpdateExerciseByKey(tmplId, chId, keyE, argE) =>
        t.ChoiceByKeyDefRef(tmplId, chId)(compile(env, keyE), compile(env, argE))
      case UpdateGetTime =>
        SEGetTime
      case UpdateLookupByKey(RetrieveByKey(templateId, key)) =>
        t.LookupByKeyDefRef(templateId)(compile(env, key))
      case UpdateFetchByKey(RetrieveByKey(templateId, key)) =>
        t.FetchByKeyDefRef(templateId)(compile(env, key))

      case UpdateTryCatch(_, body, binder, handler) =>
        unaryFunction(env) { (tokenPos, env0) =>
          s.SETryCatch(
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
  private[this] def compileAbss(env: Env, expr0: Expr, arity: Int = 0): s.SExpr =
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
        withLabelS(t.AnonymousClosure, s.SEAbs(arity, compile(env, expr0)))
    }

  @tailrec
  private[this] def compileApps(env: Env, expr0: Expr, args: List[s.SExpr] = List.empty): s.SExpr =
    expr0 match {
      case EApp(fun, arg) =>
        compileApps(env, fun, compile(env, arg) :: args)
      case ETyApp(fun, arg) =>
        compileApps(env, fun, translateType(env, arg).fold(args)(_ :: args))
      case _ if args.isEmpty =>
        compile(env, expr0)
      case _ =>
        s.SEApp(compile(env, expr0), args.toArray)
    }

  private[this] def translateType(env: Env, typ: Type): Option[s.SExpr] =
    typ match {
      case TNat(n) => SENat(n)
      case TVar(name) => env.lookupTypeVar(name)
      case _ => None
    }

  private[this] def compileScenario(env: Env, scen: Scenario, optLoc: Option[Location]): s.SExpr =
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
  ): s.SExpr =
    // let party = <partyE>
    //     update = <updateE>
    // in $submit(mustFail)(party, update)
    let(env, compile(env, partyE)) { (partyLoc, env) =>
      let(env, compile(env, updateE)) { (updateLoc, env) =>
        SBSSubmit(optLoc, mustFail)(env.toSEVar(partyLoc), env.toSEVar(updateLoc))
      }
    }

  @inline
  private[this] def compileGetParty(env: Env, expr: Expr): s.SExpr =
    labeledUnaryFunction(Profile.GetPartyLabel, env) { (tokenPos, env) =>
      SBSGetParty(compile(env, expr), env.toSEVar(tokenPos))
    }

  @inline
  private[this] def compilePass(env: Env, time: Expr): s.SExpr =
    labeledUnaryFunction(Profile.PassLabel, env) { (tokenPos, env) =>
      SBSPass(compile(env, time), env.toSEVar(tokenPos))
    }

  @inline
  private[this] def compileEmbedExpr(env: Env, expr: Expr): s.SExpr =
    // EmbedExpr's get wrapped into an extra layer of abstraction
    // to delay evaluation.
    // e.g.
    // embed (error "foo") => \token -> error "foo"
    unaryFunction(env) { (tokenPos, env) =>
      app(compile(env, expr), env.toSEVar(tokenPos))
    }

  private[this] def compilePure(env: Env, body: Expr): s.SExpr =
    // pure <E>
    // =>
    // ((\x token -> x) <E>)
    let(env, compile(env, body)) { (bodyPos, env) =>
      unaryFunction(env) { (tokenPos, env) =>
        SBSPure(env.toSEVar(bodyPos), env.toSEVar(tokenPos))
      }
    }

  private[this] def compileBlock(env: Env, bindings: ImmArray[Binding], body: Expr): s.SExpr =
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

          def loop(env: Env, list: List[Binding]): s.SExpr = list match {
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
  ): s.SExpr =
    KeyWithMaintainersStruct(
      env.toSEVar(keyPos),
      app(compile(env, tmplKey.maintainers), env.toSEVar(keyPos)),
    )

  private[this] def compileKeyWithMaintainers(
      env: Env,
      maybeTmplKey: Option[TemplateKey],
  ): s.SExpr =
    maybeTmplKey match {
      case None => s.SEValue.None
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
      )(env.toSEVar(cidPos), mbKey.fold(s.SEValue.None: s.SExpr)(pos => SBSome(env.toSEVar(pos)))),
    ) { (tmplArgPos, _env) =>
      val env =
        _env.bindExprVar(tmpl.param, tmplArgPos).bindExprVar(choice.argBinder._1, choiceArgPos)
      let(
        env,
        SBUBeginExercise(
          tmplId,
          choice.name,
          choice.consuming,
          byKey = mbKey.isDefined,
          byInterface = None,
        )(
          env.toSEVar(choiceArgPos),
          env.toSEVar(cidPos),
          compile(env, choice.controllers),
          choice.choiceObservers match {
            case Some(observers) => compile(env, observers)
            case None => s.SEValue.EmptyList
          },
        ),
      ) { (_, _env) =>
        val env = _env.bindExprVar(choice.selfBinder, cidPos)
        s.SEScopeExercise(
          app(compile(env, choice.update), env.toSEVar(tokenPos))
        )
      }
    }

  // Apply choice guard (if given) and abort transaction if false.
  // Otherwise continue with exercise.
  private[this] def withChoiceGuard(
      env: Env,
      guardPos: Option[Position],
      payloadPos: Position,
      cidPos: Position,
      choiceName: ChoiceName,
      byInterface: Option[TypeConName],
  )(body: Env => s.SExpr): s.SExpr = {
    guardPos match {
      case None => body(env)
      case Some(guardPos) =>
        let(
          env,
          SBApplyChoiceGuard(choiceName, byInterface)(
            env.toSEVar(guardPos),
            env.toSEVar(payloadPos),
            env.toSEVar(cidPos),
          ),
        ) { (_, _env) => body(_env) }
    }
  }

  // TODO https://github.com/digital-asset/daml/issues/10810:
  //   Try to factorise this with compileChoiceBody above.
  private[this] def compileInterfaceChoiceBody(
      env: Env,
      ifaceId: TypeConName,
      param: ExprVarName,
      choice: TemplateChoice,
  )(
      choiceArgPos: Position,
      cidPos: Position,
      tokenPos: Position,
      guardPos: Option[Position],
  ) =
    let(env, SBUFetchInterface(ifaceId)(env.toSEVar(cidPos))) { (payloadPos, _env) =>
      val env = _env.bindExprVar(param, payloadPos).bindExprVar(choice.argBinder._1, choiceArgPos)
      withChoiceGuard(
        env = env,
        guardPos = guardPos,
        payloadPos = payloadPos,
        cidPos = cidPos,
        choiceName = choice.name,
        byInterface = Some(ifaceId),
      ) { env =>
        let(
          env,
          SBResolveSBUBeginExercise(
            choice.name,
            choice.consuming,
            byKey = false,
            ifaceId = ifaceId,
          )(
            env.toSEVar(payloadPos),
            env.toSEVar(choiceArgPos),
            env.toSEVar(cidPos),
            compile(env, choice.controllers),
            choice.choiceObservers match {
              case Some(observers) => compile(env, observers)
              case None => s.SEValue.EmptyList
            },
          ),
        ) { (_, _env) =>
          val env = _env.bindExprVar(choice.selfBinder, cidPos)
          s.SEScopeExercise(app(compile(env, choice.update), env.toSEVar(tokenPos)))
        }
      }
    }

  private[this] def compileInterfaceChoice(
      ifaceId: TypeConName,
      param: ExprVarName,
      choice: TemplateChoice,
  ): (t.SDefinitionRef, SDefinition) =
    topLevelFunction3(t.ChoiceDefRef(ifaceId, choice.name)) {
      (cidPos, choiceArgPos, tokenPos, env) =>
        compileInterfaceChoiceBody(env, ifaceId, param, choice)(
          choiceArgPos,
          cidPos,
          tokenPos,
          None,
        )
    }

  private[this] def compileInterfaceGuardedChoice(
      ifaceId: TypeConName,
      param: ExprVarName,
      choice: TemplateChoice,
  ): (t.SDefinitionRef, SDefinition) =
    topLevelFunction4(t.GuardedChoiceDefRef(ifaceId, choice.name)) {
      (cidPos, choiceArgPos, guardPos, tokenPos, env) =>
        compileInterfaceChoiceBody(env, ifaceId, param, choice)(
          choiceArgPos,
          cidPos,
          tokenPos,
          Some(guardPos),
        )
    }

  private[this] def compileChoice(
      tmplId: TypeConName,
      tmpl: Template,
      choice: TemplateChoice,
  ): (t.SDefinitionRef, SDefinition) =
    // Compiles a choice into:
    // ChoiceDefRef(SomeTemplate, SomeChoice) = \<actors> <cid> <choiceArg> <token> ->
    //   let targ = fetch(tmplId) <cid>
    //       _ = $beginExercise(tmplId, choice.name, choice.consuming, false) <choiceArg> <cid> <actors> [tmpl.signatories] [tmpl.observers] [choice.controllers] [tmpl.key]
    //       <retValue> = [update] <token>
    //       _ = $endExercise[tmplId] <retValue>
    //   in <retValue>
    topLevelFunction3(t.ChoiceDefRef(tmplId, choice.name)) {
      (cidPos, choiceArgPos, tokenPos, env) =>
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
  ): (t.SDefinitionRef, SDefinition) =
    // Compiles a choice into:
    // ChoiceByKeyDefRef(SomeTemplate, SomeChoice) = \ <actors> <key> <choiceArg> <token> ->
    //    let <keyWithM> = { key = <key> ; maintainers = [tmpl.maintainers] <key> }
    //        <cid> = $fecthKey(tmplId) <keyWithM>
    //        targ = fetch <cid>
    //       _ = $beginExercise[tmplId,  choice.name, choice.consuming, true] <choiceArg> <cid> <actors> [tmpl.signatories] [tmpl.observers] [choice.controllers] (Some <keyWithM>)
    //       <retValue> = <updateE> <token>
    //       _ = $endExercise[tmplId] <retValue>
    //   in  <retValue>
    topLevelFunction3(t.ChoiceByKeyDefRef(tmplId, choice.name)) {
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
      )(env.toSEVar(cidPos), mbKey.fold(s.SEValue.None: s.SExpr)(pos => SBSome(env.toSEVar(pos)))),
    ) { (tmplArgPos, _env) =>
      val env = _env.bindExprVar(tmpl.param, tmplArgPos)
      let(
        env,
        SBUInsertFetchNode(tmplId, byKey = mbKey.isDefined, byInterface = None)(env.toSEVar(cidPos)),
      ) { (_, env) =>
        env.toSEVar(tmplArgPos)
      }
    }

  private[this] def compileFetch(
      tmplId: Identifier,
      tmpl: Template,
  ): (t.SDefinitionRef, SDefinition) =
    // compile a template to
    // FetchDefRef(tmplId) = \ <coid> <token> ->
    //   let <tmplArg> = $fetch(tmplId) <coid>
    //       _ = $insertFetch(tmplId, false) coid [tmpl.signatories] [tmpl.observers] [tmpl.key]
    //   in <tmplArg>
    topLevelFunction2(t.FetchDefRef(tmplId)) { (cidPos, tokenPos, env) =>
      compileFetchBody(env, tmplId, tmpl)(cidPos, None, tokenPos)
    }

  // TODO https://github.com/digital-asset/daml/issues/10810:
  //  Here we fetch twice, once by interface Id once by template Id. Try to bypass the second fetch.
  private[this] def compileFetchInterface(
      ifaceId: Identifier
  ): (t.SDefinitionRef, SDefinition) =
    topLevelFunction2(t.FetchDefRef(ifaceId)) { (cidPos, _, env) =>
      let(env, SBUFetchInterface(ifaceId)(env.toSEVar(cidPos))) { (payloadPos, env) =>
        let(
          env,
          SBResolveSBUInsertFetchNode(ifaceId)(env.toSEVar(payloadPos), env.toSEVar(cidPos)),
        ) { (_, env) =>
          env.toSEVar(payloadPos)
        }
      }
    }

  private[this] def compileInterfacePrecond(
      iface: Identifier,
      param: ExprVarName,
      expr: Expr,
  ) =
    topLevelFunction1(t.InterfacePrecondDefRef(iface))((argPos, env) =>
      compile(env.bindExprVar(param, argPos), expr)
    )

  private[this] def compileKey(
      tmplId: Identifier,
      tmpl: Template,
  ): (t.SDefinitionRef, SDefinition) =
    topLevelFunction1(t.KeyDefRef(tmplId)) { (tmplArgPos, env) =>
      compileKeyWithMaintainers(env.bindExprVar(tmpl.param, tmplArgPos), tmpl.key)
    }

  private[this] def compileSignatories(
      tmplId: Identifier,
      tmpl: Template,
  ): (t.SDefinitionRef, SDefinition) =
    topLevelFunction1(t.SignatoriesDefRef(tmplId)) { (tmplArgPos, env) =>
      compile(env.bindExprVar(tmpl.param, tmplArgPos), tmpl.signatories)
    }

  private[this] def compileObservers(
      tmplId: Identifier,
      tmpl: Template,
  ): (t.SDefinitionRef, SDefinition) =
    topLevelFunction1(t.ObserversDefRef(tmplId)) { (tmplArgPos, env) =>
      compile(env.bindExprVar(tmpl.param, tmplArgPos), tmpl.observers)
    }

  // Turn a template value into an interface value. Since interfaces have a
  // toll-free representation (for now), this is just the identity function.
  // But the existence of ImplementsDefRef implies that the template implements
  // the interface, which is useful in itself.
  private[this] def compileImplements(
      tmplId: Identifier,
      ifaceId: Identifier,
  ): (t.SDefinitionRef, SDefinition) =
    t.ImplementsDefRef(tmplId, ifaceId) ->
      SDefinition(unsafeClosureConvert(s.SEAbs.identity))

  // Compile the implementation of an interface method.
  private[this] def compileImplementsMethod(
      tmplId: Identifier,
      ifaceId: Identifier,
      method: TemplateImplementsMethod,
  ): (t.SDefinitionRef, SDefinition) = {
    val ref = t.ImplementsMethodDefRef(tmplId, ifaceId, method.name)
    ref -> SDefinition(withLabelT(ref, unsafeCompile(method.value)))
  }

  private[this] def compileCreateBody(
      tmplId: Identifier,
      tmpl: Template,
      byInterface: Option[Identifier],
      tmplArgPos: Position,
      env: Env,
  ) = {
    val env2 = env.bindExprVar(tmpl.param, tmplArgPos)
    val implementsPrecondsIterator = tmpl.implements.iterator.map[s.SExpr](impl =>
      // This relies on interfaces having the same representation as the underlying template
      t.InterfacePrecondDefRef(impl._1)(env2.toSEVar(tmplArgPos))
    )
    // TODO Clean this up as part of changing how we evaluate these
    // https://github.com/digital-asset/daml/issues/11762
    val precondsArray: ImmArray[s.SExpr] =
      (Iterator(compile(env2, tmpl.precond)) ++ implementsPrecondsIterator ++ Iterator(
        s.SEValue.EmptyList
      )).to(ImmArray)
    val preconds = s.SEApp(s.SEBuiltin(SBConsMany(precondsArray.length - 1)), precondsArray.toArray)
    // We check precondition in a separated builtin to prevent
    // further evaluation of agreement, signatories, observers and key
    // in case of failed precondition.
    let(env2, SBCheckPrecond(tmplId)(env2.toSEVar(tmplArgPos), preconds)) { (_, env) =>
      SBUCreate(tmplId, byInterface)(
        env.toSEVar(tmplArgPos),
        compile(env, tmpl.agreementText),
        compile(env, tmpl.signatories),
        compile(env, tmpl.observers),
        compileKeyWithMaintainers(env, tmpl.key),
      )
    }
  }

  private[this] def compileCreate(
      tmplId: Identifier,
      tmpl: Template,
  ): (t.SDefinitionRef, SDefinition) = {
    // Translates 'create Foo with <params>' into:
    // CreateDefRef(tmplId) = \ <tmplArg> <token> ->
    //   let _ = $checkPrecond(tmplId)(<tmplArg> [tmpl.precond ++ [precond | precond <- tmpl.implements]]
    //   in $create <tmplArg> [tmpl.agreementText] [tmpl.signatories] [tmpl.observers] [tmpl.key]
    topLevelFunction2(t.CreateDefRef(tmplId))((tmplArgPos, _, env) =>
      compileCreateBody(tmplId, tmpl, None, tmplArgPos, env)
    )
  }

  private[this] def compileCreateByInterface(
      tmplId: Identifier,
      tmpl: Template,
      ifaceId: Identifier,
  ): (t.SDefinitionRef, SDefinition) = {
    // Similar to compileCreate, but sets the 'byInterface' field in the transaction.
    topLevelFunction2(t.CreateByInterfaceDefRef(tmplId, ifaceId))((tmplArgPos, _, env) =>
      compileCreateBody(tmplId, tmpl, Some(ifaceId), tmplArgPos, env)
    )
  }

  private[this] def compileCreateInterface(
      ifaceId: Identifier
  ): (t.SDefinitionRef, SDefinition) = {
    topLevelFunction2(t.CreateDefRef(ifaceId)) { (tmplArgPos, tokenPos, env) =>
      SBResolveCreateByInterface(ifaceId)(
        env.toSEVar(tmplArgPos),
        env.toSEVar(tmplArgPos),
        env.toSEVar(tokenPos),
      )
    }
  }

  private[this] def compileCreateAndExercise(
      tmplId: Identifier,
      createArg: SValue,
      choiceId: ChoiceName,
      choiceArg: SValue,
  ): s.SExpr =
    labeledUnaryFunction(Profile.CreateAndExerciseLabel(tmplId, choiceId), Env.Empty) {
      (tokenPos, env) =>
        let(env, t.CreateDefRef(tmplId)(s.SEValue(createArg), env.toSEVar(tokenPos))) {
          (cidPos, env) =>
            t.ChoiceDefRef(tmplId, choiceId)(
              env.toSEVar(cidPos),
              s.SEValue(choiceArg),
              env.toSEVar(tokenPos),
            )
        }
    }

  private[this] def compileLookupByKey(
      tmplId: Identifier,
      tmplKey: TemplateKey,
  ): (t.SDefinitionRef, SDefinition) =
    // compile a template with key into
    // LookupByKeyDefRef(tmplId) = \ <key> <token> ->
    //    let <keyWithM> = { key = <key> ; maintainers = [tmplKey.maintainers] <key> }
    //        <mbCid> = $lookupKey(tmplId) <keyWithM>
    //        _ = $insertLookup(tmplId> <keyWithM> <mbCid>
    //    in <mbCid>
    topLevelFunction2(t.LookupByKeyDefRef(tmplId)) { (keyPos, _, env) =>
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
  ): (t.SDefinitionRef, SDefinition) =
    // compile a template with key into
    // FetchByKeyDefRef(tmplId) = \ <key> <token> ->
    //    let <keyWithM> = { key = <key> ; maintainers = [tmpl.maintainers] <key> }
    //        <coid> = $fetchKey(tmplId) <keyWithM>
    //        <contract> = $fetch(tmplId) <coid>
    //        _ = $insertFetch <coid> <signatories> <observers> (Some <keyWithM> )
    //    in { contractId: ContractId Foo, contract: Foo }
    topLevelFunction2(t.FetchByKeyDefRef(tmplId)) { (keyPos, tokenPos, env) =>
      let(env, encodeKeyWithMaintainers(env, keyPos, tmplKey)) { (keyWithMPos, env) =>
        let(env, SBUFetchKey(tmplId)(env.toSEVar(keyWithMPos))) { (cidPos, env) =>
          let(env, compileFetchBody(env, tmplId, tmpl)(cidPos, Some(keyWithMPos), tokenPos)) {
            (contractPos, env) =>
              FetchByKeyResult(env.toSEVar(cidPos), env.toSEVar(contractPos))
          }
        }
      }
    }

  private[this] def compileCommand(cmd: Command): s.SExpr = cmd match {
    case Command.Create(templateId, argument) =>
      t.CreateDefRef(templateId)(s.SEValue(argument))
    case Command.CreateByInterface(interfaceId, templateId, argument) =>
      t.CreateByInterfaceDefRef(templateId, interfaceId)(s.SEValue(argument))
    case Command.Exercise(templateId, contractId, choiceId, argument) =>
      t.ChoiceDefRef(templateId, choiceId)(s.SEValue(contractId), s.SEValue(argument))
    case Command.ExerciseByInterface(interfaceId, templateId, contractId, choiceId, argument) =>
      t.GuardedChoiceDefRef(interfaceId, choiceId)(
        s.SEValue(contractId),
        s.SEValue(argument),
        s.SEBuiltin(SBGuardTemplateId(templateId)),
      )
    case Command.ExerciseInterface(interfaceId, contractId, choiceId, argument) =>
      t.ChoiceDefRef(interfaceId, choiceId)(s.SEValue(contractId), s.SEValue(argument))
    case Command.ExerciseByKey(templateId, contractKey, choiceId, argument) =>
      t.ChoiceByKeyDefRef(templateId, choiceId)(s.SEValue(contractKey), s.SEValue(argument))
    case Command.Fetch(templateId, coid) =>
      t.FetchDefRef(templateId)(s.SEValue(coid))
    case Command.FetchByInterface(interfaceId, templateId @ _, coid) =>
      // TODO https://github.com/digital-asset/daml/issues/11703
      //   Ensure that fetched template has expected templateId.
      t.FetchDefRef(interfaceId)(s.SEValue(coid))
    case Command.FetchByKey(templateId, key) =>
      t.FetchByKeyDefRef(templateId)(s.SEValue(key))
    case Command.CreateAndExercise(templateId, createArg, choice, choiceArg) =>
      compileCreateAndExercise(
        templateId,
        createArg,
        choice,
        choiceArg,
      )
    case Command.LookupByKey(templateId, contractKey) =>
      t.LookupByKeyDefRef(templateId)(s.SEValue(contractKey))
  }

  private val SEUpdatePureUnit = unaryFunction(Env.Empty)((_, _) => s.SEValue.Unit)

  private[this] val handleEverything: s.SExpr = SBSome(SEUpdatePureUnit)

  private[this] def catchEverything(e: s.SExpr): s.SExpr =
    unaryFunction(Env.Empty) { (tokenPos, env0) =>
      s.SETryCatch(
        app(e, env0.toSEVar(tokenPos)), {
          val binderPos = env0.nextPosition
          val env1 = env0.pushVar
          SBTryHandler(handleEverything, env1.toSEVar(binderPos), env1.toSEVar(tokenPos))
        },
      )
    }

  private[this] def compileCommandForReinterpretation(cmd: Command): s.SExpr =
    catchEverything(compileCommand(cmd))

  private[this] def compileCommands(bindings: ImmArray[Command]): s.SExpr =
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
              s.SELet(exprs, s.SEValue.Unit)
            }
          }
        }
    }
}
