// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, Numeric, Struct, Time}
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageVersion
import com.daml.lf.speedy.SBuiltin._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.Anf.flattenToAnf
import com.daml.lf.transaction.VersionTimeline
import com.daml.lf.validation.{EUnknownDefinition, LEPackage, Validation, ValidationError}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

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
  case class PackageNotFound(pkgId: PackageId)
      extends RuntimeException(s"Package not found $pkgId", null, true, false)

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
      allowedLanguageVersions = VersionTimeline.stableLanguageVersions,
      packageValidation = FullPackageValidation,
      profiling = NoProfile,
      stacktracing = NoStackTrace,
    )
    val Dev = Config(
      allowedLanguageVersions = VersionTimeline.devLanguageVersions,
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

  private val SEDropSecondArgument = SEAbs(2, SEVar(2))
  private val SEUpdatePureUnit = SEAbs(1, SEValue.Unit)
  private val SEAppBoundHead = SEApp(SEVar(2), Array(SEVar(1)))

  private val SENat: Numeric.Scale => Some[SEValue] =
    Numeric.Scale.values.map(n => Some(SEValue(STNat(n))))

  /** Validates and Compiles all the definitions in the packages provided. Returns them in a Map.
    *
    * The packages do not need to be in any specific order, as long as they and all the packages
    * they transitively reference are in the [[packages]] in the compiler.
    */
  def compilePackages(
      packages: Map[PackageId, Package],
      compilerConfig: Compiler.Config,
  ): Either[String, Map[SDefinitionRef, SExpr]] = {
    val compiler = new Compiler(packages, compilerConfig)
    try {
      Right(
        packages.keys.foldLeft(Map.empty[SDefinitionRef, SExpr])(
          _ ++ compiler.unsafeCompilePackage(_))
      )
    } catch {
      case CompilationError(msg) => Left(s"Compilation Error: $msg")
      case PackageNotFound(pkgId) => Left(s"Package not found $pkgId")
      case e: ValidationError => Left(e.pretty)
    }
  }

}

private[lf] final class Compiler(
    packages: PackageId PartialFunction Package,
    config: Compiler.Config
) {

  import Compiler._

  // Stack-trace support is disabled by avoiding the construction of SELocation nodes.
  private[this] def maybeSELocation(loc: Location, sexp: SExpr): SExpr = {
    config.stacktracing match {
      case NoStackTrace => sexp
      case FullStackTrace => SELocation(loc, sexp)
    }
  }

  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  private[this] abstract class VarRef { def name: Name }
  // corresponds to DAML-LF expression variable.
  private[this] case class EVarRef(name: ExprVarName) extends VarRef
  // corresponds to DAML-LF type variable.
  private[this] case class TVarRef(name: TypeVarName) extends VarRef

  private[this] case class Env(
      position: Int = 0,
      varIndices: List[(VarRef, Option[Int])] = List.empty,
  ) {
    def incrPos: Env = copy(position = position + 1)
    def addExprVar(name: Option[ExprVarName], index: Int): Env =
      name.fold(this)(n => copy(varIndices = (EVarRef(n), Some(index)) :: varIndices))
    def addExprVar(name: ExprVarName, index: Int): Env =
      addExprVar(Some(name), index)
    def addExprVar(name: Option[ExprVarName]): Env =
      incrPos.addExprVar(name, position)
    def addExprVar(name: ExprVarName): Env =
      addExprVar(Some(name))
    def addTypeVar(name: TypeVarName): Env =
      incrPos.copy(varIndices = (TVarRef(name), Some(position)) :: varIndices)
    def hideTypeVar(name: TypeVarName): Env =
      copy(varIndices = (TVarRef(name), None) :: varIndices)

    def vars: List[VarRef] = varIndices.map(_._1)

    private[this] def lookUpVar(varRef: VarRef): Option[Int] =
      varIndices
        .find(_._1 == varRef)
        .flatMap(_._2)
        // The de Bruijin index for the binder, e.g.
        // the distance to the binder. The closest binder
        // is at distance 1.
        .map(position - _)

    def lookUpExprVar(name: ExprVarName): Int =
      lookUpVar(EVarRef(name))
        .getOrElse(
          throw CompilationError(s"Unknown variable: $name. Known: ${env.vars.mkString(",")}"))

    def lookUpTypeVar(name: TypeVarName): Option[Int] =
      lookUpVar(TVarRef(name))

  }

  /** Environment mapping names into stack positions */
  private[this] var env = Env()

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

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompile(cmds: ImmArray[Command]): SExpr =
    validate(compilationPipeline(compileCommands(cmds)))

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompile(expr: Expr): SExpr =
    validate(compilationPipeline(compile(expr)))

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeClosureConvert(sexpr: SExpr): SExpr =
    validate(compilationPipeline(sexpr))

  // Run the compilation pipeline phases:
  // (1) closure conversion
  // (2) transform to ANF
  private[this] def compilationPipeline(sexpr: SExpr): SExpr = {
    val doANF = true
    if (doANF) {
      flattenToAnf(closureConvert(Map.empty, sexpr))
    } else {
      closureConvert(Map.empty, sexpr)
    }
  }

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompileDefn(
      identifier: Identifier,
      defn: Definition,
  ): List[(SDefinitionRef, SExpr)] =
    defn match {
      case DValue(_, _, body, _) =>
        val ref = LfDefRef(identifier)
        List(ref -> withLabel(ref, unsafeCompile(body)))

      case DDataType(_, _, DataRecord(_, Some(tmpl))) =>
        val builder = List.newBuilder[(SDefinitionRef, SExpr)]

        builder += compileCreate(identifier, tmpl)
        builder += compileFetch(identifier, tmpl)

        tmpl.choices.toList.foreach {
          case (cname, choice) =>
            builder += compileChoice(identifier, tmpl, cname, choice)
        }

        tmpl.key.foreach { tmplKey =>
          builder += compileFetchByKey(identifier, tmpl, tmplKey)
          builder += compileLookupByKey(identifier, tmplKey)
          tmpl.choices.foreach {
            case (cname, choice) =>
              builder += compileChoiceByKey(identifier, tmpl, tmplKey, cname, choice)
          }
        }

        builder.result()
      case _ =>
        List()
    }

  /** Validates and compiles all the definitions in the package provided.
    *
    * Fails with [[PackageNotFound]] if the package or any of the packages it refers
    * to are not in the [[packages]].
    *
    * @throws ValidationError if the package does not pass validations.
    */
  @throws[PackageNotFound]
  @throws[CompilationError]
  @throws[ValidationError]
  def unsafeCompilePackage(
      pkgId: PackageId,
  ): Iterable[(SDefinitionRef, SExpr)] = {
    logger.trace(s"compilePackage: Compiling $pkgId...")

    val t0 = Time.Timestamp.now()

    packages.lift(pkgId) match {
      case Some(pkg) if !config.allowedLanguageVersions.contains(pkg.languageVersion) =>
        throw CompilationError(
          s"Disallowed language version in package $pkgId: " +
            s"Expected version between ${config.allowedLanguageVersions.min.pretty} and ${config.allowedLanguageVersions.max.pretty} but got ${pkg.languageVersion.pretty}"
        )
      case _ =>
    }

    config.packageValidation match {
      case Compiler.NoPackageValidation =>
      case Compiler.FullPackageValidation =>
        Validation.checkPackage(packages, pkgId).left.foreach {
          case EUnknownDefinition(_, LEPackage(pkgId_)) =>
            logger.trace(s"compilePackage: Missing $pkgId_, requesting it...")
            throw PackageNotFound(pkgId_)
          case e =>
            throw e
        }
    }

    val t1 = Time.Timestamp.now()

    val defns = for {
      module <- lookupPackage(pkgId).modules.values
      defnWithId <- module.definitions
      (defnId, defn) = defnWithId
      fullId = Identifier(pkgId, QualifiedName(module.name, defnId))
      exprWithId <- unsafeCompileDefn(fullId, defn)
    } yield exprWithId
    val t2 = Time.Timestamp.now()

    logger.trace(
      s"compilePackage: $pkgId ready, typecheck=${(t1.micros - t0.micros) / 1000}ms, compile=${(t2.micros - t1.micros) / 1000}ms",
    )

    defns
  }

  private[this] def patternNArgs(pat: SCasePat): Int = pat match {
    case _: SCPEnum | _: SCPPrimCon | SCPNil | SCPDefault | SCPNone => 0
    case _: SCPVariant | SCPSome => 1
    case SCPCons => 2
  }

  private[this] def compile(expr0: Expr): SExpr =
    expr0 match {
      case EVar(name) =>
        SEVar(env.lookUpExprVar(name))
      case EVal(ref) =>
        SEVal(LfDefRef(ref))
      case EBuiltin(bf) =>
        compileBuiltin(bf)
      case EPrimCon(con) =>
        compilePrimCon(con)
      case EPrimLit(lit) =>
        compilePrimLit(lit)
      case EAbs(_, _, _) | ETyAbs(_, _) =>
        withEnv { _ =>
          compileAbss(expr0)
        }
      case EApp(_, _) | ETyApp(_, _) =>
        compileApps(expr0)
      case ERecCon(tApp, fields) =>
        compileERecCon(tApp, fields)
      case ERecProj(tapp, field, record) =>
        SBRecProj(tapp.tycon, lookupRecordIndex(tapp, field))(
          compile(record),
        )
      case erecupd: ERecUpd =>
        compileERecUpd(erecupd)
      case EStructCon(fields) =>
        val fieldsInputOrder =
          Struct.assertFromSeq(fields.iterator.map(_._1).zipWithIndex.toSeq)
        SEApp(
          SEBuiltin(SBStructCon(fieldsInputOrder)),
          fields.iterator.map { case (_, e) => compile(e) }.toArray
        )
      case EStructProj(field, struct) =>
        SBStructProj(field)(compile(struct))
      case EStructUpd(field, struct, update) =>
        SBStructUpd(field)(compile(struct), compile(update))
      case ECase(scrut, alts) =>
        compileECase(scrut, alts)
      case ENil(_) =>
        SEValue.EmptyList
      case ECons(_, front, tail) =>
        // TODO(JM): Consider emitting SEValue(SList(...)) for
        // constant lists?
        SEApp(
          SEBuiltin(SBConsMany(front.length)),
          front.iterator.map(compile).toArray :+ compile(tail),
        )
      case ENone(_) =>
        SEValue.None
      case ESome(_, body) =>
        SEApp(
          SEBuiltin(SBSome),
          Array(compile(body)),
        )
      case EEnumCon(tyCon, constructor) =>
        val enumDef =
          lookupEnumDefinition(tyCon).getOrElse(throw CompilationError(s"enum $tyCon not found"))
        SEValue(SEnum(tyCon, constructor, enumDef.constructorRank(constructor)))
      case EVariantCon(tapp, variant, arg) =>
        val variantDef = lookupVariantDefinition(tapp.tycon)
          .getOrElse(throw CompilationError(s"variant ${tapp.tycon} not found"))
        SBVariantCon(tapp.tycon, variant, variantDef.constructorRank(variant))(compile(arg))
      case let: ELet =>
        compileELet(let)
      case EUpdate(upd) =>
        compileEUpdate(upd)
      case ELocation(loc, EScenario(scen)) =>
        maybeSELocation(loc, compileScenario(scen, Some(loc)))
      case EScenario(scen) =>
        compileScenario(scen, None)
      case ELocation(loc, e) =>
        maybeSELocation(loc, compile(e))
      case EToAny(ty, e) =>
        SEApp(SEBuiltin(SBToAny(ty)), Array(compile(e)))
      case EFromAny(ty, e) =>
        SEApp(SEBuiltin(SBFromAny(ty)), Array(compile(e)))
      case ETypeRep(typ) =>
        SEValue(STypeRep(typ))
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

          case BToTextInt64 => SBToText
          case BToTextNumeric => SBToTextNumeric
          case BToTextText => SBToText
          case BToTextTimestamp => SBToText
          case BToTextParty => SBToText
          case BToTextDate => SBToText
          case BToTextContractId => SBToTextContractId
          case BToQuotedTextParty => SBToQuotedTextParty
          case BToTextCodePoints => SBToTextCodePoints
          case BFromTextParty => SBFromTextParty
          case BFromTextInt64 => SBFromTextInt64
          case BFromTextNumeric => SBFromTextNumeric
          case BFromTextCodePoints => SBFromTextCodePoints

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

          case BTextMapInsert => SBGenMapInsert
          case BTextMapLookup => SBGenMapLookup
          case BTextMapDelete => SBGenMapDelete
          case BTextMapToList => SBGenMapToList
          case BTextMapSize => SBGenMapSize

          // GenMap

          case BGenMapInsert => SBGenMapInsert
          case BGenMapLookup => SBGenMapLookup
          case BGenMapDelete => SBGenMapDelete
          case BGenMapKeys => SBGenMapKeys
          case BGenMapValues => SBGenMapValues
          case BGenMapSize => SBGenMapSize

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
          case BFoldl | BFoldr | BCoerceContractId | BEqual | BEqualList | BLessEq | BLess |
              BGreaterEq | BGreater | BLessNumeric | BLessEqNumeric | BGreaterNumeric |
              BGreaterEqNumeric | BEqualNumeric | BTextMapEmpty | BGenMapEmpty =>
            throw CompilationError(s"unexpected $bf")
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

  @inline
  private[this] def compileERecCon(tApp: TypeConApp, fields: ImmArray[(FieldName, Expr)]): SExpr =
    if (fields.isEmpty)
      SEBuiltin(SBRecCon(tApp.tycon, ImmArray.empty))
    else {
      SEApp(
        SEBuiltin(SBRecCon(tApp.tycon, fields.map(_._1))),
        fields.iterator.map(f => compile(f._2)).toArray,
      )
    }

  @inline
  private[this] def compileERecUpd(erecupd: ERecUpd): SExpr = {
    val tapp = erecupd.tycon
    val (record, fields, updates) = collectRecUpds(erecupd)
    if (fields.length == 1) {
      SBRecUpd(tapp.tycon, lookupRecordIndex(tapp, fields.head))(
        compile(record),
        compile(updates.head),
      )
    } else {
      SBRecUpdMulti(tapp.tycon, fields.map(lookupRecordIndex(tapp, _)).toArray)(
        (record :: updates).map(compile): _*,
      )
    }
  }

  @inline
  private[this] def compileECase(scrut: Expr, alts: ImmArray[CaseAlt]): SExpr =
    SECase(
      compile(scrut),
      alts.iterator.map {
        case CaseAlt(pat, expr) =>
          pat match {
            case CPVariant(tycon, variant, binder) =>
              val variantDef = lookupVariantDefinition(tycon).getOrElse(
                throw CompilationError(s"variant $tycon not found"))
              withBinders(binder) { _ =>
                SCaseAlt(
                  SCPVariant(tycon, variant, variantDef.constructorRank(variant)),
                  compile(expr),
                )
              }

            case CPEnum(tycon, constructor) =>
              val enumDef = lookupEnumDefinition(tycon).getOrElse(
                throw CompilationError(s"enum $tycon not found"))
              SCaseAlt(
                SCPEnum(tycon, constructor, enumDef.constructorRank(constructor)),
                compile(expr),
              )

            case CPNil =>
              SCaseAlt(SCPNil, compile(expr))

            case CPCons(head, tail) =>
              withBinders(head, tail) { _ =>
                SCaseAlt(SCPCons, compile(expr))
              }

            case CPPrimCon(pc) =>
              SCaseAlt(SCPPrimCon(pc), compile(expr))

            case CPNone =>
              SCaseAlt(SCPNone, compile(expr))

            case CPSome(body) =>
              withBinders(body) { _ =>
                SCaseAlt(SCPSome, compile(expr))
              }

            case CPDefault =>
              SCaseAlt(SCPDefault, compile(expr))
          }
      }.toArray,
    )

  @inline
  // ELet(a, ELet(b, body)) => ([a, b], body)
  private[this] def collectLets(expr: Expr): (List[Binding], Expr) =
    expr match {
      case ELet(binding, body) =>
        val (bindings, body2) = collectLets(body)
        (binding :: bindings, body2)
      case e => (List.empty, e)
    }

  @inline
  private[this] def compileELet(let: ELet) = {
    val (bindings, body) = collectLets(let)
    withEnv { _ =>
      SELet(
        bindings.map {
          case Binding(optBinder, _, bound) =>
            val bound2 = withOptLabel(optBinder, compile(bound))
            env = env.addExprVar(optBinder)
            bound2
        }.toArray,
        compile(body),
      )
    }
  }

  @inline
  private[this] def compileEUpdate(update: Update): SExpr =
    update match {
      case UpdatePure(_, e) =>
        compilePure(e)
      case UpdateBlock(bindings, body) =>
        compileBlock(bindings, body)
      case UpdateFetch(tmplId, coidE) =>
        SEApp(SEVal(FetchDefRef(tmplId)), Array(compile(coidE)))
      case UpdateEmbedExpr(_, e) =>
        compileEmbedExpr(e)
      case UpdateCreate(tmplId, arg) =>
        SEApp(SEVal(CreateDefRef(tmplId)), Array(compile(arg)))
      case UpdateExercise(tmplId, chId, cidE, actorsE, argE) =>
        compileExercise(
          tmplId = tmplId,
          contractId = compile(cidE),
          choiceId = chId,
          optActors = actorsE.map(compile),
          argument = compile(argE),
        )
      case UpdateGetTime =>
        SEGetTime
      case UpdateLookupByKey(RetrieveByKey(templateId, key)) =>
        SEApp(SEVal(LookupByKeyDefRef(templateId)), Array(compile(key)))
      case UpdateFetchByKey(RetrieveByKey(templateId, key)) =>
        SEApp(SEVal(FetchByKeyDefRef(templateId)), Array(compile(key)))
    }

  @tailrec
  private[this] def compileAbss(expr0: Expr, arity: Int = 0): SExpr =
    expr0 match {
      case EAbs((binder, typ @ _), body, ref @ _) =>
        env = env.addExprVar(binder)
        compileAbss(body, arity + 1)
      case ETyAbs((binder, KNat), body) =>
        env = env.addTypeVar(binder)
        compileAbss(body, arity + 1)
      case ETyAbs((binder, _), body) =>
        env = env.hideTypeVar(binder)
        compileAbss(body, arity)
      case _ if arity == 0 =>
        compile(expr0)
      case _ =>
        withLabel(AnonymousClosure, SEAbs(arity, compile(expr0)))
    }

  @tailrec
  private[this] def compileApps(expr0: Expr, args: List[SExpr] = List.empty): SExpr =
    expr0 match {
      case EApp(fun, arg) =>
        compileApps(fun, compile(arg) :: args)
      case ETyApp(fun, arg) =>
        compileApps(fun, translateType(arg).fold(args)(_ :: args))
      case _ if args.isEmpty =>
        compile(expr0)
      case _ =>
        SEApp(compile(expr0), args.toArray)
    }

  private[this] def translateType(typ: Type): Option[SExpr] =
    typ match {
      case TNat(n) => SENat(n)
      case TVar(name) => env.lookUpTypeVar(name).map(SEVar)
      case _ => None
    }

  private[this] def compileScenario(scen: Scenario, optLoc: Option[Location]): SExpr =
    scen match {
      case ScenarioPure(_, e) =>
        compilePure(e)
      case ScenarioBlock(bindings, body) =>
        compileBlock(bindings, body)
      case ScenarioCommit(partyE, updateE, _retType @ _) =>
        compileCommit(partyE, updateE, optLoc)
      case ScenarioMustFailAt(partyE, updateE, _retType @ _) =>
        compileMustFail(partyE, updateE, optLoc)
      case ScenarioGetTime =>
        SEGetTime
      case ScenarioGetParty(e) =>
        compileGetParty(e)
      case ScenarioPass(relTime) =>
        compilePass(relTime)
      case ScenarioEmbedExpr(_, e) =>
        compileEmbedExpr(e)
    }
  @inline
  private[this] def compileCommit(partyE: Expr, updateE: Expr, optLoc: Option[Location]): SExpr =
    // let party = <partyE>
    //     update = <updateE>
    // in \token ->
    //   let _ = $beginCommit party token
    //       r = update token
    //   in $endCommit[mustFail = false] r token
    withEnv { _ =>
      val party = compile(partyE)
      env = env.incrPos // party
      val update = compile(updateE)
      env = env.incrPos // update
      env = env.incrPos // $beginCommit
      SELet(party, update) in
        withLabel(
          "submit",
          SEAbs(1) {
            SELet(
              // stack: <party> <update> <token>
              SBSBeginCommit(optLoc)(SEVar(3), SEVar(1)),
              // stack: <party> <update> <token> ()
              SEApp(SEVar(3), Array(SEVar(2))),
              // stack: <party> <update> <token> () result
            ) in
              SBSEndCommit(false)(SEVar(1), SEVar(3))
          }
        )
    }

  @inline
  private[this] def compileMustFail(partyE: Expr, updateE: Expr, optLoc: Option[Location]): SExpr =
    // \token ->
    //   let _ = $beginCommit <party> token
    //       r = $catch (<updateE> token) true false
    //   in $endCommit[mustFail = true] r token
    withEnv { _ =>
      env = env.incrPos // token
      val party = compile(partyE)
      env = env.incrPos // $beginCommit
      val update = compile(updateE)
      withLabel(
        "submitMustFail",
        SEAbs(1) {
          SELet(
            SBSBeginCommit(optLoc)(party, SEVar(1)),
            SECatch(SEApp(update, Array(SEVar(2))), SEValue.True, SEValue.False),
          ) in SBSEndCommit(true)(SEVar(1), SEVar(3))
        }
      )
    }

  @inline
  private[this] def compileGetParty(expr: Expr): SExpr =
    withEnv { _ =>
      env = env.incrPos // token
      withLabel(
        "getParty",
        SEAbs(1) {
          SBSGetParty(compile(expr), SEVar(1))
        }
      )
    }

  @inline
  private[this] def compilePass(time: Expr): SExpr =
    withEnv { _ =>
      env = env.incrPos // token
      withLabel(
        "pass",
        SEAbs(1) {
          SBSPass(compile(time), SEVar(1))
        }
      )
    }

  @inline
  private[this] def compileEmbedExpr(expr: Expr): SExpr =
    withEnv { _ =>
      env = env.incrPos // token
      // EmbedExpr's get wrapped into an extra layer of abstraction
      // to delay evaluation.
      // e.g.
      // embed (error "foo") => \token -> error "foo"
      SEAbs(1) {
        SEApp(compile(expr), Array(SEVar(1)))
      }
    }

  private[this] def compilePure(body: Expr): SExpr =
    // pure <E>
    // =>
    // ((\x token -> x) <E>)
    SEApp(SEDropSecondArgument, Array(compile(body)))

  private[this] def compileBlock(bindings: ImmArray[Binding], body: Expr): SExpr =
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
    withEnv { _ =>
      val boundHead = compile(bindings.head.bound)
      env = env.incrPos // evaluated body of first binding

      val tokenPosition = env.position
      env = env.incrPos // token

      // add the first binding into the environment
      val appBoundHead = SEApp(SEVar(2), Array(SEVar(1)))
      env = env.addExprVar(bindings.head.binder)

      // and then the rest
      val boundTail = bindings.tail.toList.map {
        case Binding(optB, _, bound) =>
          val sbound = compile(bound)
          val tokenIndex = env.position - tokenPosition
          env = env.addExprVar(optB)
          SEApp(sbound, Array(SEVar(tokenIndex)))
      }
      val allBounds = appBoundHead +: boundTail
      SELet(boundHead) in
        SEAbs(1) {
          SELet(allBounds: _*) in
            SEApp(compile(body), Array(SEVar(env.position - tokenPosition)))
        }
    }

  private[this] val keyWithMaintainersStruct: Struct[Int] =
    Struct.assertFromSeq(List(keyFieldName, maintainersFieldName).zipWithIndex)

  private def encodeKeyWithMaintainers(key: SExpr, tmplKey: TemplateKey): SExpr =
    SELet(key) in
      SBStructCon(keyWithMaintainersStruct)(
        SEVar(1), // key
        SEApp(compile(tmplKey.maintainers), Array(SEVar(1) /* key */ )),
      )

  private[this] def translateKeyWithMaintainers(tmplKey: TemplateKey): SExpr =
    encodeKeyWithMaintainers(compile(tmplKey.body), tmplKey)

  private[this] def compileChoice(
      tmplId: TypeConName,
      tmpl: Template,
      cname: ChoiceName,
      choice: TemplateChoice,
  ): (ChoiceDefRef, SExpr) =
    // Compiles a choice into:
    // SomeTemplate$SomeChoice = \<actors> <cid> <choiceArg> <token> ->
    //   let targ = fetch <cid>
    //       _ = $beginExercise[tmplId, chId] <choiceArg> <cid> <actors> <byKey flag> sigs obs ctrls mbKey <token>
    //       result = <updateE>
    //       _ = $endExercise[tmplId]
    //   in result
    ChoiceDefRef(tmplId, cname) ->
      validate(
        closureConvert(
          Map.empty,
          withEnv { _ =>
            env = env.incrPos /* <actors> */
            val selfBinderPos = env.position
            env = env.incrPos /* <cid> */
            val choiceArgumentPos = env.position
            env = env.incrPos // <choice argument>
            env = env.incrPos /* <token> */
            // <template argument>
            env = env.addExprVar(tmpl.param)
            val signatories = compile(tmpl.signatories)
            val observers = compile(tmpl.observers)
            // now allow access to the choice argument
            env = env.addExprVar(choice.argBinder._1, choiceArgumentPos)
            val controllers = compile(choice.controllers)
            val mbKey: SExpr = tmpl.key match {
              case None => SEValue.None
              case Some(k) => SEApp(SEBuiltin(SBSome), Array(translateKeyWithMaintainers(k)))
            }
            env = env.incrPos // beginExercise's ()
            // allow access to the self contract id
            env = env.addExprVar(choice.selfBinder, selfBinderPos)
            val update = compile(choice.update)

            withLabel(
              s"exercise @${tmplId.qualifiedName} ${cname}",
              SEAbs(4) {
                SELet(
                  // stack: <actors> <cid> <choiceArg> <token>
                  // <tmplArg> =
                  SBUFetch(tmplId)(SEVar(3) /* <cid> */, SEVar(1) /* <token> */ ),
                  // stack: <actors> <cid> <choiceArg> <token> <tmplArg>
                  // _ =
                  SBUBeginExercise(tmplId, choice.name, choice.consuming, byKey = false)(
                    SEVar(3), /* <choiceArg> */
                    SEVar(4), /* <cid> */
                    SEVar(5), /* <actors> */
                    signatories,
                    observers,
                    controllers,
                    mbKey,
                    SEVar(2) /* <token> */
                  ),
                  // stack: <actors> <cid> <choiceArg> <token> <tmplArg> ()
                  // <ret value> =
                  SEApp(update, Array(SEVar(3) /* <token> */ )),
                  // stack: <actors> <cid> <choiceArg> <token> <tmplArg> () <ret value>
                  // _ =
                  SBUEndExercise(tmplId)(
                    SEVar(4), /* <token> */
                    SEVar(1) /* <retValue> */
                  )
                ) in
                  // stack: <actors> <cid> <choiceArg> <token> <tmplArg> () <ret value> ()
                  SEVar(2) /* <retValue> */
              }
            )
          },
        ),
      )

  /** Compile a choice into a top-level function for exercising that choice */
  private[this] def compileChoiceByKey(
      tmplId: TypeConName,
      tmpl: Template,
      tmplKey: TemplateKey,
      cname: ChoiceName,
      choice: TemplateChoice,
  ): (ChoiceByKeyDefRef, SExpr) =
    // Compiles a choice into:
    // SomeTemplate$SomeChoice = \<byKey flag> <actors> <cid> <choiceArg> <token> ->
    //   let targ = fetch <cid>
    //       _ = $beginExercise[tmplId, chId] <choiceArg> <cid> <actors> <byKey flag> sigs obs ctrls mbKey <token>
    //       result = <updateE>
    //       _ = $endExercise[tmplId]
    //   in result
    ChoiceByKeyDefRef(tmplId, cname) ->
      validate(
        closureConvert(
          Map.empty,
          withEnv { _ =>
            env = env.incrPos /* <actors> */
            env = env.incrPos /* <key> */
            val choiceArgumentPos = env.position
            env = env.incrPos /* <choiceArg> */
            env = env.incrPos /* <token> */
            env = env.incrPos //  <keyWithM>
            val selfBinderPos = env.position
            env = env.incrPos /* <cid> */
            env = env.addExprVar(tmpl.param) /* <tmplArg> */
            val signatories = compile(tmpl.signatories)
            val observers = compile(tmpl.observers)
            // now allow access to the choice argument
            env = env.addExprVar(choice.argBinder._1, choiceArgumentPos)
            val controllers = compile(choice.controllers)
            val mbKey: SExpr = tmpl.key match {
              case None => SEValue.None
              case Some(k) => SEApp(SEBuiltin(SBSome), Array(translateKeyWithMaintainers(k)))
            }
            env = env.incrPos // beginExercise's ()
            // allow access to the self contract id
            env = env.addExprVar(choice.selfBinder, selfBinderPos)
            val update = compile(choice.update)

            withLabel(
              s"exercise @${tmplId.qualifiedName} ${cname}",
              SEAbs(4) {
                SELet(
                  // stack: <actors> <key> <choiceArg> <token>
                  // <keyWithM> =
                  SBStructCon(keyWithMaintainersStruct)(
                    SEVar(3), // key
                    SEApp(compile(tmplKey.maintainers), Array(SEVar(3) /* key */ )),
                  ),
                  // stack: <actors> <key> <choiceArg> <token> <keyWithM>
                  // <cid> =
                  SBUFetchKey(tmplId)(
                    SEVar(1), /* <keyWithM> */
                    SEVar(2) /* <token> */
                  ),
                  // stack: <actors> <key> <choiceArg> <token> <keyWithM> <cid>
                  // <tmplArg> =
                  SBUFetch(tmplId)(SEVar(1) /* <cid> */, SEVar(3) /* <token> */ ),
                  // stack: <actors> <key> <choiceArg> <token> <keyWithM> <cid> <tmplArg>
                  // _ =
                  SBUBeginExercise(tmplId, choice.name, choice.consuming, byKey = true)(
                    SEVar(5), /* <choiceArg> */
                    SEVar(2), /* <cid> */
                    SEVar(7), /* <actors> */
                    signatories,
                    observers,
                    controllers,
                    mbKey,
                    SEVar(4) /* <token> */
                  ),
                  // stack: <actors> <key> <choiceArg> <token> <keyWithM> <cid> <tmplArg> ()
                  // <ret value> =
                  SEApp(update, Array(SEVar(5) /* <token> */ )),
                  // stack: <actors> <key> <choiceArg> <token> <keyWithM> <cid> <tmplArg> () <retValue>
                  // _ =
                  SBUEndExercise(tmplId)(
                    SEVar(6), /* <token> */
                    SEVar(1) /* <retValue> */
                  ),
                ) in
                  // stack: <actors> <key> <choiceArg> <token> <keyWithM> <cid> <tmplArg> () <retValue> ()
                  SEVar(2) /* <retValue> */
              }
            )
          },
        ),
      )

  @tailrec
  private[this] def stripLocs(expr: Expr): Expr =
    expr match {
      case ELocation(_, expr1) => stripLocs(expr1)
      case _ => expr
    }

  private[this] def lookupPackage(pkgId: PackageId): Package =
    if (packages.isDefinedAt(pkgId)) packages(pkgId)
    else throw PackageNotFound(pkgId)

  private[this] def lookupDefinition(tycon: TypeConName): Option[Definition] =
    lookupPackage(tycon.packageId).modules
      .get(tycon.qualifiedName.module)
      .flatMap(mod => mod.definitions.get(tycon.qualifiedName.name))

  private[this] def lookupVariantDefinition(tycon: TypeConName): Option[DataVariant] =
    lookupDefinition(tycon).flatMap {
      case DDataType(_, _, data: DataVariant) =>
        Some(data)
      case _ =>
        None
    }

  private[this] def lookupEnumDefinition(tycon: TypeConName): Option[DataEnum] =
    lookupDefinition(tycon).flatMap {
      case DDataType(_, _, data: DataEnum) =>
        Some(data)
      case _ =>
        None
    }

  private[this] def lookupRecordIndex(tapp: TypeConApp, field: FieldName): Int =
    lookupDefinition(tapp.tycon)
      .flatMap {
        case DDataType(_, _, DataRecord(fields, _)) =>
          val idx = fields.indexWhere(_._1 == field)
          if (idx < 0) None else Some(idx)
        case _ => None
      }
      .getOrElse(throw CompilationError(s"record type $tapp not found"))

  private[this] def withEnv[A](f: Unit => A): A = {
    val oldEnv = env
    val x = f(())
    env = oldEnv
    x
  }

  private[this] def withBinders[A](binders: ExprVarName*)(f: Unit => A): A =
    withEnv { _ =>
      env = (binders foldLeft env)(_ addExprVar _)
      f(())
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
  def closureConvert(remaps: Map[Int, SELoc], expr: SExpr): SExpr = {
    // remaps is a function which maps the relative offset from variables (SEVar) to their runtime location
    // The Map must contain a binding for every variable referenced.
    // The Map is consulted when translating variable references (SEVar) and free variables of an abstraction (SEAbs)
    def remap(i: Int): SELoc =
      remaps.get(i) match {
        case None => throw CompilationError(s"remap($i),remaps=$remaps")
        case Some(loc) => loc
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
        val newRemapsF: Map[Int, SELoc] = fvs.zipWithIndex.map {
          case (orig, i) =>
            (orig + arity) -> SELocF(i)
        }.toMap
        val newRemapsA = (1 to arity).map {
          case i =>
            i -> SELocA(arity - i)
        }
        // The keys in newRemapsF and newRemapsA are disjoint
        val newBody = closureConvert(newRemapsF ++ newRemapsA, body)
        SEMakeClo(fvs.map(remap).toArray, arity, newBody)

      case x: SELoc =>
        throw CompilationError(s"closureConvert: unexpected SELoc: $x")

      case x: SEMakeClo =>
        throw CompilationError(s"closureConvert: unexpected SEMakeClo: $x")

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
          alts.map {
            case SCaseAlt(pat, body) =>
              val n = patternNArgs(pat)
              SCaseAlt(
                pat,
                closureConvert(shift(remaps, n), body),
              )
          },
        )

      case SELet(bounds, body) =>
        SELet(bounds.zipWithIndex.map {
          case (b, i) =>
            closureConvert(shift(remaps, i), b)
        }, closureConvert(shift(remaps, bounds.length), body))

      case SECatch(body, handler, fin) =>
        SECatch(
          closureConvert(remaps, body),
          closureConvert(remaps, handler),
          closureConvert(remaps, fin),
        )

      case SELabelClosure(label, expr) =>
        SELabelClosure(label, closureConvert(remaps, expr))

      case x: SEDamlException =>
        throw CompilationError(s"unexpected SEDamlException: $x")

      case x: SEImportValue =>
        throw CompilationError(s"unexpected SEImportValue: $x")

      case x: SEAppAtomicGeneral => throw CompilationError(s"closureConvert: unexpected: $x")
      case x: SEAppAtomicSaturatedBuiltin =>
        throw CompilationError(s"closureConvert: unexpected: $x")
      case x: SELet1General => throw CompilationError(s"closureConvert: unexpected: $x")
      case x: SELet1Builtin => throw CompilationError(s"closureConvert: unexpected: $x")
      case x: SECaseAtomic => throw CompilationError(s"closureConvert: unexpected: $x")
    }
  }

  // Modify/extend `remaps` to reflect when new values are pushed on the stack.  This
  // happens as we traverse into SELet and SECase bodies which have bindings which at
  // runtime will appear on the stack.
  // We must modify `remaps` because it is keyed by indexes relative to the end of the stack.
  // And any values in the map which are of the form SELocS must also be _shifted_
  // because SELocS indexes are also relative to the end of the stack.
  def shift(remaps: Map[Int, SELoc], n: Int): Map[Int, SELoc] = {

    // We must update both the keys of the map (the relative-indexes from the original SEVar)
    // And also any values in the map which are stack located (SELocS), which are also indexed relatively
    val m1 = remaps.map { case (k, loc) => (n + k, shiftLoc(loc, n)) }

    // And create mappings for the `n` new stack items
    val m2 = (1 to n).map(i => (i, SELocS(i)))

    m1 ++ m2
  }

  def shiftLoc(loc: SELoc, n: Int): SELoc = loc match {
    case SELocS(i) => SELocS(i + n)
    case SELocA(_) | SELocF(_) => loc
  }

  /** Compute the free variables in a speedy expression.
    * The returned free variables are de bruijn indices
    * adjusted to the stack of the caller. */
  def freeVars(expr: SExpr, initiallyBound: Int): Set[Int] = {
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
        case x: SELoc =>
          throw CompilationError(s"freeVars: unexpected SELoc: $x")
        case x: SEMakeClo =>
          throw CompilationError(s"freeVars: unexpected SEMakeClo: $x")
        case SECase(scrut, alts) =>
          alts.foldLeft(go(scrut, bound, free)) {
            case (acc, SCaseAlt(pat, body)) => go(body, bound + patternNArgs(pat), acc)
          }
        case SELet(bounds, body) =>
          bounds.zipWithIndex.foldLeft(go(body, bound + bounds.length, free)) {
            case (acc, (expr, idx)) => go(expr, bound + idx, acc)
          }
        case SECatch(body, handler, fin) =>
          go(body, bound, go(handler, bound, go(fin, bound, free)))
        case SELabelClosure(_, expr) =>
          go(expr, bound, free)
        case x: SEDamlException =>
          throw CompilationError(s"unexpected SEDamlException: $x")
        case x: SEImportValue =>
          throw CompilationError(s"unexpected SEImportValue: $x")

        case x: SEAppAtomicGeneral => throw CompilationError(s"freeVars: unexpected: $x")
        case x: SEAppAtomicSaturatedBuiltin => throw CompilationError(s"freeVars: unexpected: $x")
        case x: SELet1General => throw CompilationError(s"freeVars: unexpected: $x")
        case x: SELet1Builtin => throw CompilationError(s"freeVars: unexpected: $x")
        case x: SECaseAtomic => throw CompilationError(s"freeVars: unexpected: $x")
      }
    go(expr, initiallyBound, Set.empty)
  }

  /** Validate variable references in a speedy expression */
  // validate that we correctly captured all free-variables, and so reference to them is
  // via the surrounding closure, instead of just finding them higher up on the stack
  def validate(expr0: SExpr): SExpr = {

    def goV(v: SValue): Unit =
      v match {
        case _: SPrimLit | STNat(_) | STypeRep(_) =>
        case SList(a) => a.iterator.foreach(goV)
        case SOptional(x) => x.foreach(goV)
        case SGenMap(_, entries) =>
          entries.foreach {
            case (k, v) =>
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
        case x: SEVar =>
          throw CompilationError(s"validate: SEVar encountered: $x")
        case abs: SEAbs =>
          throw CompilationError(s"validate: SEAbs encountered: $abs")
        case SEMakeClo(fvs, n, body) =>
          fvs.foreach(goLoc)
          goBody(0, n, fvs.length)(body)
        case SECaseAtomic(scrut, alts) => go(SECase(scrut, alts))
        case SECase(scrut, alts) =>
          go(scrut)
          alts.foreach {
            case SCaseAlt(pat, body) =>
              val n = patternNArgs(pat)
              goBody(maxS + n, maxA, maxF)(body)
          }
        case SELet(bounds, body) =>
          bounds.zipWithIndex.foreach {
            case (rhs, i) =>
              goBody(maxS + i, maxA, maxF)(rhs)
          }
          goBody(maxS + bounds.length, maxA, maxF)(body)
        case _: SELet1General => goLets(maxS)(expr)
        case _: SELet1Builtin => goLets(maxS)(expr)
        case SECatch(body, handler, fin) =>
          go(body)
          go(handler)
          go(fin)
        case SELocation(_, body) =>
          go(body)
        case SELabelClosure(_, expr) =>
          go(expr)
        case x: SEDamlException =>
          throw CompilationError(s"unexpected SEDamlException: $x")
        case x: SEImportValue =>
          throw CompilationError(s"unexpected SEImportValue: $x")
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
          case expr =>
            go(expr)
        }
      }
      go
    }
    goBody(0, 0, 0)(expr0)
    expr0
  }

  private[this] def compileFetch(tmplId: Identifier, tmpl: Template): (FetchDefRef, SExpr) =
    // Translates 'fetch <coid>' into
    // let coid = <coidE>
    // in \token ->
    //   let arg = $fetch coid token
    //       _ = $insertFetch coid <signatories> <observers>
    //   in arg
    FetchDefRef(tmplId) ->
      validate(
        closureConvert(
          Map.empty,
          withEnv { _ =>
            env = env.incrPos // token
            env = env.addExprVar(tmpl.param) // argument
            val signatories = compile(tmpl.signatories)
            val observers = compile(tmpl.observers)
            val key = tmpl.key match {
              case None => SEValue.None
              case Some(k) => SEApp(SEBuiltin(SBSome), Array(translateKeyWithMaintainers(k)))
            }
            withLabel(
              s"fetch @${tmplId.qualifiedName}",
              SEAbs(2) {
                SELet(
                  SBUFetch(tmplId)(
                    SEVar(2), /* coid */
                    SEVar(1) /* token */
                  ),
                  SBUInsertFetchNode(tmplId, byKey = false)(
                    SEVar(3), /* coid */
                    signatories,
                    observers,
                    key,
                    SEVar(2) /* token */
                  ),
                ) in SEVar(2) /* fetch result */
              }
            )
          }
        ))

  private[this] def compileCreate(tmplId: Identifier, tmpl: Template): (CreateDefRef, SExpr) =
    // Translates 'create Foo with <params>' into:
    // let arg = <params>
    // let key = if (we have a key definition in the template) {
    //   let keyBody = <key>
    //   in Some {key: keyBody, maintainers: <key maintainers> keyBody}
    // } else {
    //   None
    // }
    // in \token ->
    //   $create arg <precond> <agreement text> <signatories> <observers> <token> <key>
    CreateDefRef(tmplId) ->
      validate(
        closureConvert(
          Map.empty,
          withEnv { _ =>
            env = env.addExprVar(tmpl.param) // argument
            env = env.incrPos // token
            val precond = compile(tmpl.precond)

            env = env.incrPos // unit returned by SBCheckPrecond
            val agreement = compile(tmpl.agreementText)
            val signatories = compile(tmpl.signatories)
            val observers = compile(tmpl.observers)
            val key = tmpl.key match {
              case None => SEValue.None
              case Some(k) => SEApp(SEBuiltin(SBSome), Array(translateKeyWithMaintainers(k)))
            }
            withLabel(
              s"create @${tmplId.qualifiedName}",
              SEAbs(2) {
                // We check precondition in a separated builtin to prevent
                // further evaluation of agreement, signatories, observers and key
                // in case of failed precondition.
                SELet(SBCheckPrecond(tmplId)(SEVar(2), precond)) in
                  SBUCreate(tmplId)(
                    SEVar(3), /* argument */
                    agreement,
                    signatories,
                    observers,
                    key,
                    SEVar(2) /* token */
                  )
              }
            )
          }
        ))

  private[this] def compileExercise(
      tmplId: Identifier,
      contractId: SExpr,
      choiceId: ChoiceName,
      // actors are only present when compiling old LF update expressions;
      // they are computed from the controllers in newer versions
      optActors: Option[SExpr],
      argument: SExpr,
  ): SExpr =
    // Translates 'A does exercise cid Choice with <params>'
    // into:
    // SomeTemplate$SomeChoice <actorsE> <cidE> <argE>
    withEnv { _ =>
      val actors: SExpr = optActors match {
        case None => SEValue.None
        case Some(actors) => SEApp(SEBuiltin(SBSome), Array(actors))
      }
      SEApp(SEVal(ChoiceDefRef(tmplId, choiceId)), Array(actors, contractId, argument))
    }

  private[this] def compileExerciseByKey(
      tmplId: Identifier,
      key: SExpr,
      choiceId: ChoiceName,
      // actors are only present when compiling old LF update expressions;
      // they are computed from the controllers in newer versions
      optActors: Option[SExpr],
      argument: SExpr,
  ): SExpr =
    // Translates 'A does exercise cid Choice with <params>'
    // into:
    // SomeTemplate$SomeChoice <actorsE> <cidE> <argE>
    withEnv { _ =>
      val actors: SExpr = optActors match {
        case None => SEValue.None
        case Some(actors) => SEApp(SEBuiltin(SBSome), Array(actors))
      }
      SEApp(SEVal(ChoiceByKeyDefRef(tmplId, choiceId)), Array(actors, key, argument))
    }

  private[this] def compileCreateAndExercise(
      tmplId: Identifier,
      createArg: SValue,
      choiceId: ChoiceName,
      choiceArg: SValue,
  ): SExpr =
    withEnv { _ =>
      withLabel(
        s"createAndExercise @${tmplId.qualifiedName} ${choiceId}",
        SEAbs(1) {
          env = env.incrPos // token
          SELet(
            SEApp(SEVal(CreateDefRef(tmplId)), Array(SEValue(createArg), SEVar(1))),
            SEApp(
              compileExercise(
                tmplId = tmplId,
                contractId = SEVar(1),
                choiceId = choiceId,
                optActors = None,
                argument = SEValue(choiceArg),
              ),
              Array(SEVar(2)),
            ),
          ) in SEVar(1)
        }
      )
    }

  private[this] def compileLookupByKey(
      tmplId: Identifier,
      tmplKey: TemplateKey,
  ): (LookupByKeyDefRef, SExpr) =
    // Translates 'lookupByKey Foo <key>' into:
    // let keyWithMaintainers = {key: <key>, maintainers: <key maintainers> <key>}
    // in \token ->
    //    let mbContractId = $lookupKey keyWithMaintainers
    //        _ = $insertLookup Foo keyWithMaintainers
    //    in mbContractId
    LookupByKeyDefRef(tmplId) ->
      validate(
        closureConvert(
          Map.empty,
          withEnv { _ =>
            withLabel(
              s"lookupByKey @${tmplId.qualifiedName}",
              SEAbs(2) {
                env = env.incrPos // key
                env = env.incrPos // token
                SELet(
                  // key with maintainer =
                  SBStructCon(keyWithMaintainersStruct)(
                    SEVar(2), // key
                    SEApp(compile(tmplKey.maintainers), Array(SEVar(2) /* key */ )),
                  ),
                  SBULookupKey(tmplId)(
                    SEVar(1), // key with maintainers
                    SEVar(2) // token
                  ),
                  SBUInsertLookupNode(tmplId)(
                    SEVar(2), // key with maintainers
                    SEVar(1), // mb contract id
                    SEVar(3) // token
                  ),
                ) in SEVar(2) // mb contract id
              }
            )
          }
        ))

  private[this] val fetchByKeyResultStruct: Struct[Int] =
    Struct.assertFromSeq(List(contractIdFieldName, contractFieldName).zipWithIndex)

  @inline
  private[this] def compileFetchByKey(
      tmplId: TypeConName,
      tmpl: Template,
      tmplKey: TemplateKey,
  ): (FetchByKeyDefRef, SExpr) =
    // Translates 'fetchByKey Foo <key>' into:
    // let keyWithMaintainers = {key: <key>, maintainers: <key maintainers> <key>}
    // in \token ->
    //    let coid = $fetchKey keyWithMaintainers token
    //        contract = $fetch coid token
    //        _ = $insertFetch coid <signatories> <observers> Some(keyWithMaintainers)
    //    in { contractId: ContractId Foo, contract: Foo }
    FetchByKeyDefRef(tmplId) ->
      validate(
        closureConvert(
          Map.empty,
          withEnv { _ =>
            withLabel(
              s"fetchByKey @${tmplId.qualifiedName}",
              SEAbs(2) {
                env = env.incrPos // key
                env = env.incrPos // token
                env = env.addExprVar(tmpl.param)
                // TODO should we evaluate this before we even construct
                // the update expression? this might be better for the user
                val signatories = compile(tmpl.signatories)
                val observers = compile(tmpl.observers)
                SELet(
                  // key with maintainer =
                  SBStructCon(keyWithMaintainersStruct)(
                    SEVar(2), // key
                    SEApp(compile(tmplKey.maintainers), Array(SEVar(2) /* key */ )),
                  ),
                  // coid =
                  SBUFetchKey(tmplId)(
                    SEVar(1), // key with maintainers
                    SEVar(2) // token
                  ),
                  // contact =
                  SBUFetch(tmplId)(
                    SEVar(1), // coid
                    SEVar(3) // token
                  ),
                  // _ =
                  SBUInsertFetchNode(tmplId, byKey = true)(
                    SEVar(2), // coid
                    signatories,
                    observers,
                    SEApp(
                      SEBuiltin(SBSome),
                      Array(SEVar(3)) // key with maintainer
                    ),
                    SEVar(4) // token
                  ),
                ) in SBStructCon(fetchByKeyResultStruct)(
                  SEVar(3), // coid
                  SEVar(2) // contract
                )
              }
            )
          }
        ))

  private[this] def compileCommand(cmd: Command): SExpr = cmd match {
    case Command.Create(templateId, argument) =>
      SEApp(SEVal(CreateDefRef(templateId)), Array(SEValue(argument)))
    case Command.Exercise(templateId, contractId, choiceId, argument) =>
      compileExercise(
        tmplId = templateId,
        contractId = SEValue(contractId),
        choiceId = choiceId,
        optActors = None,
        argument = SEValue(argument),
      )
    case Command.ExerciseByKey(templateId, contractKey, choiceId, argument) =>
      compileExerciseByKey(templateId, SEValue(contractKey), choiceId, None, SEValue(argument))
    case Command.Fetch(templateId, coid) =>
      SEApp(SEVal(FetchDefRef(templateId)), Array(SEValue(coid)))
    case Command.CreateAndExercise(templateId, createArg, choice, choiceArg) =>
      compileCreateAndExercise(
        templateId,
        createArg,
        choice,
        choiceArg,
      )
    case Command.LookupByKey(templateId, contractKey) =>
      SEApp(SEVal(LookupByKeyDefRef(templateId)), Array(SEValue(contractKey)))
  }

  @inline
  private[this] def compileCommands(bindings: ImmArray[Command]): SExpr =
    if (bindings.isEmpty)
      SEUpdatePureUnit
    else
      withEnv { _ =>
        val boundHead = compileCommand(bindings.head)
        env = env.incrPos // evaluated body of first binding

        val tokenPosition = env.position
        env = env.incrPos // token

        // add the first binding into the environment
        env = env.incrPos

        // and then the rest
        val boundTail = bindings.tail.toList.map { cmd =>
          val tokenIndex = env.position - tokenPosition
          env = env.incrPos
          SEApp(compileCommand(cmd), Array(SEVar(tokenIndex)))
        }
        val allBounds = SEAppBoundHead +: boundTail
        SELet(boundHead) in
          SEAbs(1) {
            SELet(allBounds: _*) in
              SEApp(SEUpdatePureUnit, Array(SEVar(env.position - tokenPosition)))
          }
      }

}
