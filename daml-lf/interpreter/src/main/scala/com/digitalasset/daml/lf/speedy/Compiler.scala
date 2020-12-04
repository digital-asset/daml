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
import com.daml.lf.speedy.Profile.LabelModule
import com.daml.lf.transaction.VersionTimeline
import com.daml.lf.validation.{EUnknownDefinition, LEPackage, Validation, ValidationError}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import java.util

import com.github.ghik.silencer.silent

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

  private val SENat: Numeric.Scale => Some[SEValue] =
    Numeric.Scale.values.map(n => Some(SEValue(STNat(n))))

  /** Validates and Compiles all the definitions in the packages provided. Returns them in a Map.
    *
    * The packages do not need to be in any specific order, as long as they and all the packages
    * they transitively reference are in the [[packages]] in the compiler.
    */
  def compilePackages(
      signatures: PackageId PartialFunction PackageSignature,
      packages: Map[PackageId, Package],
      compilerConfig: Compiler.Config,
  ): Either[String, Map[SDefinitionRef, SDefinition]] = {
    val compiler = new Compiler(signatures, compilerConfig)
    try {
      Right(packages.foldLeft(Map.empty[SDefinitionRef, SDefinition]) {
        case (acc, (pkgId, pkg)) => acc ++ compiler.unsafeCompilePackage(pkgId, pkg)
      })
    } catch {
      case CompilationError(msg) => Left(s"Compilation Error: $msg")
      case PackageNotFound(pkgId) => Left(s"Package not found $pkgId")
      case e: ValidationError => Left(e.pretty)
    }
  }

}

private[lf] final class Compiler(
    signatures: PackageId PartialFunction PackageSignature,
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

  case class Position(idx: Int)

  private[this] def nextPosition(): Position = {
    val p = env.position
    env = env.copy(position = env.position + 1)
    Position(p)
  }

  private[this] def svar(p: Position): SEVar = SEVar(env.position - p.idx)

  private[this] def addVar(ref: VarRef, position: Position) =
    env = env.copy(varIndices = env.varIndices.updated(ref, position))

  private[this] def addExprVar(name: ExprVarName, position: Position) =
    addVar(EVarRef(name), position)

  private[this] def addTypeVar(name: TypeVarName, position: Position) =
    addVar(TVarRef(name), position)

  private[this] def hideTypeVar(name: TypeVarName) =
    env = env.copy(varIndices = env.varIndices - TVarRef(name))

  private[this] def vars: List[VarRef] = env.varIndices.keys.toList

  private[this] def lookupVar(varRef: VarRef): Option[SEVar] =
    env.varIndices.get(varRef).map(svar)

  private[this] def lookupExprVar(name: ExprVarName): SEVar =
    lookupVar(EVarRef(name))
      .getOrElse(throw CompilationError(s"Unknown variable: $name. Known: ${vars.mkString(",")}"))

  private[this] def lookupTypeVar(name: TypeVarName): Option[SEVar] =
    lookupVar(TVarRef(name))

  private[this] case class Env(
      position: Int = 0,
      varIndices: Map[VarRef, Position] = Map.empty,
  )

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

  private[this] def app(f: SExpr, a: SExpr) = SEApp(f, Array(a))

  private[this] def let(bound: SExpr)(f: Position => SExpr): SELet =
    f(nextPosition()) match {
      case SELet(bounds, body) =>
        SELet(bound :: bounds, body)
      case otherwise =>
        SELet(List(bound), otherwise)
    }

  private[this] def unaryFunction(f: Position => SExpr): SEAbs =
    withEnv { _ =>
      f(nextPosition())
    } match {
      case SEAbs(n, body) => SEAbs(n + 1, body)
      case otherwise => SEAbs(1, otherwise)
    }

  private[this] def labeledUnaryFunction(label: String)(body: Position => SExpr): SExpr =
    unaryFunction(pos => withLabel(label, body(pos)))

  private[this] def topLevelFunction[SDefRef <: SDefinitionRef: LabelModule.Allowed](
      ref: SDefRef,
      arity: Int)(
      body: List[Position] => SExpr
  ): (SDefRef, SDefinition) =
    ref ->
      SDefinition(
        unsafeClosureConvert(
          SEAbs(arity, withLabel(ref, body(List.fill(arity)(nextPosition()))))
        ))

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
  private[this] def compilationPipeline(sexpr: SExpr): SExpr =
    flattenToAnf(closureConvert(Map.empty, sexpr))

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompileModule(
      pkgId: PackageId,
      module: Module,
  ): Iterable[(SDefinitionRef, SDefinition)] = {
    val builder = Iterable.newBuilder[(SDefinitionRef, SDefinition)]

    module.definitions.foreach {
      case (defName, DValue(_, _, body, _)) =>
        val ref = LfDefRef(Identifier(pkgId, QualifiedName(module.name, defName)))
        builder += (ref -> SDefinition(withLabel(ref, unsafeCompile(body))))
      case _ =>
    }

    module.templates.foreach {
      case (tmplName, tmpl) =>
        val identifier = Identifier(pkgId, QualifiedName(module.name, tmplName))
        builder += compileCreate(identifier, tmpl)
        builder += compileFetch(identifier, tmpl)

        tmpl.choices.values.foreach(builder += compileChoice(identifier, tmpl, _))

        tmpl.key.foreach { tmplKey =>
          builder += compileFetchByKey(identifier, tmpl, tmplKey)
          builder += compileLookupByKey(identifier, tmplKey)
          tmpl.choices.values.foreach(
            builder +=
              compileChoiceByKey(identifier, tmpl, tmplKey, _))
        }
    }

    builder.result()
  }

  /** Validates and compiles all the definitions in the package provided.
    *
    * Fails with [[PackageNotFound]] if the package or any of the packages it refers
    * to are not in the [[signatures]].
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

    signatures.lift(pkgId) match {
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
        Validation.checkPackage(signatures, pkgId, pkg).left.foreach {
          case EUnknownDefinition(_, LEPackage(pkgId_)) =>
            logger.trace(s"compilePackage: Missing $pkgId_, requesting it...")
            throw PackageNotFound(pkgId_)
          case e =>
            throw e
        }
    }

    val t1 = Time.Timestamp.now()

    val result = pkg.modules.values.flatMap(unsafeCompileModule(pkgId, _))

    val t2 = Time.Timestamp.now()
    logger.trace(
      s"compilePackage: $pkgId ready, typecheck=${(t1.micros - t0.micros) / 1000}ms, compile=${(t2.micros - t1.micros) / 1000}ms",
    )

    result
  }

  private[this] def patternNArgs(pat: SCasePat): Int = pat match {
    case _: SCPEnum | _: SCPPrimCon | SCPNil | SCPDefault | SCPNone => 0
    case _: SCPVariant | SCPSome => 1
    case SCPCons => 2
  }

  private[this] def compile(expr0: Expr): SExpr =
    expr0 match {
      case EVar(name) =>
        lookupExprVar(name)
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
      case structProj: EStructProj =>
        structProj.fieldIndex match {
          case None => SBStructProjByName(structProj.field)(compile(structProj.struct))
          case Some(index) => SBStructProj(index)(compile(structProj.struct))
        }
      case structUpd: EStructUpd =>
        structUpd.fieldIndex match {
          case None =>
            SBStructUpdByName(structUpd.field)(compile(structUpd.struct), compile(structUpd.update))
          case Some(index) =>
            SBStructUpd(index)(compile(structUpd.struct), compile(structUpd.update))
        }
      case ECase(scrut, alts) =>
        compileECase(scrut, alts)
      case ENil(_) =>
        SEValue.EmptyList
      case ECons(_, front, tail) =>
        // TODO(JM): Consider emitting SEValue(SList(...)) for
        // constant lists?
        val args = (front.iterator.map(compile) ++ Seq(compile(tail))).toArray
        if (front.length == 1) {
          SEApp(SEBuiltin(SBCons), args)
        } else {
          SEApp(SEBuiltin(SBConsMany(front.length)), args)
        }
      case ENone(_) =>
        SEValue.None
      case ESome(_, body) =>
        SBSome(compile(body))
      case EEnumCon(tyCon, constructor) =>
        val enumDef =
          lookupEnum(tyCon).getOrElse(throw CompilationError(s"enum $tyCon not found"))
        SEValue(SEnum(tyCon, constructor, enumDef.constructorRank(constructor)))
      case EVariantCon(tapp, variant, arg) =>
        val variantDef = lookupVariant(tapp.tycon)
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
        SBToAny(ty)(compile(e))
      case EFromAny(ty, e) =>
        SBFromAny(ty)(compile(e))
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

  private def noArgs = new util.ArrayList[SValue](0)

  @inline
  private[this] def compileERecCon(tApp: TypeConApp, fields: ImmArray[(FieldName, Expr)]): SExpr =
    if (fields.isEmpty)
      SEValue(SRecord(tApp.tycon, ImmArray.empty, noArgs))
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
              val variantDef =
                lookupVariant(tycon).getOrElse(throw CompilationError(s"variant $tycon not found"))
              withBinders(binder) { _ =>
                SCaseAlt(
                  SCPVariant(tycon, variant, variantDef.constructorRank(variant)),
                  compile(expr),
                )
              }

            case CPEnum(tycon, constructor) =>
              val enumDef =
                lookupEnum(tycon).getOrElse(throw CompilationError(s"enum $tycon not found"))
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
  private[this] def compileELet(elet: ELet) =
    withEnv { _ =>
      elet match {
        case ELet(Binding(optBinder, _, bound), body) =>
          let(withOptLabel(optBinder, compile(bound))) { boundPos =>
            optBinder.foreach(addExprVar(_, boundPos))
            compile(body)
          }
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
        FetchDefRef(tmplId)(compile(coidE))
      case UpdateEmbedExpr(_, e) =>
        compileEmbedExpr(e)
      case UpdateCreate(tmplId, arg) =>
        CreateDefRef(tmplId)(compile(arg))
      case UpdateExercise(tmplId, chId, cidE, argE) =>
        compileExercise(
          tmplId = tmplId,
          contractId = compile(cidE),
          choiceId = chId,
          argument = compile(argE),
        )
      case UpdateExerciseByKey(tmplId, chId, keyE, argE) =>
        compileExerciseByKey(tmplId, compile(keyE), chId, compile(argE))
      case UpdateGetTime =>
        SEGetTime
      case UpdateLookupByKey(RetrieveByKey(templateId, key)) =>
        LookupByKeyDefRef(templateId)(compile(key))
      case UpdateFetchByKey(RetrieveByKey(templateId, key)) =>
        FetchByKeyDefRef(templateId)(compile(key))
    }

  @tailrec
  private[this] def compileAbss(expr0: Expr, arity: Int = 0): SExpr =
    expr0 match {
      case EAbs((binder, typ @ _), body, ref @ _) =>
        addExprVar(binder, nextPosition())
        compileAbss(body, arity + 1)
      case ETyAbs((binder, KNat), body) =>
        addTypeVar(binder, nextPosition())
        compileAbss(body, arity + 1)
      case ETyAbs((binder, _), body) =>
        hideTypeVar(binder)
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
      case TVar(name) => lookupTypeVar(name)
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
    //   in $endCommit(mustFail = false) r token
    withEnv { _ =>
      let(compile(partyE)) { partyPos =>
        let(compile(updateE)) { updatePos =>
          labeledUnaryFunction("submit") { tokenPos =>
            let(SBSBeginCommit(optLoc)(svar(partyPos), svar(tokenPos))) { _ =>
              let(app(svar(updatePos), svar(tokenPos))) { resultPos =>
                SBSEndCommit(mustFail = false)(svar(resultPos), svar(tokenPos))
              }
            }
          }
        }
      }
    }

  @inline
  private[this] def compileMustFail(party: Expr, update: Expr, optLoc: Option[Location]): SExpr =
    // \token ->
    //   let _ = $beginCommit [party] <token>
    //       <r> = $catch ([update] <token>) true false
    //   in $endCommit(mustFail = true) <r> <token>
    withEnv { _ =>
      labeledUnaryFunction("submitMustFail") { tokenPos =>
        let(SBSBeginCommit(optLoc)(compile(party), svar(tokenPos))) { _ =>
          let(SECatch(app(compile(update), svar(tokenPos)), SEValue.True, SEValue.False)) {
            resultPos =>
              SBSEndCommit(mustFail = true)(svar(resultPos), svar(tokenPos))
          }
        }
      }
    }

  @inline
  private[this] def compileGetParty(expr: Expr): SExpr =
    labeledUnaryFunction("getParty") { tokenPos =>
      SBSGetParty(compile(expr), svar(tokenPos))
    }

  @inline
  private[this] def compilePass(time: Expr): SExpr =
    labeledUnaryFunction("pass") { tokenPos =>
      SBSPass(compile(time), svar(tokenPos))
    }

  @inline
  private[this] def compileEmbedExpr(expr: Expr): SExpr =
    // EmbedExpr's get wrapped into an extra layer of abstraction
    // to delay evaluation.
    // e.g.
    // embed (error "foo") => \token -> error "foo"
    unaryFunction { tokenPos =>
      app(compile(expr), svar(tokenPos))
    }

  private val SEDropSecondArgument = unaryFunction(firstPos => unaryFunction(_ => svar(firstPos)))

  private[this] def compilePure(body: Expr): SExpr =
    // pure <E>
    // =>
    // ((\x token -> x) <E>)
    app(SEDropSecondArgument, compile(body))

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
      let(compile(bindings.head.bound)) { firstPos =>
        unaryFunction { tokenPos =>
          let(app(svar(firstPos), svar(tokenPos))) { firstBoundPos =>
            bindings.head.binder.foreach(addExprVar(_, firstBoundPos))

            def loop(list: List[Binding]): SExpr = list match {
              case Binding(binder, _, bound) :: tail =>
                let(app(compile(bound), svar(tokenPos))) { boundPos =>
                  binder.foreach(addExprVar(_, boundPos))
                  loop(tail)
                }
              case Nil =>
                app(compile(body), svar(tokenPos))
            }

            loop(bindings.tail.toList)
          }
        }
      }
    }

  private[this] val KeyWithMaintainersStruct =
    SBStructCon(Struct.assertFromSeq(List(keyFieldName, maintainersFieldName).zipWithIndex))

  private[this] def encodeKeyWithMaintainers(keyPos: Position, tmplKey: TemplateKey): SExpr =
    KeyWithMaintainersStruct(svar(keyPos), app(compile(tmplKey.maintainers), svar(keyPos)))

  private[this] def compileKeyWithMaintainers(maybeTmplKey: Option[TemplateKey]): SExpr =
    withEnv { _ =>
      maybeTmplKey match {
        case None => SEValue.None
        case Some(tmplKey) =>
          let(compile(tmplKey.body))(keyPos => SBSome(encodeKeyWithMaintainers(keyPos, tmplKey)))
      }
    }

  private[this] def compileChoiceBody(
      tmplId: TypeConName,
      tmpl: Template,
      choice: TemplateChoice,
  )(
      choiceArgPos: Position,
      cidPos: Position,
      mbKey: Option[Position], // defined for byKey operation
      tokenPos: Position
  ) =
    let(SBUFetch(tmplId)(svar(cidPos))) { tmplArgPos =>
      addExprVar(tmpl.param, tmplArgPos)
      let(
        SBUBeginExercise(tmplId, choice.name, choice.consuming, byKey = mbKey.isDefined)(
          svar(choiceArgPos),
          svar(cidPos),
          compile(tmpl.signatories),
          compile(tmpl.observers), //
          {
            addExprVar(choice.argBinder._1, choiceArgPos)
            compile(choice.controllers)
          }, //
          {
            choice.choiceObservers match {
              case Some(observers) => compile(observers)
              case None => SEValue.EmptyList
            }
          },
          mbKey.fold(compileKeyWithMaintainers(tmpl.key))(pos => SBSome(svar(pos))),
        )
      ) { _ =>
        addExprVar(choice.selfBinder, cidPos)
        let(app(compile(choice.update), svar(tokenPos))) { retValuePos =>
          let(SBUEndExercise(tmplId)(svar(retValuePos))) { _ =>
            svar(retValuePos)
          }
        }
      }
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
    topLevelFunction(ChoiceDefRef(tmplId, choice.name), 3) {
      case List(cidPos, choiceArgPos, tokenPos) =>
        compileChoiceBody(tmplId, tmpl, choice)(
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
    topLevelFunction(ChoiceByKeyDefRef(tmplId, choice.name), 3) {
      case List(keyPos, choiceArgPos, tokenPos) =>
        let(encodeKeyWithMaintainers(keyPos, tmplKey)) { keyWithMPos =>
          let(SBUFetchKey(tmplId)(svar(keyWithMPos))) { cidPos =>
            compileChoiceBody(tmplId, tmpl, choice)(
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

  private[this] def lookupPackage(pkgId: PackageId): PackageSignature =
    if (signatures.isDefinedAt(pkgId)) signatures(pkgId)
    else throw PackageNotFound(pkgId)

  private[this] def lookupDefinitionSignature(tycon: TypeConName): Option[DefinitionSignature] =
    lookupPackage(tycon.packageId).modules
      .get(tycon.qualifiedName.module)
      .flatMap(mod => mod.definitions.get(tycon.qualifiedName.name))

  private[this] def lookupVariant(tycon: TypeConName): Option[DataVariant] =
    lookupDefinitionSignature(tycon).flatMap {
      case DDataType(_, _, data: DataVariant) =>
        Some(data)
      case _ =>
        None
    }

  private[this] def lookupEnum(tycon: TypeConName): Option[DataEnum] =
    lookupDefinitionSignature(tycon).flatMap {
      case DDataType(_, _, data: DataEnum) =>
        Some(data)
      case _ =>
        None
    }

  private[this] def lookupRecordIndex(tapp: TypeConApp, field: FieldName): Int =
    lookupDefinitionSignature(tapp.tycon)
      .flatMap {
        case DDataType(_, _, DataRecord(fields)) =>
          val idx = fields.indexWhere(_._1 == field)
          if (idx < 0) None else Some(idx)
        case _ => None
      }
      .getOrElse(throw CompilationError(s"record type ${tapp.pretty} not found"))

  private[this] def withEnv[A](f: Unit => A): A = {
    val oldEnv = env
    val x = f(())
    env = oldEnv
    x
  }

  private[this] def withBinders[A](binders: ExprVarName*)(f: Unit => A): A =
    withEnv { _ =>
      binders.foreach(addExprVar(_, nextPosition()))
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

      case x: SEAppAtomicGeneral =>
        throw CompilationError(s"closureConvert: unexpected: $x")

      case x: SEAppAtomicSaturatedBuiltin =>
        throw CompilationError(s"closureConvert: unexpected: $x")

      case SELet1General(bound, body) =>
        SELet1General(closureConvert(remaps, bound), closureConvert(shift(remaps, 1), body))

      case x: SELet1Builtin =>
        throw CompilationError(s"closureConvert: unexpected: $x")

      case x: SECaseAtomic =>
        throw CompilationError(s"closureConvert: unexpected: $x")
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
    * adjusted to the stack of the caller. */
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
  private[this] def validate(expr0: SExpr): SExpr = {

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

  @silent("parameter value tokenPos in method compileFetchBody is never used")
  private[this] def compileFetchBody(tmplId: Identifier, tmpl: Template)(
      cidPos: Position,
      mbKey: Option[Position], //defined for byKey operation
      tokenPos: Position,
  ) =
    withEnv { _ =>
      let(SBUFetch(tmplId)(svar(cidPos))) { tmplArgPos =>
        addExprVar(tmpl.param, tmplArgPos)
        let(
          SBUInsertFetchNode(tmplId, byKey = mbKey.isDefined)(
            svar(cidPos),
            compile(tmpl.signatories),
            compile(tmpl.observers),
            mbKey.fold(compileKeyWithMaintainers(tmpl.key))(p => SBSome(svar(p))),
          )) { _ =>
          svar(tmplArgPos)
        }
      }
    }

  private[this] def compileFetch(
      tmplId: Identifier,
      tmpl: Template): (SDefinitionRef, SDefinition) =
    // compile a template to
    // FetchDefRef(tmplId) = \ <coid> <token> ->
    //   let <tmplArg> = $fetch(tmplId) <coid>
    //       _ = $insertFetch(tmplId, false) coid [tmpl.signatories] [tmpl.observers] [tmpl.key]
    //   in <tmplArg>
    topLevelFunction(FetchDefRef(tmplId), 2) {
      case List(cidPos, tokenPos) =>
        compileFetchBody(tmplId, tmpl)(cidPos, None, tokenPos)
    }

  private[this] def compileCreate(
      tmplId: Identifier,
      tmpl: Template): (SDefinitionRef, SDefinition) =
    // Translates 'create Foo with <params>' into:
    // CreateDefRef(tmplId) = \ <tmplArg> <token> ->
    //   let _ = $checkPreconf(tmplId)(<tmplArg> [tmpl.precond]
    //   in $create <tmplArg> [tmpl.agreementText] [tmpl.signatories] [tmpl.observers] [tmpl.key]
    topLevelFunction(CreateDefRef(tmplId), 2) {
      case List(tmplArgPos, tokenPos @ _) =>
        addExprVar(tmpl.param, tmplArgPos)
        // We check precondition in a separated builtin to prevent
        // further evaluation of agreement, signatories, observers and key
        // in case of failed precondition.
        let(SBCheckPrecond(tmplId)(svar(tmplArgPos), compile(tmpl.precond))) { _ =>
          SBUCreate(tmplId)(
            svar(tmplArgPos),
            compile(tmpl.agreementText),
            compile(tmpl.signatories),
            compile(tmpl.observers),
            compileKeyWithMaintainers(tmpl.key),
          )
        }
    }

  private[this] def compileExercise(
      tmplId: Identifier,
      contractId: SExpr,
      choiceId: ChoiceName,
      // actors are only present when compiling old LF update expressions;
      // they are computed from the controllers in newer versions
      argument: SExpr,
  ): SExpr =
    // Translates 'A does exercise by key  Choice with <params>'
    // into:
    // SomeTemplate$SomeChoice <actorsE> <cidE> <argE>
    withEnv { _ =>
      ChoiceDefRef(tmplId, choiceId)(contractId, argument)
    }

  private[this] def compileExerciseByKey(
      tmplId: Identifier,
      key: SExpr,
      choiceId: ChoiceName,
      argument: SExpr,
  ): SExpr =
    // Translates 'A does exerciseByKey <key> <Choice> <with> <params>'
    // into:
    // ChoiceByKeyDefRef(tmplId, choiceId) <actors> <key> <arg>
    withEnv { _ =>
      ChoiceByKeyDefRef(tmplId, choiceId)(key, argument)
    }

  private[this] def compileCreateAndExercise(
      tmplId: Identifier,
      createArg: SValue,
      choiceId: ChoiceName,
      choiceArg: SValue,
  ): SExpr =
    labeledUnaryFunction(s"createAndExercise @${tmplId.qualifiedName} ${choiceId}") { tokenPos =>
      let(CreateDefRef(tmplId)(SEValue(createArg), svar(tokenPos))) { cidPos =>
        app(
          compileExercise(
            tmplId = tmplId,
            contractId = svar(cidPos),
            choiceId = choiceId,
            argument = SEValue(choiceArg),
          ),
          svar(tokenPos)
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
    topLevelFunction(LookupByKeyDefRef(tmplId), 2) {
      case List(keyPos, tokenPos @ _) =>
        let(encodeKeyWithMaintainers(keyPos, tmplKey)) { keyWithMPos =>
          let(SBULookupKey(tmplId)(svar(keyWithMPos))) { maybeCidPos =>
            let(SBUInsertLookupNode(tmplId)(svar(keyWithMPos), svar(maybeCidPos))) { _ =>
              svar(maybeCidPos)
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
    topLevelFunction(FetchByKeyDefRef(tmplId), 2) {
      case List(keyPos, tokenPos) =>
        let(encodeKeyWithMaintainers(keyPos, tmplKey)) { keyWithMPos =>
          let(SBUFetchKey(tmplId)(svar(keyWithMPos))) { cidPos =>
            let(compileFetchBody(tmplId, tmpl)(cidPos, Some(keyWithMPos), tokenPos)) {
              contractPos =>
                FetchByKeyResult(svar(cidPos), svar(contractPos))
            }
          }
        }
    }

  private[this] def compileCommand(cmd: Command): SExpr = cmd match {
    case Command.Create(templateId, argument) =>
      CreateDefRef(templateId)(SEValue(argument))
    case Command.Exercise(templateId, contractId, choiceId, argument) =>
      compileExercise(
        tmplId = templateId,
        contractId = SEValue(contractId),
        choiceId = choiceId,
        argument = SEValue(argument),
      )
    case Command.ExerciseByKey(templateId, contractKey, choiceId, argument) =>
      compileExerciseByKey(templateId, SEValue(contractKey), choiceId, SEValue(argument))
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

  private val SEUpdatePureUnit = unaryFunction(_ => SEValue.Unit)

  private[this] def compileCommands(bindings: ImmArray[Command]): SExpr =
    // commands are compile similarly as update block
    // see compileBlock
    bindings.toList match {
      case Nil =>
        SEUpdatePureUnit
      case first :: rest =>
        let(compileCommand(first)) { firstPos =>
          unaryFunction { tokenPos =>
            let(app(svar(firstPos), svar(tokenPos))) { _ =>
              // we cannot process `rest` recursively without exposing ourselves to stack overflow.
              val exprs = rest.iterator.map { cmd =>
                val expr = app(compileCommand(cmd), svar(tokenPos))
                nextPosition()
                expr
              }.toList
              SELet(exprs, SEValue.Unit)
            }
          }
        }
    }
}
