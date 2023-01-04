// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

/** SExpr0 -- AST for the speedy compiler pipeline.
  *
  * These are *not* the expression forms which run on the speedy machine. See SExpr.
  *
  * These are the expression forms which exist during the speedy compiler pipeline:
  * The pipeline stages are:
  *
  * 1: convert from LF
  *     - reducing binding forms (update & scenario becoming builtins)
  *     - convert named variables to de Bruijn levels
  *     - moving to multi-argument applications and abstractions.
  *
  * 2: closure conversion
  * 3: transform to ANF
  *
  * Stage 1 is in PhaseOne.scala
  * Stage 2 is in ClosureConversion.scala
  * Stage 3 is in Anf.scala
  *
  * During Stage2 (Closure Conversion), we move from SExpr0 to SExpr1,
  * During Stage3 (ANF transformation), we move from SExpr1 to SExpr.
  *
  * Summary of which constructors are contained by: SExp0, SExpr1 and SExpr:
  *
  * - In SExpr{0,1,} (everywhere): SEApp(General), SEBuiltin, SELabelClosure,
  *   SELocation, SEScopeExercise, SETryCatch, SEVal, SEValue,
  *
  * - In SExpr0: SEAbs, SEVar
  *
  * - In SExpr{0,1}: SECase, SELet
  *
  * - In SExpr{1,}: SELocA, SELocF, SELocS, SEMakeClo, SELet1General,
  *
  * - In SExpr: SEAppAtomicGeneral, SEAppAtomicSaturatedBuiltin, SECaseAtomic,
  *   SELet1Builtin, SELet1BuiltinArithmetic, SEAppOnlyFunIsAtomic
  *
  * - In SExpr (runtime only, i.e. rejected by validate): SEDamlException, SEImportValue
  */

import com.daml.lf.data.Ref._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SExpr.{SDefinitionRef, SCasePat}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
private[speedy] object SExpr0 {

  sealed abstract class SExpr extends Product with Serializable

  /** Reference to a variable. 'level' is the 0-based de Bruijn LEVEL (not INDEX)
    * https://en.wikipedia.org/wiki/De_Bruijn_index
    * This expression form is only allowed prior to closure conversion
    */
  final case class SEVarLevel(level: Int) extends SExpr

  /** Reference to a value. On first lookup the evaluated expression is
    * stored in 'cached'.
    */
  final case class SEVal(ref: SDefinitionRef) extends SExpr

  /** Reference to a builtin function */
  final case class SEBuiltin(b: SBuiltin) extends SExpr

  /** A pre-computed value, usually primitive literal, e.g. integer, text, boolean etc. */
  final case class SEValue(v: SValue) extends SExpr

  object SEValue extends SValueContainer[SEValue]

  /** Function application (Function and Args are unrestricted expressions) */
  final case class SEApp(fun: SExpr, args: List[SExpr]) extends SExpr

  /** Lambda abstraction. Transformed to SEMakeClo during closure conversion */
  final case class SEAbs(arity: Int, body: SExpr) extends SExpr

  /** Pattern match. */
  final case class SECase(scrut: SExpr, alts: List[SCaseAlt]) extends SExpr

  /** A non-recursive, non-parallel let block.
    * It is used as an intermediary data structure by the compiler to
    * mitigate stack overflow issues, but are later exploded into
    * [[SELet1General]] and [[SELet1Builtin]] by the ANF transformation.
    */
  final case class SELet(bounds: List[SExpr], body: SExpr) extends SExpr

  /** Location annotation. When encountered the location is stored in the 'lastLocation'
    * variable of the machine. When commit is begun the location is stored in 'commitLocation'.
    */
  final case class SELocation(loc: Location, expr: SExpr) extends SExpr

  /** This is used only during profiling. When a package is compiled with
    * profiling enabled, the right hand sides of top-level and let bindings,
    * lambdas and some builtins are wrapped into [[SELabelClosure]]. During
    * runtime, if the value resulting from evaluating [[expr]] is a
    * (partially applied) closure, the label of the closure is set to the
    * [[label]] given here.
    * See [[com.daml.lf.speedy.Profile]] for an explanation why we use
    * [[AnyRef]] for the label.
    */
  final case class SELabelClosure(label: Profile.Label, expr: SExpr) extends SExpr

  /** Exception handler */
  final case class SETryCatch(body: SExpr, handler: SExpr) extends SExpr

  /** Exercise scope (begin..end) */
  final case class SEScopeExercise(body: SExpr) extends SExpr

  final case class SEPreventCatch(body: SExpr) extends SExpr

  /** Case alternative. If the 'pattern' matches, then the environment is accordingly
    * extended and 'body' is evaluated.
    */
  final case class SCaseAlt(pattern: SCasePat, body: SExpr)
}
