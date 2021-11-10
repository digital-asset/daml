// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  *     - moving to de Bruijn indexing for variable
  *     - moving to multi-argument applications and abstractions.
  *
  * 2: closure conversion
  * 3: transform to ANF
  *
  * Stages 1 and 2 are in Compiler.scala; stage 3 in Anf.scala.
  * During Stage3 (ANF transformation), we move from this type (SExpr0) to SExpr,
  * and so have the expression form suitable for execution on a speedy machine.
  */

import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast
import com.daml.lf.value.{Value => V}
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.{SExpr => t}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
private[speedy] object SExpr0 {

  sealed abstract class SExpr extends Product with Serializable

  sealed abstract class SExprAtomic extends SExpr

  /** Reference to a variable. 'index' is the 1-based de Bruijn index,
    * that is, SEVar(1) points to the nearest enclosing variable binder.
    * which could be an SELam, SELet, or a binding variant of SECasePat.
    * https://en.wikipedia.org/wiki/De_Bruijn_index
    * This expression form is only allowed prior to closure conversion
    */
  final case class SEVar(index: Int) extends SExprAtomic

  /** Reference to a value. On first lookup the evaluated expression is
    * stored in 'cached'.
    */
  final case class SEVal(ref: t.SDefinitionRef) extends SExpr

  /** Reference to a builtin function */
  final case class SEBuiltin(b: SBuiltin) extends SExprAtomic

  /** A pre-computed value, usually primitive literal, e.g. integer, text, boolean etc. */
  final case class SEValue(v: SValue) extends SExprAtomic

  object SEValue extends SValueContainer[SEValue]

  /** Function application:
    *    General case: 'fun' and 'args' are any kind of expression
    */
  final case class SEAppGeneral(fun: SExpr, args: Array[SExpr]) extends SExpr with SomeArrayEquals

  object SEApp {
    def apply(fun: SExpr, args: Array[SExpr]): SExpr = {
      SEAppGeneral(fun, args)
    }
  }

  /** Lambda abstraction. Transformed into SEMakeClo in lambda lifting.
    * NOTE(JM): Compilation done in two passes so that closure conversion
    * can be written against this simplified expression type.
    */
  final case class SEAbs(arity: Int, body: SExpr) extends SExpr

  object SEAbs {
    // Helper for constructing abstraction expressions:
    // SEAbs(1) { ... }
    def apply(arity: Int)(body: SExpr): SExpr = SEAbs(arity, body)

    val identity: SEAbs = SEAbs(1, SEVar(1))
  }

  /** Closure creation. Create a new closure object storing the free variables
    * in 'body'.
    */
  final case class SEMakeClo(fvs: Array[SELoc], arity: Int, body: SExpr)
      extends SExpr
      with SomeArrayEquals

  /** SELoc -- Reference to the runtime location of a variable.
    *
    *    This is the closure-converted form of SEVar. There are three sub-forms, with sufffix:
    *    S/A/F, indicating [S]tack, [A]argument, or [F]ree variable captured by a closure.
    */
  sealed abstract class SELoc extends SExprAtomic

  // SELocS -- variable is located on the stack (SELet & binding forms of SECasePat)
  final case class SELocS(n: Int) extends SELoc

  // SELocS -- variable is located in the args array of the application
  final case class SELocA(n: Int) extends SELoc

  // SELocF -- variable is located in the free-vars array of the closure being applied
  final case class SELocF(n: Int) extends SELoc

  /** Pattern match. */
  final case class SECase(scrut: SExpr, alts: Array[SCaseAlt]) extends SExpr with SomeArrayEquals

  /** A let-expression with a single RHS
    * This form only exists *during* the ANF transformation, but not when the ANF
    * transformation is finished.
    */
  final case class SELet1General(rhs: SExpr, body: SExpr) extends SExpr with SomeArrayEquals

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

  /** We cannot crash in the engine call back.
    * Rather, we set the control to this expression and then crash when executing.
    */
  final case class SEDamlException(error: interpretation.Error) extends SExpr

  final case class SEImportValue(typ: Ast.Type, value: V) extends SExpr

  /** Exception handler */
  final case class SETryCatch(body: SExpr, handler: SExpr) extends SExpr

  /** Exercise scope (begin..end) */
  final case class SEScopeExercise(body: SExpr) extends SExpr

  /** Case alternative. If the 'pattern' matches, then the environment is accordingly
    * extended and 'body' is evaluated.
    */
  final case class SCaseAlt(pattern: t.SCasePat, body: SExpr)

  //
  // List builtins (equalList) are implemented as recursive
  // definition to save java stack
  //

  final case class SEBuiltinRecursiveDefinition(ref: t.SEBuiltinRecursiveDefinition.Reference)
      extends SExprAtomic

}
