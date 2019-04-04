// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

/**
  * The simplified AST for the speedy interpreter.
  *
  * This reduces the number of binding forms by moving update and scenario
  * expressions into builtins.
  */
import com.digitalasset.daml.lf.lfpackage.Ast._
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.speedy.Speedy._
import com.digitalasset.daml.lf.speedy.SError._
import com.digitalasset.daml.lf.speedy.SBuiltin._
import java.util.ArrayList

import com.digitalasset.daml.lf.data.ImmArray

/** The speedy expression:
  * - de Bruijn indexed.
  * - closure converted.
  * - multi-argument applications and abstractions.
  * - all update and scenario operations converted to builtin functions.
  */
sealed trait SExpr {
  def apply(args: SExpr*): SExpr =
    SExpr.SEApp(this, args.toArray)

  def execute(machine: Machine): Ctrl
}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
object SExpr {

  /** Reference to a variable. 'index' is the 1-based de Bruijn index,
    * that is, SEVar(1) points to the top-most value in the environment.
    * https://en.wikipedia.org/wiki/De_Bruijn_index
    */
  final case class SEVar(index: Int) extends SExpr {
    def execute(machine: Machine): Ctrl = {
      CtrlValue(machine.getEnv(index))
    }
  }

  /** Reference to a value. On first lookup the evaluated expression is
    * stored in 'cached'.
    */
  final case class SEVal(
      ref: DefinitionRef[PackageId],
      var cached: Option[SValue]
  ) extends SExpr {
    def execute(machine: Machine): Ctrl = {
      machine.lookupVal(this)
    }
  }

  /** Reference to a builtin function */
  final case class SEBuiltin(b: SBuiltin) extends SExpr {
    def execute(machine: Machine): Ctrl = {
      /* special case for nullary record constructors */
      b match {
        case SBRecCon(id, fields) if b.arity == 0 =>
          CtrlValue(SRecord(id, fields, new ArrayList()))
        case _ =>
          Ctrl.fromPrim(PBuiltin(b), b.arity)
      }
    }
  }

  /** A pre-computed value, usually primitive literal, e.g. integer, text, boolean etc. */
  final case class SEValue(v: SValue) extends SExpr {
    def execute(machine: Machine): Ctrl = CtrlValue(v)
  }

  /** Function application. Apply 'args' to function 'fun', where 'fun'
    * evaluates to a builtin or a closure.
    */
  final case class SEApp(fun: SExpr, args: Array[SExpr]) extends SExpr with SomeArrayEquals {
    def execute(machine: Machine): Ctrl = {
      machine.kont.add(KArg(args))
      CtrlExpr(fun)
    }
  }

  /** Lambda abstraction. Transformed into SEMakeClo in lambda lifting.
    * NOTE(JM): Compilation done in two passes so that closure conversion
    * can be written against this simplified expression type.
    */
  final case class SEAbs(arity: Int, body: SExpr) extends SExpr {
    def execute(machine: Machine): Ctrl =
      crash("unexpected SEAbs, expected SEMakeClo")
  }
  object SEAbs {
    // Helper for constructing abstraction expressions:
    // SEAbs(1) { ... }
    def apply(arity: Int)(body: SExpr): SExpr = SEAbs(arity, body)
  }

  /** Closure creation. Create a new closure object storing the free variables
    * in 'body'.
    */
  final case class SEMakeClo(fv: Array[Int], arity: Int, body: SExpr)
      extends SExpr
      with SomeArrayEquals {
    def execute(machine: Machine): Ctrl = {
      Ctrl.fromPrim(PClosure(body, fv.map(machine.getEnv)), arity)
    }
  }

  /** Pattern match. */
  final case class SECase(scrut: SExpr, alts: Array[SCaseAlt]) extends SExpr with SomeArrayEquals {
    def execute(machine: Machine): Ctrl = {
      machine.kont.add(KMatch(alts))
      CtrlExpr(scrut)
    }
  }

  object SECase {
    // Helper for constructing case expressions:
    // SECase(scrut) of(
    //   SECaseAlt(SCPNil ,...),
    //   SECaseAlt(SCPDefault, ...),
    // )
    case class PartialSECase(scrut: SExpr) {
      def of(alts: SCaseAlt*): SExpr = SECase(scrut, alts.toArray)
    }
    def apply(scrut: SExpr) = PartialSECase(scrut)
  }

  /** A non-recursive, non-parallel let block. Each bound expression
    * is evaluated in turn and pushed into the environment one by one,
    * with later expressions possibly referring to earlier.
    */
  final case class SELet(bounds: Array[SExpr], body: SExpr) extends SExpr with SomeArrayEquals {
    def execute(machine: Machine): Ctrl = {
      // Pop the block once we're done evaluating the body
      machine.kont.add(KPop(bounds.size))

      // Evaluate the body after we've evaluated the binders
      machine.kont.add(KPushTo(machine.env, body))

      // Start evaluating the let binders
      for (i <- 1 until bounds.size) {
        val b = bounds(bounds.size - i)
        machine.kont.add(KPushTo(machine.env, b))
      }
      CtrlExpr(bounds.head)
    }
  }

  object SELet {

    // Helpers for constructing let expressions:
    // Instead of
    //   SELet(Array(SEVar(4), SEVar(1)), SEVar(1))
    // you can write:
    //   SELet(
    //     SEVar(4),
    //     SEVar(1)
    //   ) in SEVar(1)
    case class PartialSELet(bounds: Array[SExpr]) extends SomeArrayEquals {
      def in(body: => SExpr): SExpr = SELet(bounds, body)
    }
    def apply(bounds: SExpr*) = PartialSELet(bounds.toArray)

  }

  /** Location annotation. When encountered the location is stored in the 'lastLocation'
    * variable of the machine. When commit is begun the location is stored in 'commitLocation'.
    */
  final case class SELocation(loc: Location, expr: SExpr) extends SExpr {
    def execute(machine: Machine): Ctrl = {
      machine.lastLocation = Some(loc)
      CtrlExpr(expr)
    }
  }

  /** A catch expression. This is used internally solely for the purpose of implementing
    * mustFailAt. If the evaluation of 'body' causes an exception of type 'DamlException'
    * (see SError), then the environment and continuation stacks are reset and 'handler'
    * is executed. If the evaluation is successful, then the 'fin' expression is evaluated.
    * This is on purpose very limited, with no mechanism to inspect the exception, nor a way
    * to access the value returned from 'body'.
    */
  final case class SECatch(body: SExpr, handler: SExpr, fin: SExpr) extends SExpr {
    def execute(machine: Machine): Ctrl = {
      machine.kont.add(KCatch(handler, fin, machine.env.size))
      CtrlExpr(body)
    }
  }

  /** Case patterns */
  sealed trait SCasePat

  /** Match on a variant. On match the value is unboxed and pushed to environment. */
  final case class SCPVariant(id: Identifier, variant: String) extends SCasePat

  /** Match on a primitive constructor, that is on true, false or unit. */
  final case class SCPPrimCon(pc: PrimCon) extends SCasePat

  /** Match on an empty list. */
  final case object SCPNil extends SCasePat

  /** Match on a list. On match, the head and tail of the list is pushed to environment. */
  final case object SCPCons extends SCasePat

  /** Default match case. Always matches. */
  final case object SCPDefault extends SCasePat

  final case object SCPNone extends SCasePat

  final case object SCPSome extends SCasePat

  /** Case alternative. If the 'pattern' matches, then the environment is accordingly
    * extended and 'body' is evaluated. */
  final case class SCaseAlt(pattern: SCasePat, body: SExpr)

  /** Construct a reference to a builtin compiled function */
  def makeBuiltinRef(name: String): DefinitionRef[PackageId] =
    DefinitionRef(
      SimpleString.assertFromString("-builtins-"),
      QualifiedName(
        ModuleName.unsafeFromSegments(ImmArray("$")),
        DottedName.unsafeFromSegments(ImmArray(name)))
    )

}
