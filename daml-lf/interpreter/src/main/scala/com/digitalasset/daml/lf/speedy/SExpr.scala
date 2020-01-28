// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

/**
  * The simplified AST for the speedy interpreter.
  *
  * This reduces the number of binding forms by moving update and scenario
  * expressions into builtins.
  */
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.speedy.Speedy._
import com.digitalasset.daml.lf.speedy.SError._
import com.digitalasset.daml.lf.speedy.SBuiltin._
import java.util.ArrayList

/** The speedy expression:
  * - de Bruijn indexed.
  * - closure converted.
  * - multi-argument applications and abstractions.
  * - all update and scenario operations converted to builtin functions.
  */
sealed abstract class SExpr extends Product with Serializable {
  def execute(machine: Machine): Ctrl

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  override def toString: String =
    productPrefix + productIterator.map(SExpr.prettyPrint).mkString("(", ",", ")")
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
      ref: SDefinitionRef,
      var cached: Option[(SValue, List[Location])],
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

  object SEValue extends SValueContainer[SEValue]

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
      def convertToSValues(fv: Array[Int], getEnv: Int => SValue) = {
        val sValues = new Array[SValue](fv.length)
        var i = 0
        while (i < fv.length) {
          sValues(i) = getEnv(fv(i))
          i = i + 1
        }
        sValues
      }

      val sValues = convertToSValues(fv, machine.getEnv)
      Ctrl.fromPrim(PClosure(body, sValues), arity)
    }
  }

  /** Pattern match. */
  final case class SECase(scrut: SExpr, alts: Array[SCaseAlt]) extends SExpr with SomeArrayEquals {
    def execute(machine: Machine): Ctrl = {
      machine.kont.add(KMatch(alts))
      CtrlExpr(scrut)
    }

    override def toString: String = s"SECase($scrut, ${alts.mkString("[", ",", "]")})"
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
      machine.pushLocation(loc)
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
  final case class SCPVariant(id: Identifier, variant: Name) extends SCasePat

  /** Match on a variant. On match the value is unboxed and pushed to environment. */
  final case class SCPEnum(id: Identifier, constructor: Name) extends SCasePat

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

  sealed abstract class SDefinitionRef {
    def ref: DefinitionRef
    def packageId: PackageId = ref.packageId
    def modName: ModuleName = ref.qualifiedName.module
  }

  // references to definitions that come from the archive
  final case class LfDefRef(ref: DefinitionRef) extends SDefinitionRef
  // references to definitions generated by the Speedy compiler
  final case class ChoiceDefRef(ref: DefinitionRef, choiceName: ChoiceName) extends SDefinitionRef

  //
  // List builtins (foldl, foldr, equalList) are implemented as recursive
  // definition to save java stack
  //

  final case class SEBuiltinRecursiveDefinition(ref: SEBuiltinRecursiveDefinition.Reference)
      extends SExpr {

    import SEBuiltinRecursiveDefinition._

    def execute(machine: Machine): Ctrl = {
      val body = ref match {
        case Reference.FoldL => foldLBody
        case Reference.FoldR => foldRBody
        case Reference.EqualList => equalListBody
      }
      body.execute(machine)
    }
  }

  final object SEBuiltinRecursiveDefinition {

    sealed abstract class Reference

    final object Reference {
      final case object FoldL extends Reference
      final case object FoldR extends Reference
      final case object EqualList extends Reference
    }

    val FoldL: SEBuiltinRecursiveDefinition = SEBuiltinRecursiveDefinition(Reference.FoldL)
    val FoldR: SEBuiltinRecursiveDefinition = SEBuiltinRecursiveDefinition(Reference.FoldR)
    val EqualList: SEBuiltinRecursiveDefinition = SEBuiltinRecursiveDefinition(Reference.EqualList)

    private val foldLBody: SExpr =
      // foldl f z xs =
      SEMakeClo(
        Array(),
        3,
        // case xs of
        SECase(SEVar(1)) of (
          // nil -> z
          SCaseAlt(SCPNil, SEVar(2)),
          // cons y ys ->
          SCaseAlt(
            SCPCons,
            // foldl f (f z y) ys
            SEApp(
              FoldL,
              Array(
                SEVar(5), /* f */
                SEApp(
                  SEVar(5),
                  Array(
                    SEVar(4) /* z */,
                    SEVar(2), /* y */
                  ),
                ),
                SEVar(1), /* ys */
              ),
            ),
          )
        ),
      )

    private val foldRBody: SExpr =
      // foldr f z xs =
      SEMakeClo(
        Array(),
        3,
        // case xs of
        SECase(SEVar(1)) of (// nil -> z
        SCaseAlt(SCPNil, SEVar(2)),
        // cons y ys ->
        SCaseAlt(
          SCPCons,
          // f y (foldr f z ys)
          SEApp(
            SEVar(5),
            Array(
              /* f */
              SEVar(2), /* y */
              SEApp(
                FoldR,
                Array(
                  /* foldr f z ys */
                  SEVar(5), /* f */
                  SEVar(4), /* z */
                  SEVar(1), /* ys */
                ),
              ),
            ),
          ),
        )),
      )

    private val equalListBody: SExpr =
      // equalList f xs ys =
      SEMakeClo(
        Array(),
        3,
        // case xs of
        SECase(SEVar(2) /* xs */ ) of (
          // nil ->
          SCaseAlt(
            SCPNil,
            // case ys of
            //   nil -> True
            //   default -> False
            SECase(SEVar(1)) of (SCaseAlt(SCPNil, SEValue.True),
            SCaseAlt(SCPDefault, SEValue.False)),
          ),
          // cons x xss ->
          SCaseAlt(
            SCPCons,
            // case ys of
            //       True -> listEqual f xss yss
            //       False -> False
            SECase(SEVar(3) /* ys */ ) of (
              // nil -> False
              SCaseAlt(SCPNil, SEValue.False),
              // cons y yss ->
              SCaseAlt(
                SCPCons,
                // case f x y of
                SECase(SEApp(SEVar(7), Array(SEVar(4), SEVar(2)))) of (
                  SCaseAlt(
                    SCPPrimCon(PCTrue),
                    SEApp(EqualList, Array(SEVar(7), SEVar(1), SEVar(3))),
                  ),
                  SCaseAlt(SCPPrimCon(PCFalse), SEValue.False)
                ),
              )
            ),
          )
        ),
      )
  }

  private def prettyPrint(x: Any): String =
    x match {
      case i: Array[Any] => i.mkString("[", ",", "]")
      case i: Array[Int] => i.mkString("[", ",", "]")
      case i: java.util.ArrayList[_] => i.toArray().mkString("[", ",", "]")
      case other: Any => other.toString
    }

}
