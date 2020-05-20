// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

/**
  * The simplified AST for the speedy interpreter.
  *
  * This reduces the number of binding forms by moving update and scenario
  * expressions into builtins.
  */
import java.util

import com.daml.lf.language.Ast._
import com.daml.lf.data.Ref._
import com.daml.lf.value.{Value => V}
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.Speedy._
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SBuiltin._
import java.util.ArrayList

/** The speedy expression:
  * - de Bruijn indexed.
  * - closure converted.
  * - multi-argument applications and abstractions.
  * - all update and scenario operations converted to builtin functions.
  */
sealed abstract class SExpr extends Product with Serializable {
  def execute(machine: Machine): Unit
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  override def toString: String =
    productPrefix + productIterator.map(SExpr.prettyPrint).mkString("(", ",", ")")
}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
object SExpr {

  /** Reference to a variable. 'index' is the 1-based de Bruijn index,
    * that is, SEVar(1) points to the nearest enclosing variable binder.
    * which could be an SELam, SELet, or a binding variant of SECasePat.
    * https://en.wikipedia.org/wiki/De_Bruijn_index
    * This expression form is only allowed prior to closure conversion
    */
  final case class SEVar(index: Int) extends SExpr {
    def execute(machine: Machine): Unit = {
      crash("unexpected SEVar, expected SELoc(S/A/F)")
    }
  }

  /** Reference to a value. On first lookup the evaluated expression is
    * stored in 'cached'.
    */
  final case class SEVal(
      ref: SDefinitionRef,
  ) extends SExpr {

    // The variable `_cached` is used to cache the evaluation of the
    // LF value defined by `ref` once it has been computed.  Hence we
    // avoid both the lookup in the package definition HashMap and the
    // full reevaluation of the body of the definition.
    // Here we take advantage of the Java memory model
    // (https://docs.oracle.com/javase/specs/jls/se8/html/jls-17.html#jls-17.7)
    // that guarantees the write of `_cache` (in the method
    // `setCached`) is done atomically.
    // This is similar how hashcode evaluation is cached in String
    // http://hg.openjdk.java.net/jdk8/jdk8/jdk/file/tip/src/share/classes/java/lang/String.java
    private var _cached: Option[(SValue, List[Location])] = None

    def cached: Option[(SValue, List[Location])] = _cached

    def setCached(sValue: SValue, stack_trace: List[Location]): Unit =
      _cached = Some((sValue, stack_trace))

    def execute(machine: Machine): Unit =
      machine.lookupVal(this)
  }

  /** Reference to a builtin function */
  final case class SEBuiltin(b: SBuiltin) extends SExpr {
    def execute(machine: Machine): Unit = {
      /* special case for nullary record constructors */
      machine.returnValue = b match {
        case SBRecCon(id, fields) if b.arity == 0 =>
          SRecord(id, fields, new ArrayList())
        case _ =>
          SPAP(PBuiltin(b), new util.ArrayList[SValue](), b.arity)
      }
    }
  }

  /** A pre-computed value, usually primitive literal, e.g. integer, text, boolean etc. */
  final case class SEValue(v: SValue) extends SExpr {
    def execute(machine: Machine): Unit = {
      machine.returnValue = v
    }
  }

  object SEValue extends SValueContainer[SEValue]

  /** Function application. Apply 'args' to function 'fun', where 'fun'
    * evaluates to a builtin or a closure.
    */
  final case class SEApp(fun: SExpr, args: Array[SExpr]) extends SExpr with SomeArrayEquals {
    def execute(machine: Machine): Unit = {
      machine.pushKont(KArg(args, machine.frame, machine.env.size))
      machine.ctrl = fun
    }
  }

  /** Lambda abstraction. Transformed into SEMakeClo in lambda lifting.
    * NOTE(JM): Compilation done in two passes so that closure conversion
    * can be written against this simplified expression type.
    */
  final case class SEAbs(arity: Int, body: SExpr) extends SExpr {
    def execute(machine: Machine): Unit =
      crash("unexpected SEAbs, expected SEMakeClo")
  }

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
      with SomeArrayEquals {

    def execute(machine: Machine): Unit = {
      val sValues = Array.ofDim[SValue](fvs.length)
      var i = 0
      while (i < fvs.length) {
        sValues(i) = fvs(i).lookup(machine)
        i += 1
      }
      machine.returnValue = SPAP(PClosure(null, body, sValues), new util.ArrayList[SValue](), arity)
    }
  }

  /** SELoc -- Reference to the runtime location of a variable.

    This is the closure-converted form of SEVar. There are three sub-forms, with sufffix:
    S/A/F, indicating [S]tack, [A]argument, or [F]ree variable captured by a closure.
    */
  sealed abstract class SELoc extends SExpr {
    def lookup(machine: Machine): SValue
    def execute(machine: Machine): Unit = {
      machine.returnValue = lookup(machine)
    }
  }

  // SELocS -- variable is located on the stack (SELet & binding forms of SECasePat)
  final case class SELocS(n: Int) extends SELoc {
    def lookup(machine: Machine): SValue = {
      machine.getEnvStack(n)
    }
  }

  // SELocS -- variable is located in the args array of the application
  final case class SELocA(n: Int) extends SELoc {
    def lookup(machine: Machine): SValue = {
      machine.getEnvArg(n)
    }
  }

  // SELocF -- variable is located in the free-vars array of the closure being applied
  final case class SELocF(n: Int) extends SELoc {
    def lookup(machine: Machine): SValue = {
      machine.getEnvFree(n)
    }
  }

  /** Pattern match. */
  final case class SECase(scrut: SExpr, alts: Array[SCaseAlt]) extends SExpr with SomeArrayEquals {
    def execute(machine: Machine): Unit = {
      machine.pushKont(KMatch(alts, machine.frame, machine.env.size))
      machine.ctrl = scrut
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
    def execute(machine: Machine): Unit = {

      // Evaluate the body after we've evaluated the binders
      machine.pushKont(
        KPushTo(machine.env, body, machine.frame, machine.env.size + bounds.size - 1))

      // Start evaluating the let binders
      for (i <- 1 until bounds.size) {
        val b = bounds(bounds.size - i)
        val expectedEnvSize = machine.env.size + bounds.size - i - 1
        machine.pushKont(KPushTo(machine.env, b, machine.frame, expectedEnvSize))
      }
      machine.ctrl = bounds.head
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
    def execute(machine: Machine): Unit = {
      machine.pushLocation(loc)
      machine.ctrl = expr
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
    def execute(machine: Machine): Unit = {
      machine.pushKont(KCatch(handler, fin, machine.frame, machine.env.size))
      machine.ctrl = body
    }
  }

  /** This is used only during profiling. When a package is compiled with
    * profiling enabled, the right hand sides of top-level and let bindings,
    * lambdas and some builtins are wrapped into [[SELabelClosure]]. During
    * runtime, if the value resulting from evaluating [[expr]] is a
    * (partially applied) closure, the label of the closure is set to the
    * [[label]] given here.
    * See [[com.daml.lf.speedy.Profile]] for an explanation why we use
    * [[AnyRef]] for the label.
    */
  final case class SELabelClosure(label: AnyRef, expr: SExpr) extends SExpr {
    def execute(machine: Machine): Unit = {
      machine.pushKont(KLabelClosure(label))
      machine.ctrl = expr
    }
  }

  /** When we fetch a contract id from upstream we cannot crash in the upstream
    * calls. Rather, we set the control to this expression and then crash when executing.
    */
  final case class SEWronglyTypeContractId(
      acoid: V.AbsoluteContractId,
      expected: TypeConName,
      actual: TypeConName,
  ) extends SExpr {
    def execute(machine: Machine): Unit = {
      throw DamlEWronglyTypedContract(acoid, expected, actual)
    }
  }

  final case class SEImportValue(value: V[V.ContractId]) extends SExpr {
    def execute(machine: Machine): Unit = {
      machine.importValue(value)
    }
  }

  /** Case patterns */
  sealed trait SCasePat

  /** Match on a variant. On match the value is unboxed and pushed to stack. */
  final case class SCPVariant(id: Identifier, variant: Name, constructorRank: Int) extends SCasePat

  /** Match on a variant. On match the value is unboxed and pushed to stack. */
  final case class SCPEnum(id: Identifier, constructor: Name, constructorRank: Int) extends SCasePat

  /** Match on a primitive constructor, that is on true, false or unit. */
  final case class SCPPrimCon(pc: PrimCon) extends SCasePat

  /** Match on an empty list. */
  final case object SCPNil extends SCasePat

  /** Match on a list. On match, the head and tail of the list is pushed to the stack. */
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

    def execute(machine: Machine): Unit = {
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
        SECase(SELocA(2)) of (
          // nil -> z
          SCaseAlt(SCPNil, SELocA(1)),
          // cons y ys ->
          SCaseAlt(
            SCPCons,
            // foldl f (f z y) ys
            SEApp(
              FoldL,
              Array(
                SELocA(0), /* f */
                SEApp(
                  SELocA(0),
                  Array(
                    SELocA(1), /* z */
                    SELocS(2) /* y */
                  )
                ),
                SELocS(1) /* ys */
              )
            )
          )
        )
      )

    private val foldRBody: SExpr =
      // foldr f z xs =
      SEMakeClo(
        Array(),
        3,
        // case xs of
        SECase(SELocA(2)) of (// nil -> z
        SCaseAlt(SCPNil, SELocA(1)),
        // cons y ys ->
        SCaseAlt(
          SCPCons,
          // f y (foldr f z ys)
          SEApp(
            SELocA(0),
            Array(
              /* f */
              SELocS(2), /* y */
              SEApp(
                FoldR,
                Array(
                  /* foldr f z ys */
                  SELocA(0), /* f */
                  SELocA(1), /* z */
                  SELocS(1) /* ys */
                )
              )
            )
          )
        ))
      )

    private val equalListBody: SExpr =
      // equalList f xs ys =
      SEMakeClo(
        Array(),
        3,
        // case xs of
        SECase(SELocA(1) /* xs */ ) of (
          // nil ->
          SCaseAlt(
            SCPNil,
            // case ys of
            //   nil -> True
            //   default -> False
            SECase(SELocA(2)) of (SCaseAlt(SCPNil, SEValue.True),
            SCaseAlt(SCPDefault, SEValue.False))
          ),
          // cons x xss ->
          SCaseAlt(
            SCPCons,
            // case ys of
            //       True -> listEqual f xss yss
            //       False -> False
            SECase(SELocA(2) /* ys */ ) of (
              // nil -> False
              SCaseAlt(SCPNil, SEValue.False),
              // cons y yss ->
              SCaseAlt(
                SCPCons,
                // case f x y of
                SECase(SEApp(SELocA(0), Array(SELocS(2), SELocS(4)))) of (
                  SCaseAlt(
                    SCPPrimCon(PCTrue),
                    SEApp(EqualList, Array(SELocA(0), SELocS(1), SELocS(3))),
                  ),
                  SCaseAlt(SCPPrimCon(PCFalse), SEValue.False)
                )
              )
            )
          )
        )
      )
  }

  final case object AnonymousClosure

  private def prettyPrint(x: Any): String =
    x match {
      case i: Array[Any] => i.mkString("[", ",", "]")
      case i: Array[Int] => i.mkString("[", ",", "]")
      case i: java.util.ArrayList[_] => i.toArray().mkString("[", ",", "]")
      case other: Any => other.toString
    }

}
