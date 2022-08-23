// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import java.util
import java.util.regex.Pattern
import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.data.Numeric.Scale
import com.daml.lf.interpretation.Error.InconsistentDisclosureTable
import com.daml.lf.interpretation.{Error => IE}
import com.daml.lf.language.Ast
import com.daml.lf.speedy.ArrayList.Implicits._
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.Speedy._
import com.daml.lf.speedy.{SExpr0 => compileTime}
import com.daml.lf.speedy.{SExpr => runTime}
import com.daml.lf.speedy.SValue.{SValue => _, _}
import com.daml.lf.speedy.SValue.{SValue => SV}
import com.daml.lf.transaction.{
  ContractStateMachine,
  GlobalKey,
  GlobalKeyWithMaintainers,
  Node,
  Transaction => Tx,
}
import com.daml.lf.value.{Value => V}
import com.daml.lf.value.Value.ValueArithmeticError
import com.daml.nameof.NameOf
import com.daml.scalautil.Statement.discard

import scala.jdk.CollectionConverters._
import scala.collection.immutable.TreeSet
import scala.math.Ordering.Implicits.infixOrderingOps

/**  Speedy builtins are stratified into two layers:
  *  Parent: `SBuiltin`, (which are effectful), and child: `SBuiltinPure` (which are pure).
  *
  *  Effectful builtin functions may ask questions of the ledger or change machine state.
  *  Pure builtins can be treated specially because their evaluation is immediate.
  *  This fact is used by the execution of the ANF expression form: `SELet1Builtin`.
  *
  *  Most builtins are pure, and so they extend `SBuiltinPure`
  */
private[speedy] sealed abstract class SBuiltin(val arity: Int) {
  protected def crash(msg: String): Nothing =
    throw SErrorCrash(getClass.getCanonicalName, msg)

  // Helper for constructing expressions applying this builtin.
  // E.g. SBCons(SEVar(1), SEVar(2))

  // TODO: move this into the speedy compiler code
  private[lf] def apply(args: compileTime.SExpr*): compileTime.SExpr =
    compileTime.SEApp(compileTime.SEBuiltin(this), args.toList)

  // TODO: avoid constructing application expression at run time
  private[lf] def apply(args: runTime.SExpr*): runTime.SExpr =
    runTime.SEApp(runTime.SEBuiltin(this), args.toArray)

  /** Execute the builtin with 'arity' number of arguments in 'args'.
    * Updates the machine state accordingly.
    */
  private[speedy] def execute(args: util.ArrayList[SValue], machine: Machine): Control

  protected def unexpectedType(i: Int, expected: String, found: SValue) =
    crash(s"type mismatch of argument $i: expect $expected but got $found")

  final protected def getSBool(args: util.ArrayList[SValue], i: Int): Boolean =
    args.get(i) match {
      case SBool(value) => value
      case otherwise => unexpectedType(i, "SBool", otherwise)
    }

  final protected def getSUnit(args: util.ArrayList[SValue], i: Int): Unit =
    args.get(i) match {
      case SUnit => ()
      case otherwise => unexpectedType(i, "SUnit", otherwise)
    }

  final protected def getSInt64(args: util.ArrayList[SValue], i: Int): Long =
    args.get(i) match {
      case SInt64(value) => value
      case otherwise => unexpectedType(i, "SInt64", otherwise)
    }

  final protected def getSText(args: util.ArrayList[SValue], i: Int): String =
    args.get(i) match {
      case SText(value) => value
      case otherwise => unexpectedType(i, "SText", otherwise)
    }

  final protected def getSNumeric(args: util.ArrayList[SValue], i: Int): Numeric =
    args.get(i) match {
      case SNumeric(value) => value
      case otherwise => unexpectedType(i, "SNumeric", otherwise)
    }

  final protected def getSDate(args: util.ArrayList[SValue], i: Int): Time.Date =
    args.get(i) match {
      case SDate(value) => value
      case otherwise => unexpectedType(i, "SDate", otherwise)
    }

  final protected def getSTimestamp(args: util.ArrayList[SValue], i: Int): Time.Timestamp =
    args.get(i) match {
      case STimestamp(value) => value
      case otherwise => unexpectedType(i, "STimestamp", otherwise)
    }

  final protected def getSTNat(args: util.ArrayList[SValue], i: Int): Numeric.Scale =
    args.get(i) match {
      case STNat(x) => x
      case otherwise => unexpectedType(i, "STNat", otherwise)
    }

  final protected def getSParty(args: util.ArrayList[SValue], i: Int): Party =
    args.get(i) match {
      case SParty(value) => value
      case otherwise => unexpectedType(i, "SParty", otherwise)
    }

  final protected def getSContractId(args: util.ArrayList[SValue], i: Int): V.ContractId =
    args.get(i) match {
      case SContractId(value) => value
      case otherwise => unexpectedType(i, "SContractId", otherwise)
    }

  final protected def getSBigNumeric(args: util.ArrayList[SValue], i: Int): java.math.BigDecimal =
    args.get(i) match {
      case SBigNumeric(value) => value
      case otherwise => unexpectedType(i, "SBigNumeric", otherwise)
    }

  final protected def getSList(args: util.ArrayList[SValue], i: Int): FrontStack[SValue] =
    args.get(i) match {
      case SList(value) => value
      case otherwise => unexpectedType(i, "SList", otherwise)
    }

  final protected def getSOptional(args: util.ArrayList[SValue], i: Int): Option[SValue] =
    args.get(i) match {
      case SOptional(value) => value
      case otherwise => unexpectedType(i, "SOptional", otherwise)
    }

  final protected def getSMap(args: util.ArrayList[SValue], i: Int): SMap =
    args.get(i) match {
      case genMap: SMap => genMap
      case otherwise => unexpectedType(i, "SMap", otherwise)
    }

  final protected def getSMapKey(args: util.ArrayList[SValue], i: Int): SValue = {
    val key = args.get(i)
    SMap.comparable(key)
    key
  }

  final protected def getSRecord(args: util.ArrayList[SValue], i: Int): SRecord =
    args.get(i) match {
      case record: SRecord => record
      case otherwise => unexpectedType(i, "SRecord", otherwise)
    }

  final protected def getSStruct(args: util.ArrayList[SValue], i: Int): SStruct =
    args.get(i) match {
      case struct: SStruct => struct
      case otherwise => unexpectedType(i, "SStruct", otherwise)
    }

  final protected def getSAny(args: util.ArrayList[SValue], i: Int): SAny =
    args.get(i) match {
      case any: SAny => any
      case otherwise => unexpectedType(i, "SAny", otherwise)
    }

  final protected def getSTypeRep(args: util.ArrayList[SValue], i: Int): Ast.Type =
    args.get(i) match {
      case STypeRep(ty) => ty
      case otherwise => unexpectedType(i, "STypeRep", otherwise)
    }

  final protected def getSAnyException(args: util.ArrayList[SValue], i: Int): SRecord =
    args.get(i) match {
      case SAnyException(exception) => exception
      case otherwise => unexpectedType(i, "Exception", otherwise)
    }

  final protected def getSAnyContract(
      args: util.ArrayList[SValue],
      i: Int,
  ): (TypeConName, SRecord) =
    args.get(i) match {
      case SAnyContract(tyCon, value) => (tyCon, value)
      case otherwise => unexpectedType(i, "AnyContract", otherwise)
    }

  final protected def checkToken(args: util.ArrayList[SValue], i: Int): Unit =
    args.get(i) match {
      case SToken => ()
      case otherwise => unexpectedType(i, "SToken", otherwise)
    }

}

private[speedy] sealed abstract class SBuiltinPure(arity: Int) extends SBuiltin(arity) {

  override private[speedy] final def execute(
      args: util.ArrayList[SValue],
      machine: Machine,
  ): Control = {
    Control.Value(executePure(args))
  }

  /** Execute the (pure) builtin with 'arity' number of arguments in 'args'.
    *    Returns the resulting value
    */
  private[speedy] def executePure(args: util.ArrayList[SValue]): SValue
}

private[speedy] sealed abstract class OnLedgerBuiltin(arity: Int)
    extends SBuiltin(arity)
    with Product {

  protected def execute(
      args: util.ArrayList[SValue],
      machine: Machine,
      onLedger: OnLedger,
  ): Control

  final override def execute(args: util.ArrayList[SValue], machine: Machine): Control = {
    machine.withOnLedger(productPrefix)(execute(args, machine, _))
  }
}

private[lf] object SBuiltin {

  //
  // Arithmetic
  //

  private[this] def handleArithmeticException[X](x: => X): Option[X] =
    try {
      Some(x)
    } catch {
      case _: ArithmeticException =>
        None
    }

  private[this] def add(x: Long, y: Long): Option[Long] =
    handleArithmeticException(Math.addExact(x, y))

  private[this] def div(x: Long, y: Long): Option[Long] =
    if (y == 0 || x == Long.MinValue && y == -1)
      None
    else
      Some(x / y)

  private[this] def mult(x: Long, y: Long): Option[Long] =
    handleArithmeticException(Math.multiplyExact(x, y))

  private[this] def sub(x: Long, y: Long): Option[Long] =
    handleArithmeticException(Math.subtractExact(x, y))

  private[this] def mod(x: Long, y: Long): Option[Long] =
    if (y == 0)
      None
    else
      Some(x % y)

  private[this] val SomeOne = Some(1L)

  // Exponentiation by squaring
  // https://en.wikipedia.org/wiki/Exponentiation_by_squaring
  private[this] def exp(base: Long, exponent: Long): Option[Long] =
    if (exponent < 0)
      None
    else if (exponent == 0) SomeOne
    else
      handleArithmeticException {
        var x = base
        var y = 1L
        var n = exponent

        while (n > 1) {
          if (n % 2 == 1)
            y = Math.multiplyExact(y, x)
          x = Math.multiplyExact(x, x)
          n = n >> 1
        }

        Math.multiplyExact(x, y)
      }

  sealed abstract class SBuiltinArithmetic(val name: String, arity: Int) extends SBuiltin(arity) {
    private[speedy] def compute(args: util.ArrayList[SValue]): Option[SValue]

    private[speedy] def buildException(args: util.ArrayList[SValue]) =
      SArithmeticError(
        name,
        args.view.map(litToText(getClass.getCanonicalName, _)).to(ImmArray),
      )

    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control =
      compute(args) match {
        case Some(value) =>
          Control.Value(value)
        case None =>
          unwindToHandler(machine, buildException(args))
      }
  }

  sealed abstract class SBBinaryOpInt64(name: String, op: (Long, Long) => Option[Long])
      extends SBuiltinArithmetic(name, 2) {
    override private[speedy] def compute(args: util.ArrayList[SValue]): Option[SValue] =
      op(getSInt64(args, 0), getSInt64(args, 1)).map(SInt64)
  }

  final case object SBAddInt64 extends SBBinaryOpInt64("ADD_INT64", add)
  final case object SBSubInt64 extends SBBinaryOpInt64("SUB_INT64", sub)
  final case object SBMulInt64 extends SBBinaryOpInt64("MUL_INT64", mult)
  final case object SBDivInt64 extends SBBinaryOpInt64("DIV_INT64", div)
  final case object SBModInt64 extends SBBinaryOpInt64("MOD_INT64", mod)
  final case object SBExpInt64 extends SBBinaryOpInt64("EXP_INT64", exp)

  // Numeric Arithmetic

  private[this] def add(x: Numeric, y: Numeric): Option[Numeric] =
    Numeric.add(x, y).toOption

  private[this] def subtract(x: Numeric, y: Numeric): Option[Numeric] =
    Numeric.subtract(x, y).toOption

  private[this] def multiply(scale: Scale, x: Numeric, y: Numeric): Option[Numeric] =
    Numeric.multiply(scale, x, y).toOption

  private[this] def divide(scale: Scale, x: Numeric, y: Numeric): Option[Numeric] =
    if (y.signum() == 0)
      None
    else
      Numeric.divide(scale, x, y).toOption

  sealed abstract class SBBinaryOpNumeric(name: String, op: (Numeric, Numeric) => Option[Numeric])
      extends SBuiltinArithmetic(name, 3) {
    override private[speedy] def compute(args: util.ArrayList[SValue]): Option[SValue] = {
      val scale = getSTNat(args, 0)
      val a = getSNumeric(args, 1)
      val b = getSNumeric(args, 2)
      assert(a.scale == scale && b.scale == scale)
      op(a, b).map(SNumeric(_))
    }
  }

  sealed abstract class SBBinaryOpNumeric2(
      name: String,
      op: (Scale, Numeric, Numeric) => Option[Numeric],
  ) extends SBuiltinArithmetic(name, 5) {
    override private[speedy] def compute(args: util.ArrayList[SValue]): Option[SValue] = {
      val scaleA = getSTNat(args, 0)
      val scaleB = getSTNat(args, 1)
      val scale = getSTNat(args, 2)
      val a = getSNumeric(args, 3)
      val b = getSNumeric(args, 4)
      assert(a.scale == scaleA && b.scale == scaleB)
      op(scale, a, b).map(SNumeric(_))
    }
  }

  final case object SBAddNumeric extends SBBinaryOpNumeric("ADD_NUMERIC", add)
  final case object SBSubNumeric extends SBBinaryOpNumeric("SUB_NUMERIC", subtract)
  final case object SBMulNumeric extends SBBinaryOpNumeric2("MUL_NUMERIC", multiply)
  final case object SBDivNumeric extends SBBinaryOpNumeric2("DIV_NUMERIC", divide)

  final case object SBRoundNumeric extends SBuiltinArithmetic("ROUND_NUMERIC", 3) {
    override private[speedy] def compute(args: util.ArrayList[SValue]): Option[SNumeric] = {
      val prec = getSInt64(args, 1)
      val x = getSNumeric(args, 2)
      Numeric.round(prec, x).toOption.map(SNumeric(_))
    }
  }

  final case object SBCastNumeric extends SBuiltinArithmetic("CAST_NUMERIC", 3) {
    override private[speedy] def compute(args: util.ArrayList[SValue]): Option[SNumeric] = {
      val outputScale = getSTNat(args, 1)
      val x = getSNumeric(args, 2)
      Numeric.fromBigDecimal(outputScale, x).toOption.map(SNumeric(_))
    }
  }

  final case object SBShiftNumeric extends SBuiltinPure(3) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SNumeric = {
      val inputScale = getSTNat(args, 0)
      val outputScale = getSTNat(args, 1)
      val x = getSNumeric(args, 2)
      SNumeric(
        Numeric.assertFromBigDecimal(outputScale, x.scaleByPowerOfTen(inputScale - outputScale))
      )
    }
  }

  //
  // Text functions
  //
  final case object SBExplodeText extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SList =
      SList(FrontStack.from(Utf8.explode(getSText(args, 0)).map(SText)))
  }

  final case object SBImplodeText extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SText = {
      val xs = getSList(args, 0)
      val ts = xs.map {
        case SText(t) => t
        case v => crash(s"type mismatch implodeText: expected SText, got $v")
      }
      SText(Utf8.implode(ts.toImmArray))
    }
  }

  final case object SBAppendText extends SBuiltinPure(2) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SText =
      SText(getSText(args, 0) + getSText(args, 1))
  }

  private[this] def litToText(location: String, x: SValue): String =
    x match {
      case SBool(b) => b.toString
      case SInt64(i) => i.toString
      case STimestamp(t) => t.toString
      case SText(t) => t
      case SParty(p) => p
      case SUnit => s"<unit>"
      case SDate(date) => date.toString
      case SBigNumeric(x) => Numeric.toUnscaledString(x)
      case SNumeric(x) => Numeric.toUnscaledString(x)
      case STNat(n) => s"@$n"
      case _: SContractId | SToken | _: SAny | _: SEnum | _: SList | _: SMap | _: SOptional |
          _: SPAP | _: SRecord | _: SStruct | _: STypeRep | _: SVariant =>
        throw SErrorCrash(location, s"litToText: unexpected $x")
    }

  final case object SBToText extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SText =
      SText(litToText(NameOf.qualifiedNameOfCurrentFunc, args.get(0)))
  }

  final case object SBContractIdToText extends SBuiltin(1) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      val coid = getSContractId(args, 0).coid
      machine.ledgerMode match {
        case OffLedger =>
          Control.Value(SOptional(Some(SText(coid))))
        case _ =>
          Control.Value(SValue.SValue.None)
      }
    }
  }

  final case object SBPartyToQuotedText extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SText =
      SText(s"'${getSParty(args, 0): String}'")
  }

  final case object SBCodePointsToText extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SText = {
      val codePoints = getSList(args, 0).map(_.asInstanceOf[SInt64].value)
      Utf8.pack(codePoints.toImmArray) match {
        case Right(value) =>
          SText(value)
        case Left(cp) =>
          crash(s"invalid code point 0x${cp.toHexString}.")
      }
    }
  }

  final case object SBTextToParty extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SOptional = {
      Party.fromString(getSText(args, 0)) match {
        case Left(_) => SV.None
        case Right(p) => SOptional(Some(SParty(p)))
      }
    }
  }

  final case object SBTextToInt64 extends SBuiltinPure(1) {
    private val pattern = """[+-]?\d+""".r.pattern

    override private[speedy] def executePure(args: util.ArrayList[SValue]): SOptional = {
      val s = getSText(args, 0)
      if (pattern.matcher(s).matches())
        try {
          SOptional(Some(SInt64(java.lang.Long.parseLong(s))))
        } catch {
          case _: NumberFormatException =>
            SV.None
        }
      else
        SV.None
    }
  }

  // The specification of FromTextNumeric is lenient about the format of the string it should
  // accept and convert. In particular it should convert any string with an arbitrary number of
  // leading and trailing '0's as long as the corresponding number fits a Numeric without loss of
  // precision. We should take care not calling String to BigDecimal conversion on huge strings.
  final case object SBTextToNumeric extends SBuiltinPure(2) {
    private val validFormat =
      """([+-]?)0*(\d+)(\.(\d*[1-9]|0)0*)?""".r

    override private[speedy] def executePure(args: util.ArrayList[SValue]): SOptional = {
      val scale = getSTNat(args, 0)
      val string = getSText(args, 1)
      string match {
        case validFormat(signPart, intPart, _, decPartOrNull) =>
          val decPart = Option(decPartOrNull).filterNot(_ == "0").getOrElse("")
          // First, we count the number of significant digits to avoid the conversion attempts that
          // are doomed to failure.
          val significantIntDigits = if (intPart == "0") 0 else intPart.length
          val significantDecDigits = decPart.length
          if (
            significantIntDigits <= Numeric.maxPrecision - scale && significantDecDigits <= scale
          ) {
            // Then, we reconstruct the string dropping non significant '0's to avoid unnecessary and
            // potentially very costly String to BigDecimal conversions. Take for example the String
            // "1." followed by millions of '0's
            val newString = s"$signPart$intPart.${Option(decPartOrNull).getOrElse("")}"
            SOptional(Some(SNumeric(Numeric.assertFromBigDecimal(scale, BigDecimal(newString)))))
          } else {
            SV.None
          }
        case _ =>
          SV.None
      }
    }
  }

  final case object SBTextToCodePoints extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SList = {
      val string = getSText(args, 0)
      val codePoints = Utf8.unpack(string)
      SList(FrontStack.from(codePoints.map(SInt64)))
    }
  }

  final case object SBSHA256Text extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SText =
      SText(Utf8.sha256(getSText(args, 0)))
  }

  final case object SBFoldl extends SBuiltin(3) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      val func = args.get(0)
      val init = args.get(1)
      val list = getSList(args, 2)
      machine.pushKont(KFoldl(machine, func, list))
      Control.Value(init)
    }
  }

  // NOTE: Past implementations of `foldr` have used the semantics given by the
  // recursive definition
  // ```
  // foldr f z [] = z
  // foldr f z (x::xs) = f x (foldr f z xs)
  // ```
  // When the PAP for `f` expects at least two more arguments, this leads to the
  // expected right-to-left evaluation order. However, if the PAP `f` is missing
  // only one argument, the evaluation order suddenly changes. First, `f` is
  // applied to all the elements of `xs` in left-to-right order, then the
  // resulting list of PAPs is reduced from right-to-left by application, using
  // `z` as the initial value argument.
  //
  // For this reason, we need three different continuations for `foldr`:
  // 1. `KFoldr` is for the case where `f` expects at least two more arguments.
  // 2. `KFoldr1Map` is for the first mapping from left-to-right stage when `f`
  //    is missing only one argument.
  // 3. `KFoldr1Reduce` is for the second reduce from right-to-left stage when
  //    `f` is missing only one argument.
  //
  // We could have omitted the special casse for `f` missing only one argument,
  // if the semantics of `foldr` had been implemented as
  // ```
  // foldr f z [] = z
  // foldr f z (x:xs) = let y = foldr f z xs in f x y
  // ```
  // However, this would be a breaking change compared to the aforementioned
  // implementation of `foldr`.
  final case object SBFoldr extends SBuiltin(3) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      val func = args.get(0).asInstanceOf[SPAP]
      val init = args.get(1)
      val list = getSList(args, 2)
      if (func.arity - func.actuals.size >= 2) {
        val array = list.toImmArray
        machine.pushKont(KFoldr(machine, func, array, array.length))
        Control.Value(init)
      } else {
        val stack = list
        stack.pop match {
          case None =>
            Control.Value(init)
          case Some((head, tail)) =>
            machine.pushKont(KFoldr1Map(machine, func, tail, FrontStack.empty, init))
            machine.enterApplication(func, Array(SEValue(head)))
        }
      }
    }
  }

  final case object SBMapToList extends SBuiltinPure(1) {

    override private[speedy] def executePure(args: util.ArrayList[SValue]): SList =
      SValue.toList(getSMap(args, 0).entries)
  }

  final case object SBMapInsert extends SBuiltinPure(3) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SMap =
      getSMap(args, 2).insert(getSMapKey(args, 0), args.get(1))
  }

  final case object SBMapLookup extends SBuiltinPure(2) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SOptional =
      SOptional(getSMap(args, 1).entries.get(getSMapKey(args, 0)))
  }

  final case object SBMapDelete extends SBuiltinPure(2) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SMap =
      getSMap(args, 1).delete(getSMapKey(args, 0))
  }

  final case object SBMapKeys extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SList =
      SList(getSMap(args, 0).entries.keys.to(FrontStack))
  }

  final case object SBMapValues extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SList =
      SList(getSMap(args, 0).entries.values.to(FrontStack))
  }

  final case object SBMapSize extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SInt64 =
      SInt64(getSMap(args, 0).entries.size.toLong)
  }

  //
  // Conversions
  //

  final case object SBInt64ToNumeric extends SBuiltinArithmetic("INT64_TO_NUMERIC", 2) {
    override private[speedy] def compute(args: util.ArrayList[SValue]): Option[SNumeric] = {
      val scale = getSTNat(args, 0)
      val x = getSInt64(args, 1)
      Numeric.fromLong(scale, x).toOption.map(SNumeric(_))
    }
  }

  final case object SBNumericToInt64 extends SBuiltinArithmetic("NUMERIC_TO_INT64", 2) {
    override private[speedy] def compute(args: util.ArrayList[SValue]): Option[SInt64] = {
      val x = getSNumeric(args, 1)
      Numeric.toLong(x).toOption.map(SInt64)
    }
  }

  final case object SBDateToUnixDays extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SInt64 =
      SInt64(getSDate(args, 0).days.toLong)
  }

  final case object SBUnixDaysToDate extends SBuiltinArithmetic("UNIX_DAYS_TO_DATE", 1) {
    override private[speedy] def compute(args: util.ArrayList[SValue]): Option[SDate] = {
      val days = getSInt64(args, 0)
      Time.Date.asInt(days).flatMap(Time.Date.fromDaysSinceEpoch).toOption.map(SDate)
    }
  }

  final case object SBTimestampToUnixMicroseconds extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SInt64 =
      SInt64(getSTimestamp(args, 0).micros)
  }

  final case object SBUnixMicrosecondsToTimestamp
      extends SBuiltinArithmetic("UNIX_MICROSECONDS_TO_TIMESTAMP", 1) {
    override private[speedy] def compute(args: util.ArrayList[SValue]): Option[STimestamp] = {
      val micros = getSInt64(args, 0)
      Time.Timestamp.fromLong(micros).toOption.map(STimestamp)
    }
  }

  //
  // Equality and comparisons
  //
  final case object SBEqual extends SBuiltinPure(2) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SBool = {
      SBool(svalue.Equality.areEqual(args.get(0), args.get(1)))
    }
  }

  sealed abstract class SBCompare(pred: Int => Boolean) extends SBuiltinPure(2) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SBool = {
      SBool(pred(svalue.Ordering.compare(args.get(0), args.get(1))))
    }
  }

  final case object SBLess extends SBCompare(_ < 0)
  final case object SBLessEq extends SBCompare(_ <= 0)
  final case object SBGreater extends SBCompare(_ > 0)
  final case object SBGreaterEq extends SBCompare(_ >= 0)

  /** $consMany[n] :: a -> ... -> List a -> List a */
  final case class SBConsMany(n: Int) extends SBuiltinPure(1 + n) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SList =
      SList(args.view.slice(0, n).to(ImmArray) ++: getSList(args, n))
  }

  /** $cons :: a -> List a -> List a */
  final case object SBCons extends SBuiltinPure(2) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SList = {
      SList(args.get(0) +: getSList(args, 1))
    }
  }

  /** $some :: a -> Optional a */
  final case object SBSome extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SOptional = {
      SOptional(Some(args.get(0)))
    }
  }

  /** $rcon[R, fields] :: a -> b -> ... -> R */
  final case class SBRecCon(id: Identifier, fields: ImmArray[Name])
      extends SBuiltinPure(fields.length) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SRecord = {
      SRecord(id, fields, args)
    }
  }

  /** $rupd[R, field] :: R -> a -> R */
  final case class SBRecUpd(id: Identifier, field: Int) extends SBuiltinPure(2) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SRecord = {
      val record = getSRecord(args, 0)
      if (record.id != id) {
        crash(s"type mismatch on record update: expected $id, got record of type ${record.id}")
      }
      val values2 = record.values.clone.asInstanceOf[util.ArrayList[SValue]]
      discard(values2.set(field, args.get(1)))
      record.copy(values = values2)
    }
  }

  /** $rupdmulti[R, [field_1, ..., field_n]] :: R -> a_1 -> ... -> a_n -> R */
  final case class SBRecUpdMulti(id: Identifier, updateFields: ImmArray[Int])
      extends SBuiltinPure(1 + updateFields.length) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SRecord = {
      val record = getSRecord(args, 0)
      if (record.id != id) {
        crash(s"type mismatch on record update: expected $id, got record of type ${record.id}")
      }
      val values2 = record.values.clone.asInstanceOf[util.ArrayList[SValue]]
      var i = 0
      while (i < updateFields.length) {
        discard(values2.set(updateFields(i), args.get(i + 1)))
        i += 1
      }
      record.copy(values = values2)
    }
  }

  /** $rproj[R, field] :: R -> a */
  final case class SBRecProj(id: Identifier, field: Int) extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SValue =
      getSRecord(args, 0).values.get(field)
  }

  // SBStructCon sorts the field after evaluation of its arguments to preserve
  // evaluation order of unordered fields.
  /** $tcon[fields] :: a -> b -> ... -> Struct */
  final case class SBStructCon(inputFieldsOrder: Struct[Int])
      extends SBuiltinPure(inputFieldsOrder.size) {
    private[this] val fieldNames = inputFieldsOrder.mapValues(_ => ())
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SStruct = {
      val sortedFields = new util.ArrayList[SValue](inputFieldsOrder.size)
      inputFieldsOrder.values.foreach(i => sortedFields.add(args.get(i)))
      SStruct(fieldNames, sortedFields)
    }
  }

  /** $tproj[field] :: Struct -> a */
  final case class SBStructProj(field: Ast.FieldName) extends SBuiltinPure(1) {
    // The variable `fieldIndex` is used to cache the (logarithmic) evaluation
    // of `struct.fieldNames.indexOf(field)` at the first call in order to
    // avoid its reevaluations, hence obtaining an amortized constant
    // complexity.
    private[this] var fieldIndex = -1
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SValue = {
      val struct = getSStruct(args, 0)
      if (fieldIndex < 0) fieldIndex = struct.fieldNames.indexOf(field)
      struct.values.get(fieldIndex)
    }
  }

  /** $tupd[field] :: Struct -> a -> Struct */
  final case class SBStructUpd(field: Ast.FieldName) extends SBuiltinPure(2) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SStruct = {
      val struct = getSStruct(args, 0)
      val values = struct.values.clone.asInstanceOf[util.ArrayList[SValue]]
      discard(values.set(struct.fieldNames.indexOf(field), args.get(1)))
      struct.copy(values = values)
    }
  }

  /** $vcon[V, variant] :: a -> V */
  final case class SBVariantCon(id: Identifier, variant: Ast.VariantConName, constructorRank: Int)
      extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SVariant = {
      SVariant(id, variant, constructorRank, args.get(0))
    }
  }

  final object SBScaleBigNumeric extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SInt64 = {
      SInt64(getSBigNumeric(args, 0).scale().toLong)
    }
  }

  final object SBPrecisionBigNumeric extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SInt64 = {
      SInt64(getSBigNumeric(args, 0).precision().toLong)
    }
  }

  final object SBAddBigNumeric extends SBuiltinArithmetic("ADD_BIGNUMERIC", 2) {
    override private[speedy] def compute(args: util.ArrayList[SValue]): Option[SBigNumeric] = {
      val x = getSBigNumeric(args, 0)
      val y = getSBigNumeric(args, 1)
      SBigNumeric.fromBigDecimal(x add y).toOption
    }
  }

  final object SBSubBigNumeric extends SBuiltinArithmetic("SUB_BIGNUMERIC", 2) {
    override private[speedy] def compute(args: util.ArrayList[SValue]): Option[SBigNumeric] = {
      val x = getSBigNumeric(args, 0)
      val y = getSBigNumeric(args, 1)
      SBigNumeric.fromBigDecimal(x subtract y).toOption
    }
  }

  final object SBMulBigNumeric extends SBuiltinArithmetic("MUL_BIGNUMERIC", 2) {
    override private[speedy] def compute(args: util.ArrayList[SValue]): Option[SBigNumeric] = {
      val x = getSBigNumeric(args, 0)
      val y = getSBigNumeric(args, 1)
      SBigNumeric.fromBigDecimal(x multiply y).toOption
    }
  }

  final object SBDivBigNumeric extends SBuiltinArithmetic("DIV_BIGNUMERIC", 4) {
    override private[speedy] def compute(args: util.ArrayList[SValue]): Option[SBigNumeric] = {
      val unchekedScale = getSInt64(args, 0)
      val unchekedRoundingMode = getSInt64(args, 1)
      val x = getSBigNumeric(args, 2)
      val y = getSBigNumeric(args, 3)
      for {
        scale <- SBigNumeric.checkScale(unchekedScale).toOption
        roundingModeIndex <- scala.util.Try(Math.toIntExact(unchekedRoundingMode)).toOption
        roundingMode <- java.math.RoundingMode.values().lift(roundingModeIndex)
        uncheckedResult <- handleArithmeticException(x.divide(y, scale, roundingMode))
        result <- SBigNumeric.fromBigDecimal(uncheckedResult).toOption
      } yield result
    }
  }

  final object SBShiftRightBigNumeric extends SBuiltinArithmetic("SHIFT_RIGHT_BIGNUMERIC", 2) {
    override private[speedy] def compute(args: util.ArrayList[SValue]): Option[SBigNumeric] = {
      val shifting = getSInt64(args, 0)
      val x = getSBigNumeric(args, 1)
      if (x.signum() == 0)
        Some(SBigNumeric.Zero)
      else if (shifting.abs > SBigNumeric.MaxPrecision)
        None
      else
        SBigNumeric.fromBigDecimal(x.scaleByPowerOfTen(-shifting.toInt)).toOption
    }
  }

  final object SBNumericToBigNumeric extends SBuiltinPure(2) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SBigNumeric = {
      val x = getSNumeric(args, 1)
      SBigNumeric.fromNumeric(x)
    }
  }

  final object SBBigNumericToNumeric extends SBuiltinArithmetic("BIGNUMERIC_TO_NUMERIC", 2) {
    override private[speedy] def compute(args: util.ArrayList[SValue]): Option[SNumeric] = {
      val scale = getSTNat(args, 0)
      val x = getSBigNumeric(args, 1)
      Numeric.fromBigDecimal(scale, x).toOption.map(SNumeric(_))
    }
  }

  /** $checkPrecondition
    *    :: arg (template argument)
    *    -> Unit
    *    -> Bool (false if ensure failed)
    *    -> Unit
    */
  final case class SBCheckPrecond(templateId: TypeConName) extends SBuiltin(2) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      if (!getSBool(args, 1)) {
        Control.Error(
          IE.TemplatePreconditionViolated(
            templateId = templateId,
            optLocation = None,
            arg = args.get(0).toUnnormalizedValue,
          )
        )
      } else {
        Control.Value(SUnit)
      }
    }
  }

  /** $create
    *    :: Text (agreement text)
    *    -> CachedContract
    *    -> ContractId arg
    */
  final case object SBUCreate extends OnLedgerBuiltin(2) {
    override protected def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
        onLedger: OnLedger,
    ): Control = {
      val agreement = getSText(args, 0)
      val cached = extractCachedContract(machine, args.get(1))
      val createArgValue = machine.normValue(cached.templateId, cached.value)
      cached.key match {
        case Some(Node.KeyWithMaintainers(key, maintainers)) if maintainers.isEmpty =>
          Control.Error(
            IE.CreateEmptyContractKeyMaintainers(cached.templateId, createArgValue, key)
          )
        case _ =>
          onLedger.ptx
            .insertCreate(
              submissionTime = machine.submissionTime,
              templateId = cached.templateId,
              arg = createArgValue,
              agreementText = agreement,
              optLocation = machine.lastLocation,
              signatories = cached.signatories,
              stakeholders = cached.stakeholders,
              key = cached.key,
              version = machine.tmplId2TxVersion(cached.templateId),
            ) match {
            case Right((coid, newPtx)) =>
              onLedger.updateCachedContracts(coid, cached)
              onLedger.ptx = newPtx
              Control.Value(SContractId(coid))
            case Left((newPtx, err)) =>
              onLedger.ptx = newPtx // Seems wrong. But one test in ScriptService requires this.
              Control.Error(convTxError(err))
          }
      }
    }
  }

  /** $beginExercise
    *    :: arg                                           0 (choice argument)
    *    -> ContractId arg                                1 (contract to exercise)
    *    -> List Party                                    2 (actors)
    *    -> List Party                                    3 (signatories)
    *    -> List Party                                    4 (template observers)
    *    -> List Party                                    5 (choice controllers)
    *    -> List Party                                    6 (choice observers)
    *    -> Optional {key: key, maintainers: List Party}  7 (template key, if present)
    *    -> ()
    */
  final case class SBUBeginExercise(
      templateId: TypeConName,
      interfaceId: Option[TypeConName],
      choiceId: ChoiceName,
      consuming: Boolean,
      byKey: Boolean,
  ) extends OnLedgerBuiltin(4) {

    override protected def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
        onLedger: OnLedger,
    ): Control = {
      val chosenValue = machine.normValue(templateId, args.get(0))
      val coid = getSContractId(args, 1)
      val cached =
        onLedger.cachedContracts.getOrElse(
          coid,
          crash(s"Contract ${coid.coid} is missing from cache"),
        )
      val sigs = cached.signatories
      val templateObservers = cached.observers
      val ctrls = extractParties(NameOf.qualifiedNameOfCurrentFunc, args.get(2))
      onLedger.enforceChoiceControllersLimit(ctrls, coid, templateId, choiceId, chosenValue)
      val obsrs = extractParties(NameOf.qualifiedNameOfCurrentFunc, args.get(3))
      onLedger.enforceChoiceObserversLimit(obsrs, coid, templateId, choiceId, chosenValue)
      val mbKey = cached.key
      val templateVersion = machine.tmplId2TxVersion(templateId)
      val interfaceVersion = interfaceId.map(machine.tmplId2TxVersion)
      val exerciseVersion = interfaceVersion.fold(templateVersion)(_.max(templateVersion))

      onLedger.ptx
        .beginExercises(
          targetId = coid,
          templateId = templateId,
          interfaceId = interfaceId,
          choiceId = choiceId,
          optLocation = machine.lastLocation,
          consuming = consuming,
          actingParties = ctrls,
          signatories = sigs,
          stakeholders = sigs union templateObservers,
          choiceObservers = obsrs,
          mbKey = mbKey,
          byKey = byKey,
          chosenValue = chosenValue,
          version = exerciseVersion,
        ) match {
        case Right(ptx) =>
          onLedger.ptx = ptx
          Control.Value(SUnit)
        case Left(err) =>
          Control.Error(convTxError(err))
      }
    }
  }

  // SBCastAnyContract: ContractId templateId -> Any -> templateId
  final case class SBCastAnyContract(templateId: TypeConName) extends SBuiltin(2) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      def coid = getSContractId(args, 0)
      val (actualTemplateId, record) = getSAnyContract(args, 1)
      if (actualTemplateId != templateId) {
        Control.Error(IE.WronglyTypedContract(coid, templateId, actualTemplateId))
      } else {
        Control.Value(record)
      }
    }
  }

  private[this] def getInterfaceInstance(
      machine: Machine,
      interfaceId: TypeConName,
      templateId: TypeConName,
  ): Option[InterfaceInstanceDefRef] = {
    def mkRef(parent: TypeConName) =
      InterfaceInstanceDefRef(parent, interfaceId, templateId)

    List(mkRef(templateId), mkRef(interfaceId)) find { ref =>
      machine.compiledPackages.getDefinition(ref).nonEmpty
    }
  }

  private[this] def interfaceInstanceExists(
      machine: Machine,
      interfaceId: TypeConName,
      templateId: TypeConName,
  ): Boolean =
    getInterfaceInstance(machine, interfaceId, templateId).nonEmpty

  // SBCastAnyInterface: ContractId ifaceId -> Any -> ifaceId
  final case class SBCastAnyInterface(ifaceId: TypeConName) extends SBuiltin(2) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      def coid = getSContractId(args, 0)
      val (actualTmplId, _) = getSAnyContract(args, 1)
      if (!interfaceInstanceExists(machine, ifaceId, actualTmplId)) {
        Control.Error(IE.ContractDoesNotImplementInterface(ifaceId, coid, actualTmplId))
      } else {
        Control.Value(args.get(1))
      }
    }
  }

  /** $fetchAny[T]
    *    :: ContractId a
    *    -> Optional {key: key, maintainers: List Party} (template key, if present)
    *    -> a
    */

  final case object SBFetchAny extends OnLedgerBuiltin(2) {
    override protected def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
        onLedger: OnLedger,
    ): Control = {
      val coid = getSContractId(args, 0)
      onLedger.cachedContracts.get(coid) match {
        case Some(cached) =>
          onLedger.ptx.consumedByOrInactive(coid) match {
            case Some(Left(nid)) =>
              Control.Error(IE.ContractNotActive(coid, cached.templateId, nid))

            case Some(Right(())) =>
              Control.Error(IE.ContractNotFound(coid))

            case None =>
              Control.Value(cached.any)
          }

        case None =>
          def continue(coinst: V.ContractInstance): Control = {
            machine.pushKont(KCacheContract(machine, coid))
            val e = coinst match {
              case V.ContractInstance(actualTmplId, arg, _) =>
                SEApp(
                  // The call to ToCachedContractDefRef(actualTmplId) will query package
                  // of actualTmplId if not known.
                  SEVal(ToCachedContractDefRef(actualTmplId)),
                  Array(
                    SEImportValue(Ast.TTyCon(actualTmplId), arg),
                    SEValue(args.get(1)),
                  ),
                )
            }
            Control.Expression(e)
          }

          machine.disclosureTable.contractById.get(SContractId(coid)) match {
            case Some((templateId, arg)) =>
              val v = machine.normValue(templateId, arg)
              val coinst = V.ContractInstance(templateId, v, "")
              continue(coinst)

            case None =>
              Control.Question(
                SResultNeedContract(
                  coid,
                  onLedger.committers,
                  callback = { res =>
                    val control = continue(res)
                    machine.setControl(control)
                  },
                )
              )
          }
      }

    }
  }

  final case class SBApplyChoiceGuard(
      choiceName: ChoiceName,
      byInterface: Option[TypeConName],
  ) extends SBuiltin(3) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      val guard = args.get(0)
      val (templateId, record) = getSAnyContract(args, 1)
      val coid = getSContractId(args, 2)

      val e = SEApp(SEValue(guard), Array(SEValue(SAnyContract(templateId, record))))
      machine.pushKont(KCheckChoiceGuard(machine, coid, templateId, choiceName, byInterface))
      Control.Expression(e)
    }
  }

  final case object SBGuardConstTrue extends SBuiltinPure(1) {
    override private[speedy] def executePure(
        args: util.ArrayList[SValue]
    ): SBool = {
      discard(getSAnyContract(args, 0))
      SBool(true)
    }
  }

  final case class SBGuardMatchTemplateId(
      expectedTmplId: TypeConName
  ) extends SBuiltin(2) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      val contractId = getSContractId(args, 0)
      val (actualTmplId, record @ _) = getSAnyContract(args, 1)
      if (actualTmplId != expectedTmplId) {
        Control.Error(IE.WronglyTypedContract(contractId, expectedTmplId, actualTmplId))
      } else {
        Control.Value(SBool(true))
      }
    }
  }

  final case class SBGuardRequiredInterfaceId(
      requiredIfaceId: TypeConName,
      requiringIfaceId: TypeConName,
  ) extends SBuiltin(2) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      val contractId = getSContractId(args, 0)
      val (actualTmplId, record @ _) = getSAnyContract(args, 1)
      if (!interfaceInstanceExists(machine, requiringIfaceId, actualTmplId)) {
        Control.Error(
          IE.ContractDoesNotImplementRequiringInterface(
            requiringIfaceId,
            requiredIfaceId,
            contractId,
            actualTmplId,
          )
        )
      } else {
        Control.Value(SBool(true))
      }
    }
  }

  final case class SBResolveSBUBeginExercise(
      interfaceId: TypeConName,
      choiceName: ChoiceName,
      consuming: Boolean,
      byKey: Boolean,
  ) extends SBuiltin(1) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      val e = SEBuiltin(
        SBUBeginExercise(
          templateId = getSAnyContract(args, 0)._1,
          interfaceId = Some(interfaceId),
          choiceId = choiceName,
          consuming = consuming,
          byKey = false,
        )
      )
      Control.Expression(e)
    }
  }

  final case object SBResolveSBUInsertFetchNode extends SBuiltin(1) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      val e = SEBuiltin(
        SBUInsertFetchNode(getSAnyContract(args, 0)._1, byKey = false)
      )
      Control.Expression(e)
    }
  }

  // Return a definition matching the templateId of a given payload
  sealed class SBResolveVirtual(toDef: Ref.Identifier => SDefinitionRef) extends SBuiltin(1) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      val (ty, record) = getSAnyContract(args, 0)
      val e = SEApp(SEVal(toDef(ty)), Array(SEValue(record)))
      Control.Expression(e)
    }
  }

  final case object SBResolveCreate extends SBResolveVirtual(CreateDefRef)

  final case class SBSignatoryInterface(ifaceId: TypeConName)
      extends SBResolveVirtual(SignatoriesDefRef)

  final case class SBObserverInterface(ifaceId: TypeConName)
      extends SBResolveVirtual(ObserversDefRef)

  // This wraps a contract record into an SAny where the type argument corresponds to
  // the record's templateId.
  final case class SBToAnyContract(
      tplId: TypeConName
  ) extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SAny = {
      SAnyContract(tplId, getSRecord(args, 0))
    }
  }

  // Convert an interface to a given template type if possible. Since interfaces are represented
  // by an SAny wrapping the underlying template, we need to check that the SAny type constructor
  // matches the template type, and then return the SAny internal value.
  final case class SBFromInterface(
      tplId: TypeConName
  ) extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SOptional = {
      val (tyCon, record) = getSAnyContract(args, 0)
      if (tplId == tyCon) {
        SOptional(Some(record))
      } else {
        SOptional(None)
      }
    }
  }

  // Convert an interface to a given template type if possible. Since interfaces are represented
  // by an SAny wrapping the underlying template, we need to check that the SAny type constructor
  // matches the template type, and then return the SAny internal value.
  final case class SBUnsafeFromInterface(
      tplId: TypeConName
  ) extends SBuiltin(2) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      val coid = getSContractId(args, 0)
      val (tyCon, record) = getSAnyContract(args, 1)
      if (tplId == tyCon) {
        Control.Value(record)
      } else {
        Control.Error(IE.WronglyTypedContract(coid, tplId, tyCon))
      }
    }
  }

  // Convert an interface value to another interface `requiringIfaceId`, if
  // the underlying template implements `requiringIfaceId`. Else return `None`.
  final case class SBFromRequiredInterface(
      requiringIfaceId: TypeConName
  ) extends SBuiltin(1) {

    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ) = {
      val (actualTemplateId, record) = getSAnyContract(args, 0)
      val v =
        if (interfaceInstanceExists(machine, requiringIfaceId, actualTemplateId))
          SOptional(Some(SAnyContract(actualTemplateId, record)))
        else
          SOptional(None)
      Control.Value(v)
    }
  }

  // Convert an interface `requiredIfaceId`  to another interface `requiringIfaceId`, if
  // the underlying template implements `requiringIfaceId`. Else throw a fatal
  // `ContractDoesNotImplementRequiringInterface` exception.
  final case class SBUnsafeFromRequiredInterface(
      requiredIfaceId: TypeConName,
      requiringIfaceId: TypeConName,
  ) extends SBuiltin(2) {

    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      val coid = getSContractId(args, 0)
      val (actualTmplId, record) = getSAnyContract(args, 1)
      if (!interfaceInstanceExists(machine, requiringIfaceId, actualTmplId)) {
        Control.Error(
          IE.ContractDoesNotImplementRequiringInterface(
            requiringIfaceId,
            requiredIfaceId,
            coid,
            actualTmplId,
          )
        )
      } else {
        Control.Value(SAnyContract(actualTmplId, record))
      }
    }
  }

  final case class SBCallInterface(
      ifaceId: TypeConName,
      methodName: MethodName,
  ) extends SBuiltin(1) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      val (templateId, record) = getSAnyContract(args, 0)
      val ref = getInterfaceInstance(machine, ifaceId, templateId).fold(
        crash(
          s"Attempted to call interface ${ifaceId} method ${methodName} on a wrapped " +
            s"template of type ${ifaceId}, but there's no matching interface instance."
        )
      )(iiRef => InterfaceInstanceMethodDefRef(iiRef, methodName))
      val e = SEApp(SEVal(ref), Array(SEValue(record)))
      Control.Expression(e)
    }
  }

  final case class SBViewInterface(
      ifaceId: TypeConName
  ) extends SBuiltin(1) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      val (templateId, record) = getSAnyContract(args, 0)
      val ref = getInterfaceInstance(machine, ifaceId, templateId).fold(
        crash(
          s"Attempted to call view for interface ${ifaceId} on a wrapped " +
            s"template of type ${ifaceId}, but there's no matching interface instance."
        )
      )(iiRef => InterfaceInstanceViewDefRef(iiRef))
      val e = SEApp(SEVal(ref), Array(SEValue(record)))
      Control.Expression(e)
    }
  }

  /** $insertFetch[tid]
    *    :: ContractId a
    *    -> List Party    (signatories)
    *    -> List Party    (observers)
    *    -> Optional {key: key, maintainers: List Party}  (template key, if present)
    *    -> ()
    */
  final case class SBUInsertFetchNode(
      templateId: TypeConName,
      byKey: Boolean,
  ) extends OnLedgerBuiltin(1) {
    override protected def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
        onLedger: OnLedger,
    ): Control = {
      val coid = getSContractId(args, 0)
      val cached =
        onLedger.cachedContracts.getOrElse(
          coid,
          crash(s"Contract ${coid.coid} is missing from cache"),
        )
      val signatories = cached.signatories
      val observers = cached.observers
      val key = cached.key
      onLedger.ptx.insertFetch(
        coid = coid,
        templateId = templateId,
        optLocation = machine.lastLocation,
        signatories = signatories,
        observers = observers,
        key = key,
        byKey = byKey,
        version = machine.tmplId2TxVersion(templateId),
      ) match {
        case Right(ptx) =>
          onLedger.ptx = ptx
          Control.Value(SUnit)
        case Left(err) =>
          Control.Error(convTxError(err))
      }
    }
  }

  /** $insertLookup[T]
    *    :: { key : key, maintainers: List Party}
    *    -> Maybe (ContractId T)
    *    -> ()
    */
  final case class SBUInsertLookupNode(templateId: TypeConName) extends OnLedgerBuiltin(2) {
    override protected def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
        onLedger: OnLedger,
    ): Control = {
      val keyWithMaintainers =
        extractKeyWithMaintainers(
          machine,
          templateId,
          NameOf.qualifiedNameOfCurrentFunc,
          args.get(0),
        )
      val mbCoid = args.get(1) match {
        case SOptional(mb) =>
          mb.map {
            case SContractId(coid) => coid
            case _ => crash(s"Non contract id value when inserting lookup node")
          }
        case _ => crash(s"Non option value when inserting lookup node")
      }
      onLedger.ptx.insertLookup(
        templateId = templateId,
        optLocation = machine.lastLocation,
        key = Node.KeyWithMaintainers(
          key = keyWithMaintainers.key,
          maintainers = keyWithMaintainers.maintainers,
        ),
        result = mbCoid,
        version = machine.tmplId2TxVersion(templateId),
      ) match {
        case Right(ptx) =>
          onLedger.ptx = ptx
          Control.Value(SUnit)
        case Left(err) =>
          Control.Error(convTxError(err))
      }
    }
  }

  private[this] abstract class KeyOperation {
    val templateId: TypeConName

    // Callback from the engine returned NotFound
    def handleKeyFound(machine: Machine, cid: V.ContractId): Control
    // We already saw this key, but it was undefined or was archived
    def handleKeyNotFound(machine: Machine, gkey: GlobalKey): (Control, Boolean)

    final def handleKnownInputKey(
        machine: Machine,
        gkey: GlobalKey,
        keyMapping: ContractStateMachine.KeyMapping,
    ): Control =
      keyMapping match {
        case ContractStateMachine.KeyActive(cid) =>
          handleKeyFound(machine, cid)
        case ContractStateMachine.KeyInactive =>
          val (control, _) = handleKeyNotFound(machine, gkey)
          control
      }
  }

  private[this] object KeyOperation {
    final class Fetch(override val templateId: TypeConName) extends KeyOperation {
      override def handleKeyFound(machine: Machine, cid: V.ContractId): Control = {
        Control.Value(SContractId(cid))
      }
      override def handleKeyNotFound(machine: Machine, gkey: GlobalKey): (Control, Boolean) = {
        (Control.Error(IE.ContractKeyNotFound(gkey)), false)
      }
    }

    final class Lookup(override val templateId: TypeConName) extends KeyOperation {
      override def handleKeyFound(machine: Machine, cid: V.ContractId): Control = {
        Control.Value(SOptional(Some(SContractId(cid))))
      }
      override def handleKeyNotFound(machine: Machine, key: GlobalKey): (Control, Boolean) = {
        (Control.Value(SValue.SValue.None), true)
      }
    }
  }

  private[speedy] sealed abstract class SBUKeyBuiltin(
      operation: KeyOperation
  ) extends OnLedgerBuiltin(1) {

    final override def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
        onLedger: OnLedger,
    ): Control = {
      val skey = args.get(0)
      val keyWithMaintainers =
        extractKeyWithMaintainers(
          machine,
          operation.templateId,
          NameOf.qualifiedNameOfCurrentFunc,
          skey,
        )

      if (keyWithMaintainers.maintainers.isEmpty) {
        Control.Error(
          IE.FetchEmptyContractKeyMaintainers(operation.templateId, keyWithMaintainers.key)
        )
      } else {
        val gkey = GlobalKey(operation.templateId, keyWithMaintainers.key)

        onLedger.ptx.contractState.resolveKey(gkey) match {
          case Right((keyMapping, next)) =>
            onLedger.ptx = onLedger.ptx.copy(contractState = next)
            keyMapping match {
              case ContractStateMachine.KeyActive(coid) =>
                machine.checkKeyVisibility(onLedger, gkey, coid, operation.handleKeyFound)

              case ContractStateMachine.KeyInactive =>
                operation.handleKnownInputKey(machine, gkey, keyMapping)
            }

          case Left(handle) =>
            def continue = { result =>
              val (keyMapping, next) = handle(result)
              onLedger.ptx = onLedger.ptx.copy(contractState = next)
              keyMapping match {
                case ContractStateMachine.KeyActive(coid) =>
                  // We do not call directly machine.checkKeyVisibility as it may throw an SError,
                  // and such error cannot be throw inside a ledger-question continuation.
                  machine.pushKont(
                    KCheckKeyVisibility(machine, gkey, coid, operation.handleKeyFound)
                  )
                  if (onLedger.cachedContracts.contains(coid)) {
                    (Control.Value(SUnit), true)
                  } else {
                    // SBFetchAny will populate onLedger.cachedContracts with the contract pointed by coid
                    val e = SBFetchAny(SEValue(SContractId(coid)), SBSome(SEValue(skey)))
                    (Control.Expression(e), true)
                  }

                case ContractStateMachine.KeyInactive =>
                  operation.handleKeyNotFound(machine, gkey)
              }
            }: Option[V.ContractId] => (Control, Boolean)

            // TODO (drsk) validate key hash. https://github.com/digital-asset/daml/issues/13897
            machine.disclosureTable.contractIdByKey.get(gkey.hash) match {
              case Some(coid) =>
                machine.disclosureTable.contractById.get(coid) match {
                  case Some((actualTemplateId, _)) if actualTemplateId == operation.templateId =>
                    val vcoid = coid.value
                    continue(Some(vcoid))._1

                  case Some((actualTemplateId, _)) =>
                    Control.Error(
                      InconsistentDisclosureTable.IncorrectlyTypedContract(
                        coid.value,
                        operation.templateId,
                        actualTemplateId,
                      )
                    )

                  case None =>
                    crash(
                      s"Disclosure table is in an inconsistent state: unable to locate the contract ${coid.value} even though we know its key hash ${gkey.hash}"
                    )
                }

              case None =>
                Control.Question(
                  SResultNeedKey(
                    GlobalKeyWithMaintainers(gkey, keyWithMaintainers.maintainers),
                    onLedger.committers,
                    callback = { res =>
                      val (control, bool) = continue(res)
                      machine.setControl(control)
                      bool
                    },
                  )
                )
            }
        }
      }
    }
  }

  /** $fetchKey[T]
    *   :: { key: key, maintainers: List Party }
    *   -> ContractId T
    */
  final case class SBUFetchKey(templateId: TypeConName)
      extends SBUKeyBuiltin(new KeyOperation.Fetch(templateId))

  /** $lookupKey[T]
    *   :: { key: key, maintainers: List Party }
    *   -> Maybe (ContractId T)
    */
  final case class SBULookupKey(templateId: TypeConName)
      extends SBUKeyBuiltin(new KeyOperation.Lookup(templateId))

  /** $getTime :: Token -> Timestamp */
  final case object SBGetTime extends SBuiltin(1) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      checkToken(args, 0)
      // $ugettime :: Token -> Timestamp
      machine.ledgerMode match {
        case onLedger: OnLedger =>
          onLedger.dependsOnTime = true
        case OffLedger =>
      }
      Control.Question(
        SResultNeedTime { timestamp =>
          machine.setControl(Control.Value(STimestamp(timestamp)))
        }
      )
    }
  }

  final case class SBSSubmit(optLocation: Option[Location], mustFail: Boolean) extends SBuiltin(3) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      checkToken(args, 2)
      Control.Question(
        SResultScenarioSubmit(
          committers = extractParties(NameOf.qualifiedNameOfCurrentFunc, args.get(0)),
          commands = args.get(1),
          location = optLocation,
          mustFail = mustFail,
          callback = { newValue =>
            machine.setControl(Control.Value(newValue))
          },
        )
      )
    }
  }

  /** $pure :: a -> Token -> a */
  final case object SBSPure extends SBuiltin(2) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      checkToken(args, 1)
      Control.Value(args.get(0))
    }
  }

  /** $pass :: Int64 -> Token -> Timestamp */
  final case object SBSPass extends SBuiltin(2) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      checkToken(args, 1)
      val relTime = getSInt64(args, 0)
      Control.Question(
        SResultScenarioPassTime(
          relTime,
          callback = { timestamp =>
            machine.setControl(Control.Value(STimestamp(timestamp)))
          },
        )
      )
    }
  }

  /** $getParty :: Text -> Token -> Party */
  final case object SBSGetParty extends SBuiltin(2) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      checkToken(args, 1)
      val name = getSText(args, 0)
      Control.Question(
        SResultScenarioGetParty(
          name,
          callback = { party =>
            machine.setControl(Control.Value(SParty(party)))
          },
        )
      )
    }
  }

  /** $trace :: Text -> a -> a */
  final case object SBTrace extends SBuiltin(2) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      val message = getSText(args, 0)
      machine.traceLog.add(message, machine.lastLocation)(machine.loggingContext)
      Control.Value(args.get(1))
    }
  }

  /** $error :: Text -> a */
  final case object SBError extends SBuiltin(1) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      Control.Error(IE.UserError(getSText(args, 0)))
    }
  }

  /** $throw :: AnyException -> a */
  final case object SBThrow extends SBuiltin(1) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      val excep = getSAny(args, 0)
      unwindToHandler(machine, excep)
    }
  }

  /** $try-handler :: Optional (Token -> a) -> AnyException -> Token -> a (or re-throw) */
  final case object SBTryHandler extends SBuiltin(3) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      val opt = getSOptional(args, 0)
      val excep = getSAny(args, 1)
      checkToken(args, 2)
      opt match {
        case None =>
          unwindToHandler(machine, excep) // re-throw
        case Some(handler) =>
          machine.enterApplication(handler, Array(SEValue(SToken)))
      }
    }
  }

  /** $any-exception-message :: AnyException -> Text */
  final case object SBAnyExceptionMessage extends SBuiltin(1) {
    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      val exception = getSAnyException(args, 0)
      exception.id match {
        case ValueArithmeticError.tyCon =>
          Control.Value(exception.values.get(0))
        case tyCon =>
          val e = SEApp(SEVal(ExceptionMessageDefRef(tyCon)), Array(SEValue(exception)))
          Control.Expression(e)
      }
    }
  }

  /** $to_any
    *    :: t
    *    -> Any (where t = ty)
    */
  final case class SBToAny(ty: Ast.Type) extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SAny = {
      SAny(ty, args.get(0))
    }
  }

  /** $from_any
    *    :: Any
    *    -> Optional t (where t = expectedType)
    */
  final case class SBFromAny(expectedTy: Ast.Type) extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SOptional = {
      val any = getSAny(args, 0)
      if (any.ty == expectedTy) SOptional(Some(any.value)) else SValue.SValue.None
    }
  }

  /** $interface_template_type_rep
    *    :: t
    *    -> TypeRep (where t = TTyCon(_))
    */
  final case class SBInterfaceTemplateTypeRep(tycon: TypeConName) extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): STypeRep = {
      val (tyCon, _) = getSAnyContract(args, 0)
      STypeRep(Ast.TTyCon(tyCon))
    }
  }

  /** $type_rep_ty_con_name
    *    :: TypeRep
    *    -> Optional Text
    */
  final case object SBTypeRepTyConName extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SOptional =
      getSTypeRep(args, 0) match {
        case Ast.TTyCon(name) => SOptional(Some(SText(name.toString)))
        case _ => SOptional(None)
      }
  }

  // Unstable text primitives.

  /** $text_to_upper :: Text -> Text */
  case object SBTextToUpper extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SText = {
      val t = getSText(args, 0)
      // TODO [FM]: replace with ASCII-specific function, or not
      SText(t.toUpperCase(util.Locale.ROOT))
    }
  }

  /** $text_to_lower :: Text -> Text */
  case object SBTextToLower extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SText = {
      val t = getSText(args, 0)
      // TODO [FM]: replace with ASCII-specific function, or not
      SText(t.toLowerCase(util.Locale.ROOT))
    }
  }

  /** $text_slice :: Int -> Int -> Text -> Text */
  case object SBTextSlice extends SBuiltinPure(3) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SText = {
      val from = getSInt64(args, 0)
      val to = getSInt64(args, 1)
      val t = getSText(args, 2)
      val length = t.codePointCount(0, t.length).toLong
      if (to <= 0 || from >= length || to <= from) {
        SText("")
      } else {
        val rfrom = from.max(0).toInt
        val rto = to.min(length).toInt
        // NOTE [FM]: We use toInt only after ensuring the indices are
        // between 0 and length, inclusive. Calling toInt prematurely
        // would mean dropping the high order bits indiscriminitely,
        // so for instance (0x100000000L).toInt == 0, resulting in an
        // empty string below even though `to` was larger than length.
        val ifrom = t.offsetByCodePoints(0, rfrom)
        val ito = t.offsetByCodePoints(ifrom, rto - rfrom)
        SText(t.slice(ifrom, ito))
      }
    }
  }

  /** $text_slice_index :: Text -> Text -> Optional Int */
  case object SBTextSliceIndex extends SBuiltinPure(2) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SOptional = {
      val slice = getSText(args, 0)
      val t = getSText(args, 1)
      val n = t.indexOfSlice(slice) // n is -1 if slice is not found.
      if (n < 0) {
        SOptional(None)
      } else {
        val rn = t.codePointCount(0, n).toLong // we want to return the number of codepoints!
        SOptional(Some(SInt64(rn)))
      }
    }
  }

  /** $text_contains_only :: Text -> Text -> Bool */
  case object SBTextContainsOnly extends SBuiltinPure(2) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SBool = {
      val alphabet = getSText(args, 0)
      val t = getSText(args, 1)
      val alphabetSet = alphabet.codePoints().iterator().asScala.toSet
      val result = t.codePoints().iterator().asScala.forall(alphabetSet.contains)
      SBool(result)
    }
  }

  /** $text_replicate :: Int -> Text -> Text */
  case object SBTextReplicate extends SBuiltinPure(2) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SText = {
      val n = getSInt64(args, 0)
      val t = getSText(args, 1)
      if (n < 0) {
        SText("")
      } else {
        val rn = n.min(Int.MaxValue.toLong).toInt
        SText(t * rn)
      }
    }
  }

  /** $text_split_on :: Text -> Text -> List Text */
  case object SBTextSplitOn extends SBuiltinPure(2) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SList = {
      val pattern = getSText(args, 0)
      val t = getSText(args, 1)
      val xs =
        // Java will produce a two-element list for this with the second
        // element being the empty string.
        if (pattern.isEmpty) {
          FrontStack(SText(t))
        } else {
          // We do not want to do a regex match so we use Pattern.quote
          // and we want to keep empty strings, so we use -1 as the second argument.
          t.split(Pattern.quote(pattern), -1).iterator.map(SText).to(FrontStack)
        }
      SList(xs)
    }
  }

  /** $text_intercalate :: Text -> List Text -> Text */
  final case object SBTextIntercalate extends SBuiltinPure(2) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SText = {
      val sep = getSText(args, 0)
      val vs = getSList(args, 1)
      val xs = vs.map {
        case SText(t) => t
        case x => crash(s"type mismatch SBTextIntercalate, expected Text in list, got $x")
      }
      SText(xs.iterator.mkString(sep))
    }
  }

  /** EQUAL_LIST :: (a -> a -> Bool) -> [a] -> [a] -> Bool */
  final case object SBEqualList extends SBuiltin(3) {

    private val equalListBody: SExpr =
      SECaseAtomic( // case xs of
        SELocA(1),
        Array(
          SCaseAlt(
            SCPNil, // nil ->
            SECaseAtomic( // case ys of
              SELocA(2),
              Array(
                SCaseAlt(SCPNil, SEValue.True), // nil -> True
                SCaseAlt(SCPDefault, SEValue.False),
              ),
            ), // default -> False
          ),
          SCaseAlt( // cons x xss ->
            SCPCons,
            SECaseAtomic( // case ys of
              SELocA(2),
              Array(
                SCaseAlt(SCPNil, SEValue.False), // nil -> False
                SCaseAlt( // cons y yss ->
                  SCPCons,
                  SELet1( // let sub = (f y x) in
                    SEAppAtomicGeneral(
                      SELocA(0), // f
                      Array(
                        SELocS(2), // y
                        SELocS(4),
                      ),
                    ), // x
                    SECaseAtomic( // case (f y x) of
                      SELocS(1),
                      Array(
                        SCaseAlt(
                          SCPPrimCon(Ast.PCTrue), // True ->
                          SEAppAtomicGeneral(
                            SEBuiltin(SBEqualList), // single recursive occurrence
                            Array(
                              SELocA(0), // f
                              SELocS(2), // yss
                              SELocS(4),
                            ),
                          ), // xss
                        ),
                        SCaseAlt(SCPPrimCon(Ast.PCFalse), SEValue.False), // False -> False
                      ),
                    ),
                  ),
                ),
              ),
            ),
          ),
        ),
      )

    private val closure: SValue = {
      val frame = Array.ofDim[SValue](0) // no free vars
      val arity = 3
      SPAP(PClosure(Profile.LabelUnset, equalListBody, frame), ArrayList.empty, arity)
    }

    override private[speedy] def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Control = {
      val f = args.get(0)
      val xs = args.get(1)
      val ys = args.get(2)
      machine.enterApplication(
        closure,
        Array(SEValue(f), SEValue(xs), SEValue(ys)),
      )
    }

  }

  object SBExperimental {

    private object SBExperimentalAnswer extends SBuiltin(1) {
      override private[speedy] def execute(
          args: util.ArrayList[SValue],
          machine: Machine,
      ): Control = {
        Control.Value(SInt64(42L))
      }
    }

    // TODO: move this into the speedy compiler code
    private val mapping: Map[String, compileTime.SExpr] =
      List(
        "ANSWER" -> SBExperimentalAnswer
      ).view.map { case (name, builtin) => name -> compileTime.SEBuiltin(builtin) }.toMap

    def apply(name: String): compileTime.SExpr =
      mapping.getOrElse(
        name,
        SBError(compileTime.SEValue(SText(s"experimental $name not supported."))),
      )

  }

  private[speedy] def convTxError(err: Tx.TransactionError): interpretation.Error = {
    err match {
      case Tx.AuthFailureDuringExecution(nid, fa) =>
        IE.FailedAuthorization(nid, fa)
      case Tx.DuplicateContractKey(key) =>
        IE.DuplicateContractKey(key)
    }
  }

  private[this] def extractParties(where: String, v: SValue): TreeSet[Party] =
    v match {
      case SList(vs) =>
        TreeSet.empty(Party.ordering) ++ vs.iterator.map {
          case SParty(p) => p
          case x =>
            throw SErrorCrash(where, s"non-party value in list: $x")
        }
      case SParty(p) =>
        TreeSet(p)(Party.ordering)
      case _ =>
        throw SErrorCrash(where, s"value not a list of parties or party: $v")
    }

  private[this] val keyWithMaintainersStructFields: Struct[Unit] =
    Struct.assertFromNameSeq(List(Ast.keyFieldName, Ast.maintainersFieldName))

  private[this] val keyIdx = keyWithMaintainersStructFields.indexOf(Ast.keyFieldName)
  private[this] val maintainerIdx = keyWithMaintainersStructFields.indexOf(Ast.maintainersFieldName)

  private[this] def extractKeyWithMaintainers(
      machine: Machine,
      templateId: TypeConName,
      location: String,
      v: SValue,
  ): Node.KeyWithMaintainers =
    v match {
      case SStruct(_, vals) =>
        val key = machine.normValue(templateId, vals.get(keyIdx))
        key.foreachCid(_ => throw SErrorDamlException(IE.ContractIdInContractKey(key)))
        Node.KeyWithMaintainers(
          key = key,
          maintainers = extractParties(NameOf.qualifiedNameOfCurrentFunc, vals.get(maintainerIdx)),
        )
      case _ => throw SErrorCrash(location, s"Invalid key with maintainers: $v")
    }

  private[this] def extractOptionalKeyWithMaintainers(
      machine: Machine,
      templateId: TypeConName,
      where: String,
      optKey: SValue,
  ): Option[Node.KeyWithMaintainers] =
    optKey match {
      case SOptional(mbKey) => mbKey.map(extractKeyWithMaintainers(machine, templateId, where, _))
      case v => throw SErrorCrash(where, s"Expected optional key with maintainers, got: $v")
    }

  private[this] val cachedContractFieldNames =
    List("type", "value", "signatories", "observers", "mbKey").map(Ref.Name.assertFromString)

  private[this] val cachedContractStruct =
    Struct.assertFromSeq(cachedContractFieldNames.zipWithIndex)

  private[this] val List(
    cachedContractTypeFieldIdx,
    cachedContractArgIdx,
    cachedContractSignatoriesIdx,
    cachedContractObserversIdx,
    cachedContractKeyIdx,
  ) = cachedContractFieldNames.map(cachedContractStruct.indexOf)

  private[speedy] val SBuildCachedContract =
    SBuiltin.SBStructCon(cachedContractStruct)

  private[speedy] def extractCachedContract(
      machine: Machine,
      v: SValue,
  ): CachedContract =
    v match {
      case SStruct(_, vals) if vals.size == cachedContractStruct.size =>
        val templateId = vals.get(cachedContractTypeFieldIdx) match {
          case STypeRep(Ast.TTyCon(tycon)) => tycon
          case _ =>
            throw SErrorCrash(NameOf.qualifiedNameOfCurrentFunc, s"Invalid cached contract: $v")
        }
        CachedContract(
          templateId = templateId,
          value = vals.get(cachedContractArgIdx),
          signatories = extractParties(
            NameOf.qualifiedNameOfCurrentFunc,
            vals.get(cachedContractSignatoriesIdx),
          ),
          observers =
            extractParties(NameOf.qualifiedNameOfCurrentFunc, vals.get(cachedContractObserversIdx)),
          key = extractOptionalKeyWithMaintainers(
            machine,
            templateId,
            NameOf.qualifiedNameOfCurrentFunc,
            vals.get(cachedContractKeyIdx),
          ),
        )
      case _ =>
        throw SErrorCrash(NameOf.qualifiedNameOfCurrentFunc, s"Invalid cached contract: $v")
    }
}
