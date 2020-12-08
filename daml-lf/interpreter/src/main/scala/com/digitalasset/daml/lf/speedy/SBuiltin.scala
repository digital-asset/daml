// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import java.util
import java.util.regex.Pattern

import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.data.Numeric.Scale
import com.daml.lf.language.Ast
import com.daml.lf.language.Ast.{keyFieldName, maintainersFieldName}
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.Speedy._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SValue.{SValue => SV}
import com.daml.lf.transaction.{Transaction => Tx}
import com.daml.lf.value.{Value => V}
import com.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers, Node}

import scala.collection.JavaConverters._
import scala.collection.immutable.TreeSet

/**
  Speedy builtins are stratified into two layers:
  Parent: `SBuiltin`, (which are effectful), and child: `SBuiltinPure` (which are pure).

  Effectful builtin functions may raise `SpeedyHungry` exceptions or change machine state.
  Pure builtins can be treated specially because their evaluation is immediate.
  This fact is used by the execution of the ANF expression form: `SELet1Builtin`.

  Most builtins are pure, and so they extend `SBuiltinPure`
  */
private[speedy] sealed abstract class SBuiltin(val arity: Int) {
  // Helper for constructing expressions applying this builtin.
  // E.g. SBCons(SEVar(1), SEVar(2))
  private[speedy] def apply(args: SExpr*): SExpr =
    SEApp(SEBuiltin(this), args.toArray)

  /** Execute the builtin with 'arity' number of arguments in 'args'.
    * Updates the machine state accordingly. */
  private[speedy] def execute(args: util.ArrayList[SValue], machine: Machine): Unit
}

private[speedy] sealed abstract class SBuiltinPure(val arity1: Int) extends SBuiltin(arity1) {

  override private[speedy] final def execute(
      args: util.ArrayList[SValue],
      machine: Machine,
  ): Unit = {
    machine.returnValue = executePure(args)
  }

  /** Execute the (pure) builtin with 'arity' number of arguments in 'args'.
    Returns the resulting value */
  private[speedy] def executePure(args: util.ArrayList[SValue]): SValue
}

private[speedy] sealed abstract class OnLedgerBuiltin(arity: Int)
    extends SBuiltin(arity)
    with Product {

  protected def execute(
      args: util.ArrayList[SValue],
      machine: Machine,
      onLedger: OnLedger
  ): Unit

  final override def execute(args: util.ArrayList[SValue], machine: Machine): Unit =
    machine.withOnLedger(productPrefix)(execute(args, machine, _))
}

private[lf] object SBuiltin {

  //
  // Arithmetic
  //

  private[this] def add(x: Long, y: Long): Long =
    try {
      Math.addExact(x, y)
    } catch {
      case _: ArithmeticException =>
        throw DamlEArithmeticError(s"Int64 overflow when adding $y to $x.")
    }

  private[this] def div(x: Long, y: Long): Long =
    if (y == 0)
      throw DamlEArithmeticError(s"Attempt to divide $x by 0.")
    else if (x == Long.MinValue && y == -1)
      throw DamlEArithmeticError(s"Int64 overflow when dividing $x by $y.")
    else
      x / y

  private[this] def mult(x: Long, y: Long): Long =
    try {
      Math.multiplyExact(x, y)
    } catch {
      case _: ArithmeticException =>
        throw DamlEArithmeticError(s"Int64 overflow when multiplying $x by $y.")
    }

  private[this] def sub(x: Long, y: Long): Long =
    try {
      Math.subtractExact(x, y)
    } catch {
      case _: ArithmeticException =>
        throw DamlEArithmeticError(s"Int64 overflow when subtracting $y from $x.")
    }

  private[this] def mod(x: Long, y: Long): Long =
    if (y == 0)
      throw DamlEArithmeticError(s"Attempt to compute $x modulo 0.")
    else
      x % y

  // Exponentiation by squaring
  // https://en.wikipedia.org/wiki/Exponentiation_by_squaring
  private[this] def exp(base: Long, exponent: Long): Long =
    if (exponent < 0)
      throw DamlEArithmeticError(s"Attempt to raise $base to the negative exponent $exponent.")
    else if (exponent == 0) 1
    else
      try {
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
      } catch {
        case _: ArithmeticException =>
          throw DamlEArithmeticError(
            s"Int64 overflow when raising $base to the exponent $exponent.",
          )
      }

  sealed abstract class SBBinaryOpInt64(op: (Long, Long) => Long) extends SBuiltinPure(2) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue =
      (args.get(0), args.get(1)) match {
        case (SInt64(a), SInt64(b)) => SInt64(op(a, b))
        case _ => crash(s"type mismatch add: $args")
      }
  }

  final case object SBAddInt64 extends SBBinaryOpInt64(add)
  final case object SBSubInt64 extends SBBinaryOpInt64(sub)
  final case object SBMulInt64 extends SBBinaryOpInt64(mult)
  final case object SBDivInt64 extends SBBinaryOpInt64(div)
  final case object SBModInt64 extends SBBinaryOpInt64(mod)
  final case object SBExpInt64 extends SBBinaryOpInt64(exp)

  // Numeric Arithmetic

  private[this] def add(x: Numeric, y: Numeric): Numeric =
    rightOrArithmeticError(
      s"(Numeric ${x.scale}) overflow when adding ${Numeric.toString(y)} to ${Numeric.toString(x)}.",
      Numeric.add(x, y),
    )

  private[this] def subtract(x: Numeric, y: Numeric): Numeric =
    rightOrArithmeticError(
      s"(Numeric ${x.scale}) overflow when subtracting ${Numeric.toString(y)} from ${Numeric.toString(x)}.",
      Numeric.subtract(x, y),
    )

  private[this] def multiply(scale: Scale, x: Numeric, y: Numeric): Numeric =
    rightOrArithmeticError(
      s"(Numeric $scale) overflow when multiplying ${Numeric.toString(x)} by ${Numeric.toString(y)}.",
      Numeric.multiply(scale, x, y),
    )

  private[this] def divide(scale: Scale, x: Numeric, y: Numeric): Numeric =
    if (y.signum() == 0)
      throw DamlEArithmeticError(
        s"Attempt to divide ${Numeric.toString(x)} by ${Numeric.toString(y)}.",
      )
    else
      rightOrArithmeticError(
        s"(Numeric $scale) overflow when dividing ${Numeric.toString(x)} by ${Numeric.toString(y)}.",
        Numeric.divide(scale, x, y),
      )

  sealed abstract class SBBinaryOpNumeric(op: (Numeric, Numeric) => Numeric)
      extends SBuiltinPure(3) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      val scale = args.get(0).asInstanceOf[STNat].n
      val a = args.get(1).asInstanceOf[SNumeric].value
      val b = args.get(2).asInstanceOf[SNumeric].value
      assert(a.scale == scale && b.scale == scale)
      SNumeric(op(a, b))
    }
  }

  sealed abstract class SBBinaryOpNumeric2(op: (Scale, Numeric, Numeric) => Numeric)
      extends SBuiltinPure(5) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      val scaleA = args.get(0).asInstanceOf[STNat].n
      val scaleB = args.get(1).asInstanceOf[STNat].n
      val scale = args.get(2).asInstanceOf[STNat].n
      val a = args.get(3).asInstanceOf[SNumeric].value
      val b = args.get(4).asInstanceOf[SNumeric].value
      assert(a.scale == scaleA && b.scale == scaleB)
      SNumeric(op(scale, a, b))
    }
  }

  final case object SBAddNumeric extends SBBinaryOpNumeric(add)
  final case object SBSubNumeric extends SBBinaryOpNumeric(subtract)
  final case object SBMulNumeric extends SBBinaryOpNumeric2(multiply)
  final case object SBDivNumeric extends SBBinaryOpNumeric2(divide)

  final case object SBRoundNumeric extends SBuiltinPure(3) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      val scale = args.get(0).asInstanceOf[STNat].n
      val prec = args.get(1).asInstanceOf[SInt64].value
      val x = args.get(2).asInstanceOf[SNumeric].value
      SNumeric(
        rightOrArithmeticError(s"Error while rounding (Numeric $scale)", Numeric.round(prec, x)),
      )
    }
  }

  final case object SBCastNumeric extends SBuiltinPure(3) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      val inputScale = args.get(0).asInstanceOf[STNat].n
      val outputScale = args.get(1).asInstanceOf[STNat].n
      val x = args.get(2).asInstanceOf[SNumeric].value
      SNumeric(
        rightOrArithmeticError(
          s"Error while casting (Numeric $inputScale) to (Numeric $outputScale)",
          Numeric.fromBigDecimal(outputScale, x),
        ),
      )
    }
  }

  final case object SBShiftNumeric extends SBuiltinPure(3) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      val inputScale = args.get(0).asInstanceOf[STNat].n
      val outputScale = args.get(1).asInstanceOf[STNat].n
      val x = args.get(2).asInstanceOf[SNumeric].value
      SNumeric(
        rightOrArithmeticError(
          s"Error while shifting (Numeric $inputScale) to (Numeric $outputScale)",
          Numeric.fromBigDecimal(outputScale, x.scaleByPowerOfTen(inputScale - outputScale)),
        ),
      )
    }
  }

  //
  // Text functions
  //
  final case object SBExplodeText extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SText(t) =>
          SList(FrontStack(Utf8.explode(t).map(SText)))
        case _ =>
          throw SErrorCrash(s"type mismatch explodeText: $args")
      }
    }
  }

  final case object SBImplodeText extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SList(xs) =>
          val ts = xs.map {
            case SText(t) => t
            case v =>
              throw SErrorCrash(s"type mismatch implodeText: expected SText, got $v")
          }
          SText(Utf8.implode(ts.toImmArray))
        case _ =>
          throw SErrorCrash(s"type mismatch implodeText: $args")
      }
    }
  }

  final case object SBAppendText extends SBuiltinPure(2) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      (args.get(0), args.get(1)) match {
        case (SText(head), SText(tail)) =>
          SText(head + tail)
        case _ =>
          throw SErrorCrash(s"type mismatch appendText: $args")
      }
    }
  }

  final case object SBToText extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      litToText(args)
    }

    def litToText(vs: util.ArrayList[SValue]): SValue = {
      val v = vs.get(0).asInstanceOf[SPrimLit]
      SText(v match {
        case SBool(b) => b.toString
        case SInt64(i) => i.toString
        case STimestamp(t) => t.toString
        case SText(t) => t
        case SParty(p) => p
        case SUnit => s"<unit>"
        case SDate(date) => date.toString
        case SContractId(_) | SNumeric(_) => crash("litToText: literal not supported")
      })
    }
  }

  final case object SBToTextContractId extends SBuiltin(1) {
    override private[speedy] final def execute(
        args: util.ArrayList[SValue],
        machine: Machine): Unit = {
      args.get(0) match {
        case SContractId(cid) =>
          machine.ledgerMode match {
            case _: OnLedger =>
              machine.returnValue = SValue.SValue.None
            case _: OffLedger.type =>
              machine.returnValue = SOptional(Some(SText(cid.coid)))
          }
        case _ => crash(s"type mismatch toTextContractId: $args")
      }
    }
  }

  final case object SBToTextNumeric extends SBuiltinPure(2) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      val x = args.get(1).asInstanceOf[SNumeric].value
      SText(Numeric.toUnscaledString(x))
    }
  }

  final case object SBToQuotedTextParty extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      val v = args.get(0).asInstanceOf[SParty]
      SText(s"'${v.value: String}'")
    }
  }

  final case object SBToTextCodePoints extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      val codePoints = args.get(0).asInstanceOf[SList].list.map(_.asInstanceOf[SInt64].value)
      SText(Utf8.pack(codePoints.toImmArray))
    }
  }

  final case object SBFromTextParty extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      val v = args.get(0).asInstanceOf[SText]
      Party.fromString(v.value) match {
        case Left(_) => SV.None
        case Right(p) => SOptional(Some(SParty(p)))
      }
    }
  }

  final case object SBFromTextInt64 extends SBuiltinPure(1) {
    private val pattern = """[+-]?\d+""".r.pattern

    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      val s = args.get(0).asInstanceOf[SText].value
      if (pattern.matcher(s).matches())
        try {
          SOptional(Some(SInt64(java.lang.Long.parseLong(s))))
        } catch {
          case _: NumberFormatException =>
            SV.None
        } else
        SV.None
    }
  }

  // The specification of FromTextNumeric is lenient about the format of the string it should
  // accept and convert. In particular it should convert any string with an arbitrary number of
  // leading and trailing '0's as long as the corresponding number fits a Numeric without loss of
  // precision. We should take care not calling String to BigDecimal conversion on huge strings.
  final case object SBFromTextNumeric extends SBuiltinPure(2) {
    private val validFormat =
      """([+-]?)0*(\d+)(\.(\d*[1-9]|0)0*)?""".r

    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      val scale = args.get(0).asInstanceOf[STNat].n
      val string = args.get(1).asInstanceOf[SText].value
      string match {
        case validFormat(signPart, intPart, _, decPartOrNull) =>
          val decPart = Option(decPartOrNull).filterNot(_ == "0").getOrElse("")
          // First, we count the number of significant digits to avoid the conversion attempts that
          // are doomed to failure.
          val significantIntDigits = if (intPart == "0") 0 else intPart.length
          val significantDecDigits = decPart.length
          if (significantIntDigits <= Numeric.maxPrecision - scale && significantDecDigits <= scale) {
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

  final case object SBFromTextCodePoints extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      val string = args.get(0).asInstanceOf[SText].value
      val codePoints = Utf8.unpack(string)
      SList(FrontStack(codePoints.map(SInt64)))
    }
  }

  final case object SBSHA256Text extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SText(t) => SText(Utf8.sha256(t))
        case _ =>
          throw SErrorCrash(s"type mismatch textSHA256: $args")
      }
    }
  }

  final case object SBFoldl extends SBuiltin(3) {
    override private[speedy] final def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Unit = {
      val func = args.get(0)
      val init = args.get(1)
      val list = args.get(2).asInstanceOf[SList].list
      machine.pushKont(KFoldl(machine, func, list))
      machine.returnValue = init
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
    override private[speedy] final def execute(
        args: util.ArrayList[SValue],
        machine: Machine): Unit = {
      val func = args.get(0).asInstanceOf[SPAP]
      val init = args.get(1)
      val list = args.get(2)
      if (func.arity - func.actuals.size >= 2) {
        val array = list.asInstanceOf[SList].list.toImmArray
        machine.pushKont(KFoldr(machine, func, array, array.length))
        machine.returnValue = init
      } else {
        val stack = list.asInstanceOf[SList].list
        stack.pop match {
          case None => machine.returnValue = init
          case Some((head, tail)) =>
            machine.pushKont(KFoldr1Map(machine, func, tail, FrontStack.empty, init))
            machine.enterApplication(func, Array(SEValue(head)))
        }
      }
    }
  }

  final case object SBGenMapToList extends SBuiltinPure(1) {

    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SGenMap(_, entries) =>
          SValue.toList(entries)
        case x =>
          throw SErrorCrash(s"type mismatch SBGenMapToList, expected GenMap get $x")
      }
    }
  }

  final case object SBGenMapInsert extends SBuiltinPure(3) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(2) match {
        case SGenMap(isTextMap, entries) =>
          val key = args.get(0)
          SGenMap.comparable(key)
          SGenMap(isTextMap, entries.updated(key, args.get(1)))
        case x =>
          throw SErrorCrash(s"type mismatch SBGenMapInsert, expected GenMap got $x")
      }
    }
  }

  final case object SBGenMapLookup extends SBuiltinPure(2) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(1) match {
        case SGenMap(_, entries) =>
          val key = args.get(0)
          SGenMap.comparable(key)
          SOptional(entries.get(key))
        case x =>
          throw SErrorCrash(s"type mismatch SBGenMapLookup, expected GenMap get $x")
      }
    }
  }

  final case object SBGenMapDelete extends SBuiltinPure(2) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(1) match {
        case SGenMap(isTextMap, entries) =>
          val key = args.get(0)
          SGenMap.comparable(key)
          SGenMap(isTextMap, entries - key)
        case x =>
          throw SErrorCrash(s"type mismatch SBGenMapDelete, expected GenMap get $x")
      }
    }
  }

  final case object SBGenMapKeys extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SGenMap(_, entries) =>
          SList(ImmArray(entries.keys) ++: FrontStack.empty)
        case x =>
          throw SErrorCrash(s"type mismatch SBGenMapKeys, expected GenMap get $x")
      }
    }
  }

  final case object SBGenMapValues extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SGenMap(_, entries) =>
          SList(ImmArray(entries.values) ++: FrontStack.empty)
        case x =>
          throw SErrorCrash(s"type mismatch SBGenMapValues, expected GenMap get $x")
      }
    }
  }

  final case object SBGenMapSize extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SGenMap(_, entries) =>
          SInt64(entries.size.toLong)
        case x =>
          throw SErrorCrash(s"type mismatch SBGenMapSize, expected GenMap get $x")
      }
    }
  }

  //
  // Conversions
  //

  final case object SBInt64ToNumeric extends SBuiltinPure(2) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      val scale = args.get(0).asInstanceOf[STNat].n
      val x = args.get(1).asInstanceOf[SInt64].value
      SNumeric(
        rightOrArithmeticError(
          s"overflow when converting $x to (Numeric $scale)",
          Numeric.fromLong(scale, x),
        ),
      )
    }
  }

  final case object SBNumericToInt64 extends SBuiltinPure(2) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      val x = args.get(1).asInstanceOf[SNumeric].value
      SInt64(
        rightOrArithmeticError(
          s"Int64 overflow when converting ${Numeric.toString(x)} to Int64",
          Numeric.toLong(x),
        ),
      )
    }
  }

  final case object SBDateToUnixDays extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SDate(d) => SInt64(d.days.toLong)
        case _ =>
          throw SErrorCrash(s"type mismatch dateToUnixDays: $args")
      }
    }
  }

  final case object SBUnixDaysToDate extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SInt64(days) =>
          SDate(
            rightOrArithmeticError(
              s"Could not convert Int64 $days to Date.",
              Time.Date.asInt(days) flatMap Time.Date.fromDaysSinceEpoch,
            ),
          )
        case _ =>
          throw SErrorCrash(s"type mismatch unixDaysToDate: $args")
      }
    }
  }

  final case object SBTimestampToUnixMicroseconds extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case STimestamp(t) => SInt64(t.micros)
        case _ =>
          throw SErrorCrash(s"type mismatch timestampToUnixMicroseconds: $args")
      }
    }
  }

  final case object SBUnixMicrosecondsToTimestamp extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SInt64(t) =>
          STimestamp(
            rightOrArithmeticError(
              s"Could not convert Int64 $t to Timestamp.",
              Time.Timestamp.fromLong(t),
            ),
          )
        case _ =>
          throw SErrorCrash(s"type mismatch unixMicrosecondsToTimestamp: $args")
      }
    }
  }

  //
  // Equality and comparisons
  //
  final case object SBEqual extends SBuiltinPure(2) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      SBool(svalue.Equality.areEqual(args.get(0), args.get(1)))
    }
  }

  sealed abstract class SBCompare(pred: Int => Boolean) extends SBuiltinPure(2) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      SBool(pred(svalue.Ordering.compare(args.get(0), args.get(1))))
    }
  }

  final case object SBLess extends SBCompare(_ < 0)
  final case object SBLessEq extends SBCompare(_ <= 0)
  final case object SBGreater extends SBCompare(_ > 0)
  final case object SBGreaterEq extends SBCompare(_ >= 0)

  /** $consMany[n] :: a -> ... -> List a -> List a */
  final case class SBConsMany(n: Int) extends SBuiltinPure(1 + n) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(n) match {
        case SList(tail) =>
          SList(ImmArray(args.subList(0, n).asScala) ++: tail)
        case x =>
          crash(s"Cons onto non-list: $x")
      }
    }
  }

  /** $cons :: a -> List a -> List a */
  final case object SBCons extends SBuiltinPure(2) {
    override private[speedy] def executePure(args: util.ArrayList[SValue]): SValue = {
      SList(args.get(0) +: args.get(1).asInstanceOf[SList].list)
    }
  }

  /** $some :: a -> Optional a */
  final case object SBSome extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      SOptional(Some(args.get(0)))
    }
  }

  /** $rcon[R, fields] :: a -> b -> ... -> R */
  final case class SBRecCon(id: Identifier, fields: ImmArray[Name])
      extends SBuiltinPure(fields.length)
      with SomeArrayEquals {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      SRecord(id, fields, args)
    }
  }

  /** $rupd[R, field] :: R -> a -> R */
  final case class SBRecUpd(id: Identifier, field: Int) extends SBuiltinPure(2) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SRecord(id2, fields, values) =>
          if (id != id2) {
            crash(s"type mismatch on record update: expected $id, got record of type $id2")
          }
          val values2 = values.clone.asInstanceOf[util.ArrayList[SValue]]
          values2.set(field, args.get(1))
          SRecord(id2, fields, values2)
        case v =>
          crash(s"RecUpd on non-record: $v")
      }
    }
  }

  /** $rupdmulti[R, [field_1, ..., field_n]] :: R -> a_1 -> ... -> a_n -> R */
  final case class SBRecUpdMulti(id: Identifier, updateFields: Array[Int])
      extends SBuiltinPure(1 + updateFields.length)
      with SomeArrayEquals {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SRecord(id2, fields, values) =>
          if (id != id2) {
            crash(s"type mismatch on record update: expected $id, got record of type $id2")
          }
          val values2 = values.clone.asInstanceOf[util.ArrayList[SValue]]
          var i = 0
          while (i < updateFields.length) {
            values2.set(updateFields(i), args.get(i + 1))
            i += 1
          }
          SRecord(id2, fields, values2)
        case v =>
          crash(s"RecUpd on non-record: $v")
      }
    }
  }

  /** $rproj[R, field] :: R -> a */
  final case class SBRecProj(id: Identifier, field: Int) extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SRecord(id @ _, _, values) => values.get(field)
        case v =>
          crash(s"RecProj on non-record: $v")
      }
    }
  }

  // SBStructCon sorts the field after evaluation of its arguments to preserve
  // evaluation order of unordered fields.
  /** $tcon[fields] :: a -> b -> ... -> Struct */
  final case class SBStructCon(inputFieldsOrder: Struct[Int])
      extends SBuiltinPure(inputFieldsOrder.size) {
    private[this] val fieldNames = inputFieldsOrder.mapValues(_ => ())
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      val sortedFields = new util.ArrayList[SValue](inputFieldsOrder.size)
      inputFieldsOrder.values.foreach(i => sortedFields.add(args.get(i)))
      SStruct(fieldNames, sortedFields)
    }
  }

  /** $tproj[fieldIndex] :: Struct -> a */
  final case class SBStructProj(fieldIndex: Int) extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SStruct(fields @ _, values) =>
          values.get(fieldIndex)
        case v =>
          crash(s"StructProj on non-struct: $v")
      }
    }
  }

  /** $tproj[field] :: Struct -> a */
  // This is a slower version of `SBStructProj` for the case when we didn't run
  // the DAML-LF type checker and hence didn't infer the field index.
  final case class SBStructProjByName(field: Ast.FieldName) extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SStruct(fields @ _, values) =>
          values.get(fields.indexOf(field))
        case v =>
          crash(s"StructProj on non-struct: $v")
      }
    }
  }

  /** $tupd[fieldIndex] :: Struct -> a -> Struct */
  final case class SBStructUpd(fieldIndex: Int) extends SBuiltinPure(2) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SStruct(fields, values) =>
          val values2 = values.clone.asInstanceOf[util.ArrayList[SValue]]
          values2.set(fieldIndex, args.get(1))
          SStruct(fields, values2)
        case v =>
          crash(s"StructUpd on non-struct: $v")
      }
    }
  }

  /** $tupd[field] :: Struct -> a -> Struct */
  // This is a slower version of `SBStructUpd` for the case when we didn't run
  // the DAML-LF type checker and hence didn't infer the field index.
  final case class SBStructUpdByName(field: Ast.FieldName) extends SBuiltinPure(2) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SStruct(fields, values) =>
          val values2 = values.clone.asInstanceOf[util.ArrayList[SValue]]
          values2.set(fields.indexOf(field), args.get(1))
          SStruct(fields, values2)
        case v =>
          crash(s"StructUpd on non-struct: $v")
      }
    }
  }

  /** $vcon[V, variant] :: a -> V */
  final case class SBVariantCon(id: Identifier, variant: Ast.VariantConName, constructorRank: Int)
      extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      SVariant(id, variant, constructorRank, args.get(0))
    }
  }

  /** $checkPrecondition
    *    :: arg (template argument)
    *    -> Bool (false if ensure failed)
    *    -> Unit
    */
  final case class SBCheckPrecond(templateId: TypeConName) extends SBuiltinPure(2) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(1) match {
        case SBool(true) =>
          ()
        case SBool(false) =>
          throw DamlETemplatePreconditionViolated(
            templateId = templateId,
            optLocation = None,
            arg = args.get(0).toValue,
          )
        case v =>
          crash(s"PrecondCheck on non-boolean: $v")
      }
      SUnit
    }
  }

  /** $create
    *    :: arg  (template argument)
    *    -> Text (agreement text)
    *    -> List Party (signatories)
    *    -> List Party (observers)
    *    -> Optional {key: key, maintainers: List Party} (template key, if present)
    *    -> ContractId arg
    */
  final case class SBUCreate(templateId: TypeConName) extends OnLedgerBuiltin(5) {
    override protected final def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
        onLedger: OnLedger
    ): Unit = {
      val createArg = args.get(0)
      val createArgValue = createArg.toValue
      val agreement = args.get(1) match {
        case SText(t) => t
        case v => crash(s"agreement not text: $v")
      }
      val sigs = extractParties(args.get(2))
      val obs = extractParties(args.get(3))
      val mbKey = extractOptionalKeyWithMaintainers(args.get(4))
      mbKey.foreach {
        case Node.KeyWithMaintainers(key, maintainers) =>
          if (maintainers.isEmpty)
            throw DamlECreateEmptyContractKeyMaintainers(templateId, createArg.toValue, key)
      }
      val auth = machine.auth
      val (coid, newPtx) = onLedger.ptx
        .insertCreate(
          auth = auth,
          coinst =
            V.ContractInst(template = templateId, arg = createArgValue, agreementText = agreement),
          optLocation = machine.lastLocation,
          signatories = sigs,
          stakeholders = sigs union obs,
          key = mbKey,
        )
        .fold(err => throw DamlETransactionError(err), identity)

      machine.addLocalContract(coid, templateId, createArg)
      onLedger.ptx = newPtx
      checkAborted(onLedger.ptx)
      machine.returnValue = SContractId(coid)
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
      choiceId: ChoiceName,
      consuming: Boolean,
      byKey: Boolean,
  ) extends OnLedgerBuiltin(7) {

    override protected final def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
        onLedger: OnLedger
    ): Unit = {
      val arg = args.get(0).toValue
      val coid = args.get(1) match {
        case SContractId(coid) => coid
        case v => crash(s"expected contract id, got: $v")
      }
      val sigs = extractParties(args.get(2))
      val templateObservers = extractParties(args.get(3))
      val ctrls = extractParties(args.get(4))
      val choiceObservers = extractParties(args.get(5))

      val mbKey = extractOptionalKeyWithMaintainers(args.get(6))
      val auth = machine.auth

      onLedger.ptx = onLedger.ptx
        .beginExercises(
          auth = auth,
          targetId = coid,
          templateId = templateId,
          choiceId = choiceId,
          optLocation = machine.lastLocation,
          consuming = consuming,
          actingParties = ctrls,
          signatories = sigs,
          stakeholders = sigs union templateObservers,
          choiceObservers = choiceObservers,
          mbKey = mbKey,
          byKey = byKey,
          chosenValue = arg,
        )
        .fold(err => throw DamlETransactionError(err), identity)
      checkAborted(onLedger.ptx)
      machine.returnValue = SUnit
    }
  }

  /** $endExercise[T]
    *    :: Value   (result of the exercise)
    *    -> ()
    */
  final case class SBUEndExercise(templateId: TypeConName) extends OnLedgerBuiltin(1) {
    override protected final def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
        onLedger: OnLedger
    ): Unit = {
      val exerciseResult = args.get(0).toValue
      onLedger.ptx = onLedger.ptx.endExercises(exerciseResult)
      checkAborted(onLedger.ptx)
      machine.returnValue = SUnit
    }
  }

  /** $fetch[T]
    *    :: ContractId a
    *    -> a
    */
  final case class SBUFetch(templateId: TypeConName) extends OnLedgerBuiltin(1) {
    override protected final def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
        onLedger: OnLedger
    ): Unit = {
      val coid = args.get(0) match {
        case SContractId(coid) => coid
        case v => crash(s"expected contract id, got: $v")
      }

      onLedger.localContracts.get(coid) match {
        case Some((tmplId, contract)) =>
          if (tmplId != templateId)
            crash(s"contract $coid ($templateId) not found from partial transaction")
          else
            machine.returnValue = contract
        case None =>
          throw SpeedyHungry(
            SResultNeedContract(
              coid,
              templateId,
              onLedger.committers,
              cbMissing = _ => machine.tryHandleException(),
              cbPresent = {
                case V.ContractInst(actualTmplId, V.VersionedValue(_, arg), _) =>
                  // Note that we cannot throw in this continuation -- instead
                  // set the control appropriately which will crash the machine
                  // correctly later.
                  machine.ctrl =
                    if (actualTmplId != templateId)
                      SEDamlException(DamlEWronglyTypedContract(coid, templateId, actualTmplId))
                    else
                      SEImportValue(arg)
              },
            ),
          )
      }
    }
  }

  /** $insertFetch[tid]
    *    :: ContractId a
    *    -> List Party    (signatories)
    *    -> List Party    (observers)
    *    -> Optional {key: key, maintainers: List Party}  (template key, if present)
    *    -> ()
    */
  final case class SBUInsertFetchNode(templateId: TypeConName, byKey: Boolean)
      extends OnLedgerBuiltin(4) {
    override protected final def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
        onLedger: OnLedger
    ): Unit = {
      val coid = args.get(0) match {
        case SContractId(coid) => coid
        case v => crash(s"expected contract id, got: $v")
      }
      val signatories = extractParties(args.get(1))
      val observers = extractParties(args.get(2))
      val key = extractOptionalKeyWithMaintainers(args.get(3))
      val stakeholders = observers union signatories
      val contextActors = machine.contextActors
      val auth = machine.auth
      onLedger.ptx = onLedger.ptx.insertFetch(
        auth,
        coid,
        templateId,
        machine.lastLocation,
        contextActors intersect stakeholders,
        signatories,
        stakeholders,
        key,
        byKey,
      )
      checkAborted(onLedger.ptx)
      machine.returnValue = SUnit
    }
  }

  /** $lookupKey[T]
    *   :: { key: key, maintainers: List Party }
    *   -> Maybe (ContractId T)
    */
  final case class SBULookupKey(templateId: TypeConName) extends OnLedgerBuiltin(1) {
    override protected final def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
        onLedger: OnLedger
    ): Unit = {
      val keyWithMaintainers = extractKeyWithMaintainers(args.get(0))
      if (keyWithMaintainers.maintainers.isEmpty)
        throw DamlEFetchEmptyContractKeyMaintainers(templateId, keyWithMaintainers.key)
      val gkey = GlobalKey(templateId, keyWithMaintainers.key)
      // check if we find it locally
      onLedger.ptx.keys.get(gkey) match {
        case Some(mbCoid) =>
          machine.returnValue = SOptional(mbCoid.map(SContractId))
        case None =>
          // if we cannot find it here, send help, and make sure to update [[PartialTransaction.key]] after
          // that.
          throw SpeedyHungry(
            SResultNeedKey(
              GlobalKeyWithMaintainers(gkey, keyWithMaintainers.maintainers),
              onLedger.committers, {
                case SKeyLookupResult.Found(cid) =>
                  onLedger.ptx = onLedger.ptx.copy(keys = onLedger.ptx.keys + (gkey -> Some(cid)))
                  // We have to check that the discriminator of cid does not conflict with a local ones
                  // however we cannot raise an exception in case of failure here.
                  // We delegate to CtrlImportValue the task to check cid.
                  machine.ctrl = SEImportValue(V.ValueOptional(Some(V.ValueContractId(cid))))
                  true
                case SKeyLookupResult.NotFound =>
                  onLedger.ptx = onLedger.ptx.copy(keys = onLedger.ptx.keys + (gkey -> None))
                  machine.returnValue = SV.None
                  true
                case SKeyLookupResult.NotVisible =>
                  machine.tryHandleException()
              },
            ),
          )
      }
    }
  }

  /** $insertLookup[T]
    *    :: { key : key, maintainers: List Party}
    *    -> Maybe (ContractId T)
    *    -> ()
    */
  final case class SBUInsertLookupNode(templateId: TypeConName) extends OnLedgerBuiltin(2) {
    override protected final def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
        onLedger: OnLedger
    ): Unit = {
      val keyWithMaintainers = extractKeyWithMaintainers(args.get(0))
      val mbCoid = args.get(1) match {
        case SOptional(mb) =>
          mb.map {
            case SContractId(coid) => coid
            case _ => crash(s"Non contract id value when inserting lookup node")
          }
        case _ => crash(s"Non option value when inserting lookup node")
      }
      val auth = machine.auth
      onLedger.ptx = onLedger.ptx.insertLookup(
        auth,
        templateId,
        machine.lastLocation,
        Node.KeyWithMaintainers(
          key = keyWithMaintainers.key,
          maintainers = keyWithMaintainers.maintainers,
        ),
        mbCoid,
      )
      checkAborted(onLedger.ptx)
      machine.returnValue = SV.Unit
    }
  }

  /** $fetchKey[T]
    *   :: { key: key, maintainers: List Party }
    *   -> ContractId T
    */
  final case class SBUFetchKey(templateId: TypeConName) extends OnLedgerBuiltin(1) {
    override protected final def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
        onLedger: OnLedger
    ): Unit = {
      val keyWithMaintainers = extractKeyWithMaintainers(args.get(0))
      if (keyWithMaintainers.maintainers.isEmpty)
        throw DamlEFetchEmptyContractKeyMaintainers(templateId, keyWithMaintainers.key)
      val gkey = GlobalKey(templateId, keyWithMaintainers.key)
      // check if we find it locally
      onLedger.ptx.keys.get(gkey) match {
        case Some(None) =>
          crash(s"Could not find key $gkey")
        case Some(Some(coid)) =>
          machine.returnValue = SContractId(coid)
        case None =>
          // if we cannot find it here, send help, and make sure to update [[PartialTransaction.key]] after
          // that.
          throw SpeedyHungry(
            SResultNeedKey(
              GlobalKeyWithMaintainers(gkey, keyWithMaintainers.maintainers),
              onLedger.committers, {
                case SKeyLookupResult.Found(cid) =>
                  onLedger.ptx = onLedger.ptx.copy(keys = onLedger.ptx.keys + (gkey -> Some(cid)))
                  // We have to check that the discriminator of cid does not conflict with a local ones
                  // however we cannot raise an exception in case of failure here.
                  // We delegate to CtrlImportValue the task to check cid.
                  machine.ctrl = SEImportValue(V.ValueContractId(cid))
                  true
                case SKeyLookupResult.NotFound | SKeyLookupResult.NotVisible =>
                  onLedger.ptx = onLedger.ptx.copy(keys = onLedger.ptx.keys + (gkey -> None))
                  machine.tryHandleException()
              },
            ),
          )
      }
    }
  }

  /** $getTime :: Token -> Timestamp */
  final case object SBGetTime extends SBuiltin(1) {
    override private[speedy] final def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Unit = {
      checkToken(args.get(0))
      // $ugettime :: Token -> Timestamp
      throw SpeedyHungry(
        SResultNeedTime(timestamp => machine.returnValue = STimestamp(timestamp)),
      )
    }
  }

  /** $beginCommit :: Party -> Token -> () */
  final case class SBSBeginCommit(optLocation: Option[Location]) extends OnLedgerBuiltin(2) {
    override protected final def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
        onLedger: OnLedger
    ): Unit = {
      checkToken(args.get(1))
      onLedger.localContracts = Map.empty
      onLedger.globalDiscriminators = Set.empty
      onLedger.committers = extractParties(args.get(0))
      onLedger.commitLocation = optLocation
      machine.returnValue = SV.Unit
    }
  }

  /** $endCommit[mustFail?] :: result -> Token -> () */
  final case class SBSEndCommit(mustFail: Boolean) extends OnLedgerBuiltin(2) {
    override protected final def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
        onLedger: OnLedger
    ): Unit = {
      checkToken(args.get(1))
      if (mustFail) executeMustFail(args, machine, onLedger)
      else executeCommit(args, machine, onLedger)
    }

    private[this] final def executeMustFail(
        args: util.ArrayList[SValue],
        machine: Machine,
        onLedger: OnLedger): Unit = {
      // A mustFail commit evaluated the update with
      // a catch. The second argument is a boolean
      // that marks whether an exception was thrown
      // or not.
      val committerOld = onLedger.committers
      val ptxOld = onLedger.ptx
      val commitLocationOld = onLedger.commitLocation

      args.get(0) match {
        case SBool(true) =>
          // update expression threw an exception. we're
          // now done.
          machine.clearCommit
          machine.returnValue = SV.Unit
          throw SpeedyHungry(SResultScenarioInsertMustFail(committerOld, commitLocationOld))

        case SBool(false) =>
          ptxOld.finish match {
            case PartialTransaction.CompleteTransaction(tx) =>
              // Transaction finished successfully. It might still
              // fail when committed, so tell the scenario runner to
              // do that.
              machine.returnValue = SV.Unit
              throw SpeedyHungry(
                SResultScenarioMustFail(tx, committerOld, _ => machine.clearCommit))
            case PartialTransaction.IncompleteTransaction(_) =>
              machine.clearCommit
              machine.returnValue = SV.Unit
          }
        case v =>
          crash(s"endCommit: expected bool, got: $v")
      }
    }

    private[this] def executeCommit(
        args: util.ArrayList[SValue],
        machine: Machine,
        onLedger: OnLedger): Unit =
      onLedger.ptx.finish match {
        case PartialTransaction.CompleteTransaction(tx) =>
          throw SpeedyHungry(
            SResultScenarioCommit(
              value = args.get(0),
              tx = tx,
              committers = onLedger.committers,
              callback = newValue => {
                machine.clearCommit
                machine.returnValue = newValue
              }
            )
          )
        case PartialTransaction.IncompleteTransaction(ptx) =>
          checkAborted(ptx)
          crash("IMPOSSIBLE: PartialTransaction.finish failed, but transaction was not aborted")
      }
  }

  /** $pass :: Int64 -> Token -> Timestamp */
  final case object SBSPass extends SBuiltin(2) {
    override private[speedy] final def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Unit = {
      checkToken(args.get(1))
      val relTime = args.get(0) match {
        case SInt64(t) => t
        case v =>
          crash(s"expected timestamp, got: $v")
      }
      throw SpeedyHungry(
        SResultScenarioPassTime(
          relTime,
          timestamp => machine.returnValue = STimestamp(timestamp),
        ),
      )
    }
  }

  /** $getParty :: Text -> Token -> Party */
  final case object SBSGetParty extends SBuiltin(2) {
    override private[speedy] final def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Unit = {
      checkToken(args.get(1))
      args.get(0) match {
        case SText(name) =>
          throw SpeedyHungry(
            SResultScenarioGetParty(name, party => machine.returnValue = SParty(party)),
          )
        case v =>
          crash(s"invalid argument to GetParty: $v")
      }
    }
  }

  /** $trace :: Text -> a -> a */
  final case object SBTrace extends SBuiltin(2) {
    override private[speedy] final def execute(
        args: util.ArrayList[SValue],
        machine: Machine,
    ): Unit = {
      args.get(0) match {
        case SText(message) =>
          machine.traceLog.add(message, machine.lastLocation)
          machine.returnValue = args.get(1)
        case v =>
          crash(s"invalid argument to trace: $v")
      }
    }
  }

  /** $error :: Text -> a */
  final case object SBError extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue =
      throw DamlEUserError(args.get(0).asInstanceOf[SText].value)
  }

  /** $to_any
    *    :: t
    *    -> Any (where t = ty)
    */
  final case class SBToAny(ty: Ast.Type) extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      SAny(ty, args.get(0))
    }
  }

  /** $from_any
    *    :: Any
    *    -> Optional t (where t = expectedType)
    */
  final case class SBFromAny(expectedTy: Ast.Type) extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SAny(actualTy, v) =>
          SOptional(if (actualTy == expectedTy) Some(v) else None)
        case v => crash(s"FromAny applied to non-Any: $v")
      }
    }
  }

  // Unstable text primitives.

  /** $text_to_upper :: Text -> Text */
  final case object SBTextToUpper extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SText(t) =>
          SText(t.toUpperCase(util.Locale.ROOT))
        // TODO [FM]: replace with ASCII-specific function, or not
        case x =>
          throw SErrorCrash(s"type mismatch SBTextoUpper, expected Text got $x")
      }
    }
  }

  /** $text_to_lower :: Text -> Text */
  final case object SBTextToLower extends SBuiltinPure(1) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SText(t) =>
          SText(t.toLowerCase(util.Locale.ROOT))
        // TODO [FM]: replace with ASCII-specific function, or not
        case x =>
          throw SErrorCrash(s"type mismatch SBTextToLower, expected Text got $x")
      }
    }
  }

  /** $text_slice :: Int -> Int -> Text -> Text */
  final case object SBTextSlice extends SBuiltinPure(3) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SInt64(from) =>
          args.get(1) match {
            case SInt64(to) =>
              args.get(2) match {
                case SText(t) =>
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
                case x =>
                  throw SErrorCrash(s"type mismatch SBTextSlice, expected Text got $x")
              }
            case x =>
              throw SErrorCrash(s"type mismatch SBTextSlice, expected Int64 got $x")
          }
        case x =>
          throw SErrorCrash(s"type mismatch SBTextSlice, expected Int64 got $x")
      }
    }
  }

  /** $text_slice_index :: Text -> Text -> Optional Int */
  final case object SBTextSliceIndex extends SBuiltinPure(2) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SText(slice) =>
          args.get(1) match {
            case SText(t) =>
              val n = t.indexOfSlice(slice) // n is -1 if slice is not found.
              if (n < 0) {
                SOptional(None)
              } else {
                val rn = t.codePointCount(0, n).toLong // we want to return the number of codepoints!
                SOptional(Some(SInt64(rn)))
              }
            case x =>
              throw SErrorCrash(s"type mismatch SBTextSliceIndex, expected Text got $x")
          }
        case x =>
          throw SErrorCrash(s"type mismatch SBTextSliceIndex, expected Text got $x")
      }
    }
  }

  /** $text_contains_only :: Text -> Text -> Bool */
  final case object SBTextContainsOnly extends SBuiltinPure(2) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SText(alphabet) =>
          args.get(1) match {
            case SText(t) =>
              val alphabetSet = alphabet.codePoints().iterator().asScala.toSet
              val result = t.codePoints().iterator().asScala.forall(alphabetSet.contains(_))
              SBool(result)
            case x =>
              throw SErrorCrash(s"type mismatch SBTextContainsOnly, expected Text got $x")
          }
        case x =>
          throw SErrorCrash(s"type mismatch SBTextContainsOnly, expected Text got $x")
      }
    }
  }

  /** $text_replicate :: Int -> Text -> Text */
  final case object SBTextReplicate extends SBuiltinPure(2) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SInt64(n) =>
          args.get(1) match {
            case SText(t) =>
              if (n < 0) {
                SText("")
              } else {
                val rn = n.min(Int.MaxValue.toLong).toInt
                SText(t * rn)
              }
            case x =>
              throw SErrorCrash(s"type mismatch SBTextReplicate, expected Text got $x")
          }
        case x =>
          throw SErrorCrash(s"type mismatch SBTextReplicate, expected Int64 got $x")
      }
    }
  }

  /** $text_split_on :: Text -> Text -> List Text */
  final case object SBTextSplitOn extends SBuiltinPure(2) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SText(pattern) =>
          args.get(1) match {
            case SText(t) =>
              val seq: Seq[SValue] =
                // Java will produce a two-element list for this with the second
                // element being the empty string.
                if (pattern.isEmpty) {
                  Seq(SText(t))
                } else {
                  // We do not want to do a regex match so we use Pattern.quote
                  // and we want to keep empty strings, so we use -1 as the second argument.
                  t.split(Pattern.quote(pattern), -1).map(SText).toSeq
                }
              SList(FrontStack(seq))
            case x =>
              throw SErrorCrash(s"type mismatch SBTextSplitOn, expected Text got $x")
          }
        case x =>
          throw SErrorCrash(s"type mismatch SBTextSplitOn, expected Text got $x")
      }
    }
  }

  /** $text_intercalate :: Text -> List Text -> Text */
  final case object SBTextIntercalate extends SBuiltinPure(2) {
    override private[speedy] final def executePure(args: util.ArrayList[SValue]): SValue = {
      args.get(0) match {
        case SText(sep) =>
          args.get(1) match {
            case SList(vs) =>
              val xs = vs.map { (v: SValue) =>
                v match {
                  case SText(t) => t
                  case x =>
                    throw SErrorCrash(
                      s"type mismatch SBTextIntercalate, expected Text in list, got $x",
                    )
                }
              }
              SText(xs.iterator.mkString(sep))
            case x =>
              throw SErrorCrash(s"type mismatch SBTextIntercalate, expected List got $x")
          }
        case x =>
          throw SErrorCrash(s"type mismatch SBTextIntercalate, expected Text got $x")
      }
    }
  }

  // Helpers
  //

  /** Check whether the partial transaction has been aborted, and
    * throw if so. The partial transaction abort status must be
    * checked after every operation on it.
    */
  private[this] def checkAborted(ptx: PartialTransaction): Unit =
    ptx.aborted match {
      case Some(Tx.AuthFailureDuringExecution(nid, fa)) =>
        throw DamlEFailedAuthorization(nid, fa)
      case Some(Tx.ContractNotActive(coid, tid, consumedBy)) =>
        throw DamlELocalContractNotActive(coid, tid, consumedBy)
      case Some(Tx.EndExerciseInRootContext) =>
        crash("internal error: end exercise in root context")
      case None =>
        ()
    }

  private[this] def checkToken(v: SValue): Unit =
    v match {
      case SToken => ()
      case _ =>
        crash(s"value not a token: $v")
    }

  private[this] def extractParties(v: SValue): TreeSet[Party] =
    v match {
      case SList(vs) =>
        TreeSet.empty(Party.ordering) ++ vs.iterator.map {
          case SParty(p) => p
          case x => crash(s"non-party value in list: $x")
        }
      case SParty(p) =>
        TreeSet(p)(Party.ordering)
      case _ =>
        crash(s"value not a list of parties or party: $v")
    }

  private[this] val keyWithMaintainersStructFields: Struct[Unit] =
    Struct.assertFromNameSeq(List(keyFieldName, maintainersFieldName))

  private[this] val keyIdx = keyWithMaintainersStructFields.indexOf(keyFieldName)
  private[this] val maintainerIdx = keyWithMaintainersStructFields.indexOf(maintainersFieldName)

  private[this] def extractKeyWithMaintainers(
      v: SValue,
  ): Node.KeyWithMaintainers[V[Nothing]] =
    v match {
      case SStruct(_, vals) =>
        rightOrCrash(
          for {
            keyVal <- vals
              .get(keyIdx)
              .toValue
              .ensureNoCid
              .left
              .map(coid => s"Unexpected contract id in key: $coid")
          } yield
            Node.KeyWithMaintainers(
              key = keyVal,
              maintainers = extractParties(vals.get(maintainerIdx))
            ))
      case _ => crash(s"Invalid key with maintainers: $v")
    }

  private[this] def extractOptionalKeyWithMaintainers(
      optKey: SValue,
  ): Option[Node.KeyWithMaintainers[V[Nothing]]] =
    optKey match {
      case SOptional(mbKey) => mbKey.map(extractKeyWithMaintainers)
      case v => crash(s"Expected optional key with maintainers, got: $v")
    }

  private[this] def rightOrArithmeticError[A](message: String, mb: Either[String, A]): A =
    mb.fold(_ => throw DamlEArithmeticError(s"$message"), identity)

  private[this] def rightOrCrash[A](either: Either[String, A]) =
    either.fold(crash, identity)

}
