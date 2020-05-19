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
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.Speedy.{Machine, SpeedyHungry}
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SValue.{SValue => SV}
import com.daml.lf.transaction.{Transaction => Tx}
import com.daml.lf.value.{Value => V}
import com.daml.lf.value.ValueVersions.asVersionedValue
import com.daml.lf.transaction.Node.{GlobalKey, KeyWithMaintainers}

import scala.collection.JavaConverters._
import scala.collection.immutable.TreeSet

/** Speedy builtin functions */
sealed abstract class SBuiltin(val arity: Int) {
  // Helper for constructing expressions applying this builtin.
  // E.g. SBCons(SEVar(1), SEVar(2))
  def apply(args: SExpr*): SExpr =
    SEApp(SEBuiltin(this), args.toArray)

  /** Execute the builtin with 'arity' number of arguments in 'args'.
    * Updates the machine state accordingly. */
  def execute(args: util.ArrayList[SValue], machine: Machine): Unit
}

object SBuiltin {
  //
  // Arithmetic
  //

  private def add(x: Long, y: Long): Long =
    try {
      Math.addExact(x, y)
    } catch {
      case _: ArithmeticException =>
        throw DamlEArithmeticError(s"Int64 overflow when adding $y to $x.")
    }

  private def div(x: Long, y: Long): Long =
    if (y == 0)
      throw DamlEArithmeticError(s"Attempt to divide $x by 0.")
    else if (x == Long.MinValue && y == -1)
      throw DamlEArithmeticError(s"Int64 overflow when dividing $x by $y.")
    else
      x / y

  private def mult(x: Long, y: Long): Long =
    try {
      Math.multiplyExact(x, y)
    } catch {
      case _: ArithmeticException =>
        throw DamlEArithmeticError(s"Int64 overflow when multiplying $x by $y.")
    }

  private def sub(x: Long, y: Long): Long =
    try {
      Math.subtractExact(x, y)
    } catch {
      case _: ArithmeticException =>
        throw DamlEArithmeticError(s"Int64 overflow when subtracting $y from $x.")
    }

  private def mod(x: Long, y: Long): Long =
    if (y == 0)
      throw DamlEArithmeticError(s"Attempt to compute $x modulo 0.")
    else
      x % y

  // Exponentiation by squaring
  // https://en.wikipedia.org/wiki/Exponentiation_by_squaring
  private def exp(base: Long, exponent: Long): Long =
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

  sealed abstract class SBBinaryOpInt64(op: (Long, Long) => Long) extends SBuiltin(2) {
    final def execute(args: util.ArrayList[SValue], machine: Machine): Unit =
      machine.returnValue = (args.get(0), args.get(1)) match {
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

  private def add(x: Numeric, y: Numeric): Numeric =
    rightOrArithmeticError(
      s"(Numeric ${x.scale}) overflow when adding ${Numeric.toString(y)} to ${Numeric.toString(x)}.",
      Numeric.add(x, y),
    )

  private def subtract(x: Numeric, y: Numeric): Numeric =
    rightOrArithmeticError(
      s"(Numeric ${x.scale}) overflow when subtracting ${Numeric.toString(y)} from ${Numeric.toString(x)}.",
      Numeric.subtract(x, y),
    )

  private def multiply(scale: Scale, x: Numeric, y: Numeric): Numeric =
    rightOrArithmeticError(
      s"(Numeric $scale) overflow when multiplying ${Numeric.toString(x)} by ${Numeric.toString(y)}.",
      Numeric.multiply(scale, x, y),
    )

  private def divide(scale: Scale, x: Numeric, y: Numeric): Numeric =
    if (y.signum() == 0)
      throw DamlEArithmeticError(
        s"Attempt to divide ${Numeric.toString(x)} by ${Numeric.toString(y)}.",
      )
    else
      rightOrArithmeticError(
        s"(Numeric $scale) overflow when dividing ${Numeric.toString(x)} by ${Numeric.toString(y)}.",
        Numeric.divide(scale, x, y),
      )

  sealed abstract class SBBinaryOpNumeric(op: (Numeric, Numeric) => Numeric) extends SBuiltin(3) {
    final def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val scale = args.get(0).asInstanceOf[STNat].n
      val a = args.get(1).asInstanceOf[SNumeric].value
      val b = args.get(2).asInstanceOf[SNumeric].value
      assert(a.scale == scale && b.scale == scale)
      machine.returnValue = SNumeric(op(a, b))
    }
  }

  sealed abstract class SBBinaryOpNumeric2(op: (Scale, Numeric, Numeric) => Numeric)
      extends SBuiltin(5) {
    final def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val scaleA = args.get(0).asInstanceOf[STNat].n
      val scaleB = args.get(1).asInstanceOf[STNat].n
      val scale = args.get(2).asInstanceOf[STNat].n
      val a = args.get(3).asInstanceOf[SNumeric].value
      val b = args.get(4).asInstanceOf[SNumeric].value
      assert(a.scale == scaleA && b.scale == scaleB)
      machine.returnValue = SNumeric(op(scale, a, b))
    }
  }

  final case object SBAddNumeric extends SBBinaryOpNumeric(add)
  final case object SBSubNumeric extends SBBinaryOpNumeric(subtract)
  final case object SBMulNumeric extends SBBinaryOpNumeric2(multiply)
  final case object SBDivNumeric extends SBBinaryOpNumeric2(divide)

  final case object SBRoundNumeric extends SBuiltin(3) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val scale = args.get(0).asInstanceOf[STNat].n
      val prec = args.get(1).asInstanceOf[SInt64].value
      val x = args.get(2).asInstanceOf[SNumeric].value
      machine.returnValue = SNumeric(
        rightOrArithmeticError(s"Error while rounding (Numeric $scale)", Numeric.round(prec, x)),
      )
    }
  }

  final case object SBCastNumeric extends SBuiltin(3) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val inputScale = args.get(0).asInstanceOf[STNat].n
      val outputScale = args.get(1).asInstanceOf[STNat].n
      val x = args.get(2).asInstanceOf[SNumeric].value
      machine.returnValue = SNumeric(
        rightOrArithmeticError(
          s"Error while casting (Numeric $inputScale) to (Numeric $outputScale)",
          Numeric.fromBigDecimal(outputScale, x),
        ),
      )
    }
  }

  final case object SBShiftNumeric extends SBuiltin(3) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val inputScale = args.get(0).asInstanceOf[STNat].n
      val outputScale = args.get(1).asInstanceOf[STNat].n
      val x = args.get(2).asInstanceOf[SNumeric].value
      machine.returnValue = SNumeric(
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
  final case object SBExplodeText extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
        case SText(t) =>
          SList(FrontStack(Utf8.explode(t).map(SText)))
        case _ =>
          throw SErrorCrash(s"type mismatch explodeText: $args")
      }
    }
  }

  final case object SBImplodeText extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
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

  final case object SBAppendText extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = (args.get(0), args.get(1)) match {
        case (SText(head), SText(tail)) =>
          SText(head + tail)
        case _ =>
          throw SErrorCrash(s"type mismatch appendText: $args")
      }
    }
  }

  final case object SBToText extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = litToText(args)
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

  final case object SBToTextNumeric extends SBuiltin(2) {
    override def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val x = args.get(1).asInstanceOf[SNumeric].value
      machine.returnValue = SText(Numeric.toUnscaledString(x))
    }
  }

  final case object SBToQuotedTextParty extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val v = args.get(0).asInstanceOf[SParty]
      machine.returnValue = SText(s"'${v.value: String}'")
    }
  }

  final case object SBToTextCodePoints extends SBuiltin(1) {
    override def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val codePoints = args.get(0).asInstanceOf[SList].list.map(_.asInstanceOf[SInt64].value)
      machine.returnValue = SText(Utf8.pack(codePoints.toImmArray))
    }
  }

  final case object SBFromTextParty extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val v = args.get(0).asInstanceOf[SText]
      machine.returnValue = Party.fromString(v.value) match {
        case Left(_) => SV.None
        case Right(p) => SOptional(Some(SParty(p)))
      }
    }
  }

  final case object SBFromTextInt64 extends SBuiltin(1) {
    private val pattern = """[+-]?\d+""".r.pattern

    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val s = args.get(0).asInstanceOf[SText].value
      machine.returnValue =
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
  final case object SBFromTextNumeric extends SBuiltin(2) {
    private val validFormat =
      """([+-]?)0*(\d+)(\.(\d*[1-9]|0)0*)?""".r

    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val scale = args.get(0).asInstanceOf[STNat].n
      val string = args.get(1).asInstanceOf[SText].value
      machine.returnValue = string match {
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

  final case object SBFromTextCodePoints extends SBuiltin(1) {
    override def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val string = args.get(0).asInstanceOf[SText].value
      val codePoints = Utf8.unpack(string)
      machine.returnValue = SList(FrontStack(codePoints.map(SInt64)))
    }
  }

  final case object SBSHA256Text extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
        case SText(t) => SText(Utf8.sha256(t))
        case _ =>
          throw SErrorCrash(s"type mismatch textSHA256: $args")
      }
    }
  }

  final case object SBTextMapInsert extends SBuiltin(3) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(2) match {
        case STextMap(map) =>
          args.get(0) match {
            case SText(key) =>
              STextMap(map.updated(key, args.get(1)))
            case x =>
              throw SErrorCrash(s"type mismatch SBTextMapInsert, expected Text got $x")
          }
        case x =>
          throw SErrorCrash(s"type mismatch SBTextMapInsert, expected TextMap got $x")
      }
    }
  }

  final case object SBTextMapLookup extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(1) match {
        case STextMap(map) =>
          args.get(0) match {
            case SText(key) =>
              SOptional(map.get(key))
            case x =>
              throw SErrorCrash(s"type mismatch SBTextMapLookup, expected Text get $x")
          }
        case x =>
          throw SErrorCrash(s"type mismatch SBTextMapLookup, expected TextMap get $x")
      }
    }
  }

  final case object SBTextMapDelete extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(1) match {
        case STextMap(map) =>
          args.get(0) match {
            case SText(key) =>
              STextMap(map - key)
            case x =>
              throw SErrorCrash(s"type mismatch SBTextMapDelete, expected Text get $x")
          }
        case x =>
          throw SErrorCrash(s"type mismatch SBTextMapDelete, expected TextMap get $x")
      }
    }
  }

  final case object SBTextMapToList extends SBuiltin(1) {

    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
        case map: STextMap =>
          SValue.toList(map)
        case x =>
          throw SErrorCrash(s"type mismatch SBTextMapToList, expected TextMap get $x")
      }
    }
  }

  final case object SBTextMapSize extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
        case STextMap(map) =>
          SInt64(map.size.toLong)
        case x =>
          throw SErrorCrash(s"type mismatch SBTextMapSize, expected TextMap get $x")
      }
    }
  }

  final case object SBGenMapInsert extends SBuiltin(3) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(2) match {
        case SGenMap(map) =>
          val key = args.get(0)
          SGenMap.comparable(key)
          SGenMap(map.updated(key, args.get(1)))
        case x =>
          throw SErrorCrash(s"type mismatch SBGenMapInsert, expected GenMap got $x")
      }
    }
  }

  final case object SBGenMapLookup extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(1) match {
        case SGenMap(value) =>
          val key = args.get(0)
          SGenMap.comparable(key)
          SOptional(value.get(key))
        case x =>
          throw SErrorCrash(s"type mismatch SBGenMapLookup, expected GenMap get $x")
      }
    }
  }

  final case object SBGenMapDelete extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(1) match {
        case SGenMap(value) =>
          val key = args.get(0)
          SGenMap.comparable(key)
          SGenMap(value - key)
        case x =>
          throw SErrorCrash(s"type mismatch SBGenMapDelete, expected GenMap get $x")
      }
    }
  }

  final case object SBGenMapKeys extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
        case SGenMap(value) =>
          SList(ImmArray(value.keys) ++: FrontStack.empty)
        case x =>
          throw SErrorCrash(s"type mismatch SBGenMapKeys, expected GenMap get $x")
      }
    }
  }

  final case object SBGenMapValues extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
        case SGenMap(value) =>
          SList(ImmArray(value.values) ++: FrontStack.empty)
        case x =>
          throw SErrorCrash(s"type mismatch SBGenMapValues, expected GenMap get $x")
      }
    }
  }

  final case object SBGenMapSize extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
        case SGenMap(value) =>
          SInt64(value.size.toLong)
        case x =>
          throw SErrorCrash(s"type mismatch SBGenMapSize, expected GenMap get $x")
      }
    }
  }

  //
  // Conversions
  //

  final case object SBInt64ToNumeric extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val scale = args.get(0).asInstanceOf[STNat].n
      val x = args.get(1).asInstanceOf[SInt64].value
      machine.returnValue = SNumeric(
        rightOrArithmeticError(
          s"overflow when converting $x to (Numeric $scale)",
          Numeric.fromLong(scale, x),
        ),
      )
    }
  }

  final case object SBNumericToInt64 extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val x = args.get(1).asInstanceOf[SNumeric].value
      machine.returnValue = SInt64(
        rightOrArithmeticError(
          s"Int64 overflow when converting ${Numeric.toString(x)} to Int64",
          Numeric.toLong(x),
        ),
      )
    }
  }

  final case object SBDateToUnixDays extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
        case SDate(d) => SInt64(d.days.toLong)
        case _ =>
          throw SErrorCrash(s"type mismatch dateToUnixDays: $args")
      }
    }
  }

  final case object SBUnixDaysToDate extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
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

  final case object SBTimestampToUnixMicroseconds extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
        case STimestamp(t) => SInt64(t.micros)
        case _ =>
          throw SErrorCrash(s"type mismatch timestampToUnixMicroseconds: $args")
      }
    }
  }

  final case object SBUnixMicrosecondsToTimestamp extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
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
  final case object SBEqual extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = SBool(svalue.Equality.areEqual(args.get(0), args.get(1)))
    }
  }

  sealed abstract class SBCompare(pred: Int => Boolean) extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = SBool(pred(svalue.Ordering.compare(args.get(0), args.get(1))))
    }
  }

  final case object SBLess extends SBCompare(_ < 0)
  final case object SBLessEq extends SBCompare(_ <= 0)
  final case object SBGreater extends SBCompare(_ > 0)
  final case object SBGreaterEq extends SBCompare(_ >= 0)

  /** $consMany[n] :: a -> ... -> List a -> List a */
  final case class SBConsMany(n: Int) extends SBuiltin(1 + n) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(n) match {
        case SList(tail) =>
          SList(ImmArray(args.subList(0, n).asScala) ++: tail)
        case x =>
          crash(s"Cons onto non-list: $x")
      }
    }
  }

  /** $some :: a -> Optional a */
  final case object SBSome extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = SOptional(Some(args.get(0)))
    }
  }

  /** $rcon[R, fields] :: a -> b -> ... -> R */
  final case class SBRecCon(id: Identifier, fields: Array[Name])
      extends SBuiltin(fields.length)
      with SomeArrayEquals {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = SRecord(id, fields, args)
    }
  }

  /** $rupd[R, field] :: R -> a -> R */
  final case class SBRecUpd(id: Identifier, field: Int) extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
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

  /** $rproj[R, field] :: R -> a */
  final case class SBRecProj(id: Identifier, field: Int) extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
        case SRecord(id @ _, _, values) => values.get(field)
        case v =>
          crash(s"RecProj on non-record: $v")
      }
    }
  }

  /** $tcon[fields] :: a -> b -> ... -> Struct */
  final case class SBStructCon(fields: Array[Name])
      extends SBuiltin(fields.length)
      with SomeArrayEquals {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = SStruct(fields, args)
    }
  }

  /** $tproj[field] :: Struct -> a */
  final case class SBStructProj(field: Ast.FieldName) extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
        case SStruct(fields, values) =>
          values.get(fields.indexOf(field))
        case v =>
          crash(s"StructProj on non-struct: $v")
      }
    }
  }

  /** $tupd[field] :: Struct -> a -> Struct */
  final case class SBStructUpd(field: Ast.FieldName) extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
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
      extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = SVariant(id, variant, constructorRank, args.get(0))
    }
  }

  /** $checkPrecondition
    *    :: arg (template argument)
    *    -> Bool (false if ensure failed)
    *    -> Unit
    */
  final case class SBCheckPrecond(templateId: TypeConName) extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      if (args.get(0).isInstanceOf[STextMap])
        throw new Error(args.toString)
      args.get(1) match {
        case SBool(true) =>
          ()
        case SBool(false) =>
          asVersionedValue(args.get(0).toValue) match {
            case Left(err) => crash(err)
            case Right(createArg) =>
              throw DamlETemplatePreconditionViolated(
                templateId = templateId,
                optLocation = None,
                arg = createArg,
              )
          }
        case v =>
          crash(s"PrecondCheck on non-boolean: $v")
      }
      machine.returnValue = SUnit
    }
  }

  /** $create
    *    :: arg  (template argument)
    *    -> Text (agreement text)
    *    -> List Party (signatories)
    *    -> List Party (observers)
    *    -> Optional {key: key, maintainers: List Party} (template key, if present)
    *    -> Token
    *    -> ContractId arg
    */
  final case class SBUCreate(templateId: TypeConName) extends SBuiltin(6) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      checkToken(args.get(5))
      val createArg = args.get(0)
      val createArgValue = asVersionedValue(createArg.toValue) match {
        case Left(err) => crash(err)
        case Right(x) => x
      }
      val agreement = args.get(1) match {
        case SText(t) => t
        case v => crash(s"agreement not text: $v")
      }
      val sigs = extractParties(args.get(2))
      val obs = extractParties(args.get(3))
      val key = extractOptionalKeyWithMaintainers(args.get(4))

      val (coid, newPtx) = machine.ptx
        .insertCreate(
          coinst =
            V.ContractInst(template = templateId, arg = createArgValue, agreementText = agreement),
          optLocation = machine.lastLocation,
          signatories = sigs,
          stakeholders = sigs union obs,
          key = key,
        )
        .fold(err => throw DamlETransactionError(err), identity)

      machine.addLocalContract(coid, templateId, createArg)
      machine.ptx = newPtx
      checkAborted(machine.ptx)
      machine.returnValue = SContractId(coid)
    }
  }

  /** $beginExercise
    *    :: arg                                           (choice argument)
    *    -> ContractId arg                                (contract to exercise)
    *    -> List Party                                    (actors)
    *    -> List Party                                    (signatories)
    *    -> List Party                                    (observers)
    *    -> List Party                                    (choice controllers)
    *    -> Optional {key: key, maintainers: List Party}  (template key, if present)
    *    -> Token
    *    -> ()
    */
  final case class SBUBeginExercise(
      templateId: TypeConName,
      choiceId: ChoiceName,
      consuming: Boolean,
  ) extends SBuiltin(9) {

    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      checkToken(args.get(8))
      val arg = args.get(0).toValue
      val coid = args.get(1) match {
        case SContractId(coid) => coid
        case v => crash(s"expected contract id, got: $v")
      }
      val optActors = args.get(2) match {
        case SOptional(optValue) => optValue.map(extractParties)
        case v => crash(s"expect optional parties, got: $v")
      }
      val byKey = args.get(3) match {
        case SBool(b) => b
        case v => crash(s"expect boolean flag, got: $v")
      }
      val sigs = extractParties(args.get(4))
      val obs = extractParties(args.get(5))
      val ctrls = extractParties(args.get(6))

      val mbKey = extractOptionalKeyWithMaintainers(args.get(7))

      machine.ptx = machine.ptx
        .beginExercises(
          targetId = coid,
          templateId = templateId,
          choiceId = choiceId,
          optLocation = machine.lastLocation,
          consuming = consuming,
          actingParties = optActors.getOrElse(ctrls),
          signatories = sigs,
          stakeholders = sigs union obs,
          controllers = ctrls,
          mbKey = mbKey,
          byKey = byKey,
          chosenValue = asVersionedValue(arg) match {
            case Left(err) => crash(err)
            case Right(x) => x
          },
        )
        .fold(err => throw DamlETransactionError(err), identity)
      checkAborted(machine.ptx)
      machine.returnValue = SUnit
    }
  }

  /** $endExercise[T]
    *    :: Token
    *    -> Value   (result of the exercise)
    *    -> ()
    */
  final case class SBUEndExercise(templateId: TypeConName) extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      checkToken(args.get(0))
      val exerciseResult = args.get(1).toValue
      machine.ptx = machine.ptx
        .endExercises(asVersionedValue(exerciseResult) match {
          case Left(err) => crash(err)
          case Right(x) => x
        })
      checkAborted(machine.ptx)
      machine.returnValue = SUnit
    }
  }

  /** $fetch[T]
    *    :: ContractId a
    *    -> Token
    *    -> a
    */
  final case class SBUFetch(templateId: TypeConName) extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      checkToken(args.get(1))
      val coid = args.get(0) match {
        case SContractId(coid) => coid
        case v => crash(s"expected contract id, got: $v")
      }

      machine.localContracts.get(coid) match {
        case Some((tmplId, contract)) =>
          if (tmplId != templateId)
            crash(s"contract $coid ($templateId) not found from partial transaction")
          else
            machine.returnValue = contract
        case None =>
          coid match {
            case acoid: V.AbsoluteContractId =>
              throw SpeedyHungry(
                SResultNeedContract(
                  acoid,
                  templateId,
                  machine.committers,
                  cbMissing = _ => machine.tryHandleException(),
                  cbPresent = { coinst =>
                    // Note that we cannot throw in this continuation -- instead
                    // set the control appropriately which will crash the machine
                    // correctly later.
                    if (coinst.template != templateId)
                      machine.ctrl = SEWronglyTypeContractId(acoid, templateId, coinst.template)
                    else
                      machine.ctrl = SEImportValue(coinst.arg.value)
                  },
                ),
              )
            case _ =>
              crash(s"contract $coid ($templateId) not found from partial transaction")
          }
      }
    }
  }

  /** $insertFetch[tid]
    *    :: ContractId a
    *    -> List Party    (signatories)
    *    -> List Party    (observers)
    *    -> Optional {key: key, maintainers: List Party}  (template key, if present)
    *    -> Token
    *    -> ()
    */
  final case class SBUInsertFetchNode(templateId: TypeConName, byKey: Boolean) extends SBuiltin(5) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      checkToken(args.get(4))
      val coid = args.get(0) match {
        case SContractId(coid) => coid
        case v => crash(s"expected contract id, got: $v")
      }
      val signatories = extractParties(args.get(1))
      val observers = extractParties(args.get(2))
      val key = extractOptionalKeyWithMaintainers(args.get(3))

      val stakeholders = observers union signatories
      val contextActors = machine.ptx.context.exeContext match {
        case Some(ctx) =>
          ctx.actingParties union ctx.signatories
        case None =>
          machine.committers
      }

      machine.ptx = machine.ptx.insertFetch(
        coid,
        templateId,
        machine.lastLocation,
        contextActors intersect stakeholders,
        signatories,
        stakeholders,
        key,
        byKey,
      )
      checkAborted(machine.ptx)
      machine.returnValue = SUnit
    }
  }

  /** $lookupKey[T]
    *   :: { key: key, maintainers: List Party }
    *   -> Token
    *   -> Maybe (ContractId T)
    */
  final case class SBULookupKey(templateId: TypeConName) extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      checkToken(args.get(1))
      val keyWithMaintainers = extractKeyWithMaintainers(args.get(0))
      val gkey = GlobalKey(templateId, keyWithMaintainers.key.value)
      // check if we find it locally
      machine.ptx.keys.get(gkey) match {
        case Some(mbCoid) =>
          machine.returnValue = SOptional(mbCoid.map { coid =>
            SContractId(coid)
          })
        case None =>
          // if we cannot find it here, send help, and make sure to update [[PartialTransaction.key]] after
          // that.
          throw SpeedyHungry(
            SResultNeedKey(
              gkey,
              machine.committers, {
                case SKeyLookupResult.Found(cid) =>
                  machine.ptx = machine.ptx.copy(keys = machine.ptx.keys + (gkey -> Some(cid)))
                  // We have to check that the discriminator of cid does not conflict with a local ones
                  // however we cannot raise an exception in case of failure here.
                  // We delegate to CtrlImportValue the task to check cid.
                  machine.ctrl = SEImportValue(V.ValueOptional(Some(V.ValueContractId(cid))))
                  true
                case SKeyLookupResult.NotFound =>
                  machine.ptx = machine.ptx.copy(keys = machine.ptx.keys + (gkey -> None))
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
    *    -> Token
    *    -> ()
    */
  final case class SBUInsertLookupNode(templateId: TypeConName) extends SBuiltin(3) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      checkToken(args.get(2))
      val keyWithMaintainers = extractKeyWithMaintainers(args.get(0))
      val mbCoid = args.get(1) match {
        case SOptional(mb) =>
          mb.map {
            case SContractId(coid) => coid
            case _ => crash(s"Non contract id value when inserting lookup node")
          }
        case _ => crash(s"Non option value when inserting lookup node")
      }
      machine.ptx = machine.ptx.insertLookup(
        templateId,
        machine.lastLocation,
        KeyWithMaintainers(
          key = keyWithMaintainers.key,
          maintainers = keyWithMaintainers.maintainers,
        ),
        mbCoid,
      )
      checkAborted(machine.ptx)
      machine.returnValue = SV.Unit
    }
  }

  /** $fetchKey[T]
    *   :: { key: key, maintainers: List Party }
    *   -> Token
    *   -> ContractId T
    */
  final case class SBUFetchKey(templateId: TypeConName) extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      checkToken(args.get(1))
      val keyWithMaintainers = extractKeyWithMaintainers(args.get(0))
      val gkey = GlobalKey(templateId, keyWithMaintainers.key.value)
      // check if we find it locally
      machine.ptx.keys.get(gkey) match {
        case Some(None) =>
          crash(s"Could not find key $gkey")
        case Some(Some(coid)) =>
          machine.returnValue = SContractId(coid)
        case None =>
          // if we cannot find it here, send help, and make sure to update [[PartialTransaction.key]] after
          // that.
          throw SpeedyHungry(
            SResultNeedKey(
              gkey,
              machine.committers, {
                case SKeyLookupResult.Found(cid) =>
                  machine.ptx = machine.ptx.copy(keys = machine.ptx.keys + (gkey -> Some(cid)))
                  // We have to check that the discriminator of cid does not conflict with a local ones
                  // however we cannot raise an exception in case of failure here.
                  // We delegate to CtrlImportValue the task to check cid.
                  machine.ctrl = SEImportValue(V.ValueContractId(cid))
                  true
                case SKeyLookupResult.NotFound | SKeyLookupResult.NotVisible =>
                  machine.ptx = machine.ptx.copy(keys = machine.ptx.keys + (gkey -> None))
                  machine.tryHandleException()
              },
            ),
          )
      }
    }
  }

  /** $getTime :: Token -> Timestamp */
  final case object SBGetTime extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      checkToken(args.get(0))
      // $ugettime :: Token -> Timestamp
      throw SpeedyHungry(
        SResultNeedTime(timestamp => machine.returnValue = STimestamp(timestamp)),
      )
    }
  }

  /** $beginCommit :: Party -> Token -> () */
  final case class SBSBeginCommit(optLocation: Option[Location]) extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      checkToken(args.get(1))
      machine.localContracts = Map.empty
      machine.globalDiscriminators = Set.empty
      machine.committers = extractParties(args.get(0))
      machine.commitLocation = optLocation
      machine.returnValue = SV.Unit
    }
  }

  /** $endCommit[mustFail?] :: result -> Token -> () */
  final case class SBSEndCommit(mustFail: Boolean) extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      checkToken(args.get(1))
      if (mustFail) executeMustFail(args, machine)
      else executeCommit(args, machine)
    }

    private def executeMustFail(args: util.ArrayList[SValue], machine: Machine): Unit = {
      // A mustFail commit evaluated the update with
      // a catch. The second argument is a boolean
      // that marks whether an exception was thrown
      // or not.
      val committerOld = machine.committers
      val ptxOld = machine.ptx
      val commitLocationOld = machine.commitLocation

      args.get(0) match {
        case SBool(true) =>
          // update expression threw an exception. we're
          // now done.
          machine.clearCommit
          machine.returnValue = SV.Unit
          throw SpeedyHungry(SResultScenarioInsertMustFail(committerOld, commitLocationOld))

        case SBool(false) =>
          ptxOld.finish match {
            case Left(_) =>
              machine.clearCommit
              machine.returnValue = SV.Unit
            case Right(tx) =>
              // Transaction finished successfully. It might still
              // fail when committed, so tell the scenario runner to
              // do that.
              machine.returnValue = SV.Unit
              throw SpeedyHungry(
                SResultScenarioMustFail(tx, committerOld, _ => machine.clearCommit))
          }
        case v =>
          crash(s"endCommit: expected bool, got: $v")
      }
    }

    private def executeCommit(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val tx =
        machine.ptx.finish.fold(
          ptx => {
            checkAborted(ptx)
            crash("IMPOSSIBLE: PartialTransaction.finish failed, but transaction was not aborted")
          },
          identity,
        )

      throw SpeedyHungry(
        SResultScenarioCommit(
          value = args.get(0),
          tx = tx,
          committers = machine.committers,
          callback = newValue => {
            machine.clearCommit
            machine.returnValue = newValue
          },
        ),
      )
    }
  }

  /** $pass :: Int64 -> Token -> Timestamp */
  final case object SBSPass extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
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
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
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
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
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
  final case object SBError extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit =
      throw DamlEUserError(args.get(0).asInstanceOf[SText].value)
  }

  /** $to_any
    *    :: t
    *    -> Any (where t = ty)
    */
  final case class SBToAny(ty: Ast.Type) extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = SAny(ty, args.get(0))
    }
  }

  /** $from_any
    *    :: Any
    *    -> Optional t (where t = expectedType)
    */
  final case class SBFromAny(expectedTy: Ast.Type) extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
        case SAny(actualTy, v) =>
          SOptional(if (actualTy == expectedTy) Some(v) else None)
        case v => crash(s"FromAny applied to non-Any: $v")
      }
    }
  }

  // Unstable text primitives.

  /** $text_to_upper :: Text -> Text */
  final case object SBTextToUpper extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      args.get(0) match {
        case SText(t) =>
          machine.returnValue = SText(t.toUpperCase(util.Locale.ROOT))
        // TODO [FM]: replace with ASCII-specific function, or not
        case x =>
          throw SErrorCrash(s"type mismatch SBTextoUpper, expected Text got $x")
      }
    }
  }

  /** $text_to_lower :: Text -> Text */
  final case object SBTextToLower extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      args.get(0) match {
        case SText(t) =>
          machine.returnValue = SText(t.toLowerCase(util.Locale.ROOT))
        // TODO [FM]: replace with ASCII-specific function, or not
        case x =>
          throw SErrorCrash(s"type mismatch SBTextToLower, expected Text got $x")
      }
    }
  }

  /** $text_slice :: Int -> Int -> Text -> Text */
  final case object SBTextSlice extends SBuiltin(3) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
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
  final case object SBTextSliceIndex extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
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
  final case object SBTextContainsOnly extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
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
  final case object SBTextReplicate extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
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
  final case object SBTextSplitOn extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
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
  final case object SBTextIntercalate extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.returnValue = args.get(0) match {
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
  private def checkAborted(ptx: PartialTransaction): Unit =
    ptx.aborted match {
      case Some(Tx.ContractNotActive(coid, tid, consumedBy)) =>
        throw DamlELocalContractNotActive(coid, tid, consumedBy)
      case Some(Tx.EndExerciseInRootContext) =>
        crash("internal error: end exercise in root context")
      case None =>
        ()
    }

  private def checkToken(v: SValue): Unit =
    v match {
      case SToken => ()
      case _ =>
        crash(s"value not a token: $v")
    }

  private def extractParties(v: SValue): TreeSet[Party] =
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

  private def extractKeyWithMaintainers(v: SValue): KeyWithMaintainers[Tx.Value[Nothing]] =
    v match {
      case SStruct(flds, vals)
          if flds.length == 2 && flds(0) == Ast.keyFieldName && flds(1) == Ast.maintainersFieldName =>
        rightOrCrash(
          for {
            keyVal <- vals
              .get(0)
              .toValue
              .ensureNoCid
              .left
              .map(coid => s"Unexpected contract id in key: $coid")
            versionedKeyVal <- asVersionedValue(keyVal)
          } yield
            KeyWithMaintainers(
              key = versionedKeyVal,
              maintainers = extractParties(vals.get(1))
            ))
      case _ => crash(s"Invalid key with maintainers: $v")
    }

  private def extractOptionalKeyWithMaintainers(
      optKey: SValue): Option[KeyWithMaintainers[Tx.Value[Nothing]]] =
    optKey match {
      case SOptional(mbKey) => mbKey.map(extractKeyWithMaintainers)
      case v => crash(s"Expected optional key with maintainers, got: $v")
    }

  private def rightOrArithmeticError[A](message: String, mb: Either[String, A]): A =
    mb.fold(_ => throw DamlEArithmeticError(s"$message"), identity)

  private def rightOrCrash[A](either: Either[String, A]) =
    either.fold(crash, identity)

}
