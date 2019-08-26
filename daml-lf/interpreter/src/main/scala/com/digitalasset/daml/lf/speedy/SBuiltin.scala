// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import java.util

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.SError._
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.Speedy.{
  CtrlValue,
  CtrlWronglyTypeContractId,
  Machine,
  SpeedyHungry
}
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.transaction.Transaction._
import com.digitalasset.daml.lf.value.{Value => V}
import com.digitalasset.daml.lf.value.ValueVersions.asVersionedValue
import com.digitalasset.daml.lf.transaction.Node.{GlobalKey, KeyWithMaintainers}
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, RelativeContractId}

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

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
            s"Int64 overflow when raising $base to the exponent $exponent.")
      }

  sealed abstract class SBBinaryOpInt64(op: (Long, Long) => Long) extends SBuiltin(2) {
    final def execute(args: util.ArrayList[SValue], machine: Machine): Unit =
      machine.ctrl = CtrlValue(
        (args.get(0), args.get(1)) match {
          case (SInt64(a), SInt64(b)) => SInt64(op(a, b))
          case _ => crash(s"type mismatch add: $args")
        }
      )
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
      Numeric.add(x, y)
    )

  private def divide(x: Numeric, y: Numeric): Numeric =
    if (y.signum() == 0)
      throw DamlEArithmeticError(
        s"Attempt to divide ${Numeric.toString(x)} by ${Numeric.toString(y)}.")
    else
      rightOrArithmeticError(
        s"(Numeric ${x.scale}) overflow when dividing ${Numeric.toString(x)} by ${Numeric.toString(y)}.",
        Numeric.divide(x, y)
      )

  private def multiply(x: Numeric, y: Numeric): Numeric =
    rightOrArithmeticError(
      s"(Numeric ${x.scale}) overflow when multiplying ${Numeric.toString(x)} by ${Numeric.toString(y)}.",
      Numeric.multiply(x, y)
    )

  private def subtract(x: Numeric, y: Numeric): Numeric =
    rightOrArithmeticError(
      s"(Numeric ${x.scale}) overflow when subtracting ${Numeric.toString(y)} from ${Numeric.toString(x)}.",
      Numeric.subtract(x, y)
    )

  sealed abstract class SBBinaryOpNumeric(op: (Numeric, Numeric) => Numeric) extends SBuiltin(3) {
    final def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val value = (args.get(0), args.get(1), args.get(2)) match {
        case (STNat(scale), SNumeric(a), SNumeric(b)) if a.scale == scale && b.scale == scale =>
          SNumeric(op(a, b))
        case _ => crash(s"type mismatch add: $args")
      }
      machine.ctrl = CtrlValue(value)
    }
  }

  final case object SBAddNumeric extends SBBinaryOpNumeric(add)
  final case object SBSubNumeric extends SBBinaryOpNumeric(subtract)
  final case object SBMulNumeric extends SBBinaryOpNumeric(multiply)
  final case object SBDivNumeric extends SBBinaryOpNumeric(divide)

  //
  // Text functions
  //
  final case object SBExplodeText extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(
        args.get(0) match {
          case SText(t) =>
            SList(FrontStack(Utf8.explode(t).map(SText)))
          case _ =>
            throw SErrorCrash(s"type mismatch explodeText: $args")
        }
      )
    }
  }

  final case object SBImplodeText extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(
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
      )
    }
  }

  final case object SBAppendText extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(
        (args.get(0), args.get(1)) match {
          case (SText(head), SText(tail)) =>
            SText(head + tail)
          case _ =>
            throw SErrorCrash(s"type mismatch appendText: $args")
        }
      )
    }
  }

  final case object SBToText extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(litToText(args))
    }

    def litToText(vs: util.ArrayList[SValue]): SValue = {
      val v = vs.get(0).asInstanceOf[SPrimLit]
      SText(v match {
        case SBool(b) => b.toString
        case SInt64(i) => i.toString
        case STimestamp(t) => t.toString
        case SText(t) => t
        case SParty(p) => p
        case SUnit(_) => s"<unit>"
        case SDate(date) => date.toString
        case SContractId(_) | SNumeric(_) => crash("litToText: literal not supported")
      })
    }
  }

  final case object SBToTextNumeric extends SBuiltin(2) {
    override def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val value = (args.get(0), args.get(1)) match {
        case (STNat(scale), SNumeric(x)) if x.scale == scale =>
          SText(Numeric.toUnscaledString(x))
        case _ =>
          throw SErrorCrash(s"type mismatch FromTextNumeric: $args")
      }
      machine.ctrl = CtrlValue(value)
    }
  }

  final case object SBToQuotedTextParty extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val v = args.get(0).asInstanceOf[SParty]
      machine.ctrl = CtrlValue(SText(s"'${v.value: String}'"))
    }
  }

  final case object SBToTextCodePoints extends SBuiltin(1) {
    override def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val codePoints = args.get(0).asInstanceOf[SList].list.map(_.asInstanceOf[SInt64].value)
      machine.ctrl = CtrlValue(SText(Utf8.pack(codePoints.toImmArray)))
    }
  }

  final case object SBFromTextParty extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val v = args.get(0).asInstanceOf[SText]
      val mbParty = Party.fromString(v.value) match {
        case Left(_) => None
        case Right(p) => Some(SParty(p))
      }
      machine.ctrl = CtrlValue(SOptional(mbParty))
    }
  }

  final case object SBFromTextInt64 extends SBuiltin(1) {
    private val pattern = """[+-]?\d+""".r.pattern

    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val s = args.get(0).asInstanceOf[SText].value
      val int64 =
        if (pattern.matcher(s).matches())
          try {
            Some(SInt64(java.lang.Long.parseLong(s)))
          } catch {
            case _: NumberFormatException =>
              None
          } else
          None
      machine.ctrl = CtrlValue(SOptional(int64))
    }
  }

  final case object SBFromTextNumeric extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val num = (args.get(0), args.get(1)) match {
        case (STNat(scale), SText(s)) =>
          Numeric
            .fromUnscaledString(s)
            .flatMap(Numeric.fromBigDecimal(scale, _))
            .fold(_ => None, x => Some(SNumeric(x)))
        case _ =>
          throw SErrorCrash(s"type mismatch FromTextNumeric: $args")
      }
      machine.ctrl = CtrlValue(SOptional(num))
    }
  }

  final case object SBFromTextCodePoints extends SBuiltin(1) {
    override def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val string = args.get(0).asInstanceOf[SText].value
      val codePoints = Utf8.unpack(string)
      machine.ctrl = CtrlValue(SList(FrontStack(codePoints.map(SInt64))))
    }
  }

  final case object SBSHA256Text extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(args.get(0) match {
        case SText(t) => SText(Utf8.sha256(t))
        case _ =>
          throw SErrorCrash(s"type mismatch textSHA256: $args")
      })
    }
  }

  final case object SBMapEmpty extends SBuiltin(0) {
    private val result = CtrlValue(SMap(HashMap.empty))
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = result
    }
  }

  final case object SBMapInsert extends SBuiltin(3) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(args.get(2) match {
        case SMap(map) =>
          args.get(0) match {
            case SText(key) =>
              SMap(map.updated(key, args.get(1)))
            case x =>
              throw SErrorCrash(s"type mismatch SBMapInsert, expected Text got $x")
          }
        case x =>
          throw SErrorCrash(s"type mismatch SBMapInsert, expected Map got $x")
      })
    }
  }

  final case object SBMapLookup extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(args.get(1) match {
        case SMap(map) =>
          args.get(0) match {
            case SText(key) =>
              SOptional(map.get(key))
            case x =>
              throw SErrorCrash(s"type mismatch SBMapLookup, expected Text get $x")
          }
        case x =>
          throw SErrorCrash(s"type mismatch SBMapLookup, expected Map get $x")
      })
    }
  }

  final case object SBMapDelete extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(args.get(1) match {
        case SMap(map) =>
          args.get(0) match {
            case SText(key) =>
              SMap(map - key)
            case x =>
              throw SErrorCrash(s"type mismatch SBMapDelete, expected Text get $x")
          }
        case x =>
          throw SErrorCrash(s"type mismatch SBMapDelete, expected Map get $x")
      })
    }
  }

  final case object SBMapToList extends SBuiltin(1) {

    private val entryFields = Name.Array(Ast.keyFieldName, Ast.valueFieldName)

    private def entry(key: String, value: SValue) = {
      val args = new util.ArrayList[SValue](2)
      args.add(SText(key))
      args.add(value)
      STuple(entryFields, args)
    }

    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(args.get(0) match {
        case SMap(map) =>
          val entries = SortedLookupList(map).toImmArray
          SList(FrontStack(entries.map { case (k, v) => entry(k, v) }))
        case x =>
          throw SErrorCrash(s"type mismatch SBMaptoList, expected Map get $x")
      })
    }
  }

  final case object SBMapSize extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(args.get(0) match {
        case SMap(map) =>
          SInt64(map.size.toLong)
        case x =>
          throw SErrorCrash(s"type mismatch SBMapSize, expected Map get $x")
      })
    }
  }

  //
  // Conversions
  //

  final case object SBInt64ToNumeric extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val value = (args.get(0), args.get(1)) match {
        case (STNat(scale), SInt64(x)) =>
          SNumeric(
            rightOrArithmeticError(
              s"overflow when converting $x to (Numeric $scale)",
              Numeric.fromLong(scale, x)))
        case _ =>
          throw SErrorCrash(s"type mismatch int64ToNumeric: $args")
      }
      machine.ctrl = CtrlValue(value)
    }
  }

  final case object SBNumericToInt64 extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val value = (args.get(0), args.get(1)) match {
        case (STNat(scale), SNumeric(x)) if x.scale == scale =>
          SInt64(
            rightOrArithmeticError(
              s"Int64 overflow when converting ${Numeric.toString(x)} to Int64",
              Numeric.toLong(x)))
        case _ =>
          throw SErrorCrash(s"type mismatch NumericToInt64: $args")
      }
      machine.ctrl = CtrlValue(value)
    }
  }

  final case object SBDateToUnixDays extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(args.get(0) match {
        case SDate(d) => SInt64(d.days.toLong)
        case _ =>
          throw SErrorCrash(s"type mismatch dateToUnixDays: $args")
      })
    }
  }

  final case object SBUnixDaysToDate extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(
        args.get(0) match {
          case SInt64(days) =>
            SDate(
              rightOrArithmeticError(
                s"Could not convert Int64 $days to Date.",
                Time.Date.asInt(days) flatMap Time.Date.fromDaysSinceEpoch))
          case _ =>
            throw SErrorCrash(s"type mismatch unixDaysToDate: $args")
        }
      )
    }
  }

  final case object SBTimestampToUnixMicroseconds extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(
        args.get(0) match {
          case STimestamp(t) => SInt64(t.micros)
          case _ =>
            throw SErrorCrash(s"type mismatch timestampToUnixMicroseconds: $args")
        }
      )
    }
  }

  final case object SBUnixMicrosecondsToTimestamp extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(
        args.get(0) match {
          case SInt64(t) =>
            STimestamp(
              rightOrArithmeticError(
                s"Could not convert Int64 $t to Timestamp.",
                Time.Timestamp.fromLong(t)))
          case _ =>
            throw SErrorCrash(s"type mismatch unixMicrosecondsToTimestamp: $args")
        }
      )
    }
  }

  final case object SBRoundNumeric extends SBuiltin(3) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val value = (args.get(0), args.get(1), args.get(2)) match {
        case (STNat(scale), SInt64(prec), SNumeric(x)) if x.scale == scale =>
          SNumeric(
            rightOrArithmeticError(
              s"Error while rounding (Numeric $scale)",
              Numeric.round(prec, x)))
        case _ =>
          throw SErrorCrash(s"type mismatch roundD: $args")
      }
      machine.ctrl = CtrlValue(value)
    }
  }

  //
  // Equality and comparisons
  //
  final case object SBEqual extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(
        SBool(args.get(0).asInstanceOf[SPrimLit].equalTo(args.get(1).asInstanceOf[SPrimLit])))
    }
  }

  sealed abstract class SBCompare(pred: Int => Boolean) extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(SBool((args.get(0), args.get(1)) match {
        case (SInt64(a), SInt64(b)) => pred(a compareTo b)
        case (STimestamp(a), STimestamp(b)) => pred(a compareTo b)
        case (SText(a), SText(b)) => pred(Utf8.Ordering.compare(a, b))
        case (SDate(a), SDate(b)) => pred(a compareTo b)
        case (SParty(a), SParty(b)) => pred(a compareTo b)
        case _ =>
          crash(s"type mismatch less: $args")
      }))
    }
  }

  sealed abstract class SBCompareNumeric(pred: Int => Boolean) extends SBuiltin(3) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      val value = SBool((args.get(0), args.get(1), args.get(2)) match {
        case (STNat(scale), SNumeric(a), SNumeric(b)) if a.scale == scale && b.scale == scale =>
          pred(Numeric.compareTo(a, b))
        case _ =>
          crash(s"type mismatch less: $args")
      })
      machine.ctrl = CtrlValue(value)
    }
  }

  final case object SBLess extends SBCompare(_ < 0)
  final case object SBLessEq extends SBCompare(_ <= 0)
  final case object SBGreater extends SBCompare(_ > 0)
  final case object SBGreaterEq extends SBCompare(_ >= 0)

  final case object SBEqualNumeric extends SBCompareNumeric(_ == 0)
  final case object SBLessNumeric extends SBCompareNumeric(_ < 0)
  final case object SBLessEqNumeric extends SBCompareNumeric(_ <= 0)
  final case object SBGreaterNumeric extends SBCompareNumeric(_ > 0)
  final case object SBGreaterEqNumeric extends SBCompareNumeric(_ >= 0)

  /** $consMany[n] :: a -> ... -> List a -> List a */
  final case class SBConsMany(n: Int) extends SBuiltin(1 + n) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(args.get(n) match {
        case SList(tail) =>
          SList(ImmArray(args.subList(0, n).asScala) ++: tail)
        case x =>
          crash(s"Cons onto non-list: $x")
      })
    }
  }

  /** $some :: a -> Optional a */
  final case object SBSome extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(SOptional(Some(args.get(0))))
    }
  }

  /** $rcon[R, fields] :: a -> b -> ... -> R */
  final case class SBRecCon(id: Identifier, fields: Array[Name])
      extends SBuiltin(fields.length)
      with SomeArrayEquals {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(SRecord(id, fields, args))
    }
  }

  /** $rupd[R, field] :: R -> a -> R */
  final case class SBRecUpd(id: Identifier, field: Int) extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(args.get(0) match {
        case SRecord(id2, fields, values) =>
          if (id != id2) {
            crash(s"type mismatch on record update: expected $id, got record of type $id2")
          }
          val values2 = values.clone.asInstanceOf[util.ArrayList[SValue]]
          values2.set(field, args.get(1))
          SRecord(id2, fields, values2)
        case v =>
          crash(s"RecUpd on non-record: $v")
      })
    }
  }

  /** $rproj[R, field] :: R -> a */
  final case class SBRecProj(id: Identifier, field: Int) extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(args.get(0) match {
        case SRecord(id @ _, _, values) => values.get(field)
        case v =>
          crash(s"RecProj on non-record: $v")
      })
    }
  }

  /** $tcon[fields] :: a -> b -> ... -> Tuple */
  final case class SBTupleCon(fields: Array[Name])
      extends SBuiltin(fields.length)
      with SomeArrayEquals {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(STuple(fields, args))
    }
  }

  /** $tproj[field] :: Tuple -> a */
  final case class SBTupleProj(field: FieldName) extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(args.get(0) match {
        case STuple(fields, values) =>
          values.get(fields.indexOf(field))
        case v =>
          crash(s"TupleProj on non-tuple: $v")
      })
    }
  }

  /** $tupd[field] :: Tuple -> a -> Tuple */
  final case class SBTupleUpd(field: FieldName) extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(args.get(0) match {
        case STuple(fields, values) =>
          val values2 = values.clone.asInstanceOf[util.ArrayList[SValue]]
          values2.set(fields.indexOf(field), args.get(1))
          STuple(fields, values2)
        case v =>
          crash(s"TupleUpd on non-tuple: $v")
      })
    }
  }

  /** $vcon[V, variant] :: a -> V */
  final case class SBVariantCon(id: Identifier, variant: VariantConName) extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      machine.ctrl = CtrlValue(SVariant(id, variant, args.get(0)))
    }
  }

  /** $checkPrecondition
    *    :: arg (template argument)
    *    -> Bool (false if ensure failed)
    *    -> Unit
    */
  final case class SBCheckPrecond(templateId: TypeConName) extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      if (args.get(0).isInstanceOf[SMap])
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
                arg = createArg)
          }
        case v =>
          crash(s"PrecondCheck on non-boolean: $v")
      }
      machine.ctrl = CtrlValue(SUnit(()))
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
      val createArg = asVersionedValue(args.get(0).toValue) match {
        case Left(err) => crash(err)
        case Right(x) => x
      }
      val agreement = args.get(1) match {
        case SText(t) => t
        case v => crash(s"agreement not text: $v")
      }
      val sigs = extractParties(args.get(2))
      val obs = extractParties(args.get(3))
      val key = args.get(4) match {
        case SOptional(None) => None
        case SOptional(Some(STuple(flds, vals)))
            if flds.length == 2 && flds(0) == "key" && flds(1) == "maintainers" =>
          asVersionedValue(vals.get(0).toValue) match {
            case Left(err) => crash(err)
            case Right(keyVal) =>
              Some(KeyWithMaintainers(key = keyVal, maintainers = extractParties(vals.get(1))))
          }
        case _ => crash("Bad key")
      }
      val (coid, newPtx) = machine.ptx
        .create(
          coinst = V.ContractInst(template = templateId, arg = createArg, agreementText = agreement),
          optLocation = machine.lastLocation,
          signatories = sigs,
          stakeholders = sigs union obs,
          key = key,
        )
        .fold(err => throw DamlETransactionError(err), identity)

      machine.ptx = newPtx
      machine.ctrl = CtrlValue(SContractId(coid))
      checkAborted(machine.ptx)
    }
  }

  /** $beginExercise
    *    :: arg            (choice argument)
    *    -> ContractId arg (contract to exercise)
    *    -> List Party     (actors)
    *    -> List Party     (signatories)
    *    -> List Party     (observers)
    *    -> List Party     (choice controllers)
    *    -> Optional key   (template key)
    *    -> Token
    *    -> ()
    */
  final case class SBUBeginExercise(
      templateId: TypeConName,
      choiceId: ChoiceName,
      consuming: Boolean)
      extends SBuiltin(8) {

    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      checkToken(args.get(7))
      val arg = args.get(0).toValue
      val coid = args.get(1) match {
        case SContractId(coid) => coid
        case v => crash(s"expected contract id, got: $v")
      }
      val optActors = args.get(2) match {
        case SOptional(optValue) => optValue.map(extractParties)
        case v => crash(s"expect optional parties, got: $v")
      }
      val sigs = extractParties(args.get(3))
      val obs = extractParties(args.get(4))
      val ctrls = extractParties(args.get(5))
      val mbKey = args.get(6) match {
        case SOptional(mbKey) => mbKey.map(_.toValue)
        case _ => crash("Bad key, expected optional")
      }

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
          mbKey = mbKey.map { k =>
            asVersionedValue(k) match {
              case Left(err) => crash(err)
              case Right(x) =>
                x.mapContractId {
                  case RelativeContractId(rcoid) => crash(s"got relative contract id $rcoid in key")
                  case coid: AbsoluteContractId => coid
                }
            }
          },
          chosenValue = asVersionedValue(arg) match {
            case Left(err) => crash(err)
            case Right(x) => x
          }
        )
        .fold(err => throw DamlETransactionError(err), identity)
      checkAborted(machine.ptx)
      machine.ctrl = CtrlValue(SUnit(()))
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
        ._2
      machine.ctrl = CtrlValue(SUnit(()))
      checkAborted(machine.ptx)
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
      val arg = coid match {
        case rcoid: V.RelativeContractId =>
          machine.ptx.lookupLocalContract(rcoid) match {
            case None =>
              crash(s"Relative contract $rcoid ($templateId) not found from partial transaction")
            case Some((_, Some(consumedBy))) =>
              throw DamlELocalContractNotActive(coid, templateId, consumedBy)
            case Some((coinst, None)) =>
              // Here we crash hard rather than throwing a "nice" error
              // ([[DamlEWronglyTypedContract]]) since if _relative_ contract
              // id to be of the wrong template it means that the DAML-LF
              // program that generated it is ill-typed.
              //
              // On the other hand absolute contract ids can come from outside
              // (e.g. Ledger API) and thus we need to fail more gracefully
              // (see below).
              if (coinst.template != templateId) {
                crash(s"Relative contract $rcoid ($templateId) not found from partial transaction")
              }
              coinst.arg
          }
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
                if (coinst.template != templateId) {
                  machine.ctrl = CtrlWronglyTypeContractId(acoid, templateId, coinst.template)
                } else {
                  machine.ctrl = CtrlValue(SValue.fromValue(coinst.arg.value))
                }
              }
            ))
      }
      machine.ctrl = CtrlValue(SValue.fromValue(arg.value))
    }
  }

  /** $insertFetch[tid]
    *    :: ContractId a
    *    -> List Party    (signatories)
    *    -> List Party    (observers)
    *    -> Token
    *    -> ()
    */
  final case class SBUInsertFetchNode(templateId: TypeConName) extends SBuiltin(4) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      checkToken(args.get(3))
      val coid = args.get(0) match {
        case SContractId(coid) => coid
        case v => crash(s"expected contract id, got: $v")
      }
      val signatories = extractParties(args.get(1))
      val observers = extractParties(args.get(2))
      val stakeholders = observers union signatories
      val contextActors = machine.ptx.context match {
        case ContextExercises(ctx) => ctx.actingParties union ctx.signatories
        case ContextRoot => machine.committers.toList.toSet
      }

      machine.ptx = machine.ptx.insertFetch(
        coid,
        templateId,
        machine.lastLocation,
        contextActors intersect stakeholders,
        signatories,
        stakeholders)
      machine.ctrl = CtrlValue(SUnit(()))
      checkAborted(machine.ptx)
    }
  }

  /** $lookupKey[T]
    *   :: key
    *   -> List Party (maintainers)
    *   -> Token
    *   -> Maybe (ContractId T)
    */
  final case class SBULookupKey(templateId: TypeConName) extends SBuiltin(3) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      checkToken(args.get(2))
      val key = asVersionedValue(args.get(0).toValue.mapContractId[Nothing] { cid =>
        crash(s"Unexpected contract id in key: $cid")
      }) match {
        case Left(err) => crash(err)
        case Right(x) => x
      }
      val maintainers = extractParties(args.get(1))
      checkLookupMaintainers(templateId, machine, maintainers)
      val gkey = GlobalKey(templateId, key)
      // check if we find it locally
      machine.ptx.keys.get(gkey) match {
        case Some(mbCoid) =>
          machine.ctrl = CtrlValue(SOptional(mbCoid.map { coid =>
            SContractId(coid)
          }))
        case None =>
          // if we cannot find it here, send help, and make sure to update [[PartialTransaction.key]] after
          // that.
          throw SpeedyHungry(
            SResultNeedKey(
              gkey,
              machine.committers,
              cbMissing = _ => {
                machine.ptx = machine.ptx.copy(keys = machine.ptx.keys + (gkey -> None))
                machine.ctrl = CtrlValue(SOptional(None))
                true
              },
              cbPresent = { contractId =>
                machine.ptx = machine.ptx.copy(keys = machine.ptx.keys + (gkey -> Some(contractId)))
                machine.ctrl = CtrlValue(SOptional(Some(SContractId(contractId))))
              }
            ))
      }
    }
  }

  /** $insertLookup[T]
    *    :: key
    *    -> List Party (maintainers)
    *    -> Maybe (ContractId T)
    *    -> Token
    *    -> ()
    */
  final case class SBUInsertLookupNode(templateId: TypeConName) extends SBuiltin(4) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      checkToken(args.get(3))
      val key =
        asVersionedValue(
          args
            .get(0)
            .toValue
            .mapContractId(coid => crash(s"Unexpected contract id in key: $coid"))) match {
          case Left(err) => crash(err)
          case Right(v) => v
        }
      val maintainers = extractParties(args.get(1))
      val mbCoid = args.get(2) match {
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
        KeyWithMaintainers(key = key, maintainers = maintainers),
        mbCoid)
      machine.ctrl = CtrlValue(SUnit(()))
      checkAborted(machine.ptx)
    }
  }

  /** $fetchKey[T]
    *   :: key
    *   -> List Party (maintainers)
    *   -> Token
    *   -> ContractId T
    */
  final case class SBUFetchKey(templateId: TypeConName) extends SBuiltin(3) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      checkToken(args.get(2))
      val key = asVersionedValue(args.get(0).toValue.mapContractId[Nothing] { cid =>
        crash(s"Unexpected contract id in key: $cid")
      }) match {
        case Left(err) => crash(err)
        case Right(x) => x
      }
      val maintainers = extractParties(args.get(1))
      checkLookupMaintainers(templateId, machine, maintainers)
      val gkey = GlobalKey(templateId, key)
      // check if we find it locally
      machine.ptx.keys.get(gkey) match {
        case Some(None) =>
          crash(s"Could not find key $gkey")
        case Some(Some(coid)) =>
          machine.ctrl = CtrlValue(SContractId(coid))
        case None =>
          // if we cannot find it here, send help, and make sure to update [[PartialTransaction.key]] after
          // that.
          throw SpeedyHungry(
            SResultNeedKey(
              gkey,
              machine.committers,
              cbMissing = _ => {
                machine.ptx = machine.ptx.copy(keys = machine.ptx.keys + (gkey -> None))
                machine.tryHandleException()
              },
              cbPresent = { contractId =>
                machine.ptx = machine.ptx.copy(keys = machine.ptx.keys + (gkey -> Some(contractId)))
                machine.ctrl = CtrlValue(SContractId(contractId))
              }
            ))
      }
    }
  }

  /** $getTime :: Token -> Timestamp */
  final case object SBGetTime extends SBuiltin(1) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      checkToken(args.get(0))
      // $ugettime :: Token -> Timestamp
      throw SpeedyHungry(
        SResultNeedTime(timestamp => machine.ctrl = CtrlValue(STimestamp(timestamp))))
    }
  }

  /** $beginCommit :: Party -> Token -> () */
  final case class SBSBeginCommit(optLocation: Option[Location]) extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      checkToken(args.get(1))
      machine.committers = extractParties(args.get(0))
      machine.commitLocation = optLocation
      machine.ctrl = CtrlValue(SUnit(()))
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

      def clearCommit(): Unit = {
        machine.committers = Set.empty
        machine.commitLocation = None
        machine.ptx = PartialTransaction.initial
      }

      args.get(0) match {
        case SBool(true) =>
          // update expression threw an exception. we're
          // now done.
          clearCommit
          machine.ctrl = CtrlValue(SUnit(()))
          throw SpeedyHungry(SResultScenarioInsertMustFail(committerOld, commitLocationOld))

        case SBool(false) =>
          ptxOld.finish match {
            case Left(_) =>
              machine.ctrl = CtrlValue(SUnit(()))
              clearCommit
            case Right(tx) =>
              // Transaction finished successfully. It might still
              // fail when committed, so tell the scenario runner to
              // do that.
              machine.ctrl = CtrlValue(SUnit(()))
              throw SpeedyHungry(SResultScenarioMustFail(tx, committerOld, _ => clearCommit))
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
          identity
        )

      throw SpeedyHungry(
        SResultScenarioCommit(
          value = args.get(0),
          tx = tx,
          committers = machine.committers,
          callback = newValue => {
            machine.committers = Set.empty
            machine.commitLocation = None
            machine.ptx = PartialTransaction.initial
            machine.ctrl = CtrlValue(newValue)
          }
        )
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
          timestamp => machine.ctrl = CtrlValue(STimestamp(timestamp))))
    }
  }

  /** $getParty :: Text -> Token -> Party */
  final case object SBSGetParty extends SBuiltin(2) {
    def execute(args: util.ArrayList[SValue], machine: Machine): Unit = {
      checkToken(args.get(1))
      args.get(0) match {
        case SText(name) =>
          throw SpeedyHungry(
            SResultScenarioGetParty(name, party => machine.ctrl = CtrlValue(SParty(party))))
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
          machine.ctrl = CtrlValue(args.get(1))
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

  // Helpers
  //

  /** Check whether the partial transaction has been aborted, and
    * throw if so. The partial transaction abort status must be
    * checked after every operation on it.
    */
  private def checkAborted(ptx: PartialTransaction): Unit =
    ptx.aborted match {
      case Some(ContractNotActive(coid, tid, consumedBy)) =>
        throw DamlELocalContractNotActive(coid, tid, consumedBy)
      case Some(EndExerciseInRootContext) =>
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

  private def extractParties(v: SValue): Set[Party] =
    v match {
      case SList(vs) =>
        vs.iterator.collect {
          case SParty(p) => p
          case x => crash(s"non-party value in list: $x")
        }.toSet
      case SParty(p) =>
        Set(p)
      case _ =>
        crash(s"value not a list of parties or party: $v")
    }

  private def checkLookupMaintainers(
      templateId: Identifier,
      machine: Machine,
      maintainers: Set[Party]): Unit = {
    // This check is dependent on whether we are submitting or validating the transaction.
    // See <https://github.com/digital-asset/daml/issues/1866#issuecomment-506315152>,
    // specifically "Consequently it suffices to implement this check
    // only for the submission. There is no intention to enforce "submitter
    // must be a maintainer" during validation; if we find in the future a
    // way to disclose key information or support interactive submission,
    // then we can lift this restriction without changing the validation
    // parts. In particular, this should not affect whether we have to ship
    // the submitter along with the transaction."
    if (!machine.validating) {
      val submitter = if (machine.committers.size != 1) {
        crash(
          s"expecting exactly one committer since we're not validating, but got ${machine.committers}")
      } else {
        machine.committers.toSeq.head
      }
      if (machine.checkSubmitterInMaintainers) {
        if (!(maintainers.contains(submitter))) {
          throw DamlESubmitterNotInMaintainers(templateId, submitter, maintainers)
        }
      }
    }
  }

  private def rightOrArithmeticError[A](message: String, mb: Either[String, A]): A =
    mb.fold(_ => throw DamlEArithmeticError(s"$message"), identity)

}
