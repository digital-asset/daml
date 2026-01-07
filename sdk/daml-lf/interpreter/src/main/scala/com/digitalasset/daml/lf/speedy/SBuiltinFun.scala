// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.daml.nameof.NameOf
import com.daml.scalautil.Statement.discard
import com.digitalasset.daml.lf.crypto.{Hash, SValueHash}
import com.digitalasset.daml.lf.crypto.Hash.{HashingMethod, hashContractInstance}
import com.digitalasset.daml.lf.data.Numeric.Scale
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.data.support._
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.speedy.metrics.TxNodeCount
import com.digitalasset.daml.lf.speedy.SError._
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.SValue.{SValue => SV, _}
import com.digitalasset.daml.lf.speedy.Speedy._
import com.digitalasset.daml.lf.speedy.{SExpr => runTime}
import com.digitalasset.daml.lf.speedy.compiler.{SExpr0 => compileTime}
import com.digitalasset.daml.lf.transaction.TransactionErrors.{
  AuthFailureDuringExecution,
  DuplicateContractId,
  DuplicateContractKey,
}
import com.digitalasset.daml.lf.transaction.{
  ContractStateMachine,
  FatContractInstance,
  GlobalKey,
  GlobalKeyWithMaintainers,
  SerializationVersion,
  TransactionErrors => TxErr,
}
import com.digitalasset.daml.lf.value.{Value => V}

import java.nio.charset.StandardCharsets
import java.security.{
  InvalidKeyException,
  KeyFactory,
  NoSuchAlgorithmException,
  NoSuchProviderException,
  PublicKey,
  SignatureException,
}
import java.security.spec.{InvalidKeySpecException, X509EncodedKeySpec}
import java.time.Duration
import java.time.temporal.ChronoUnit
import scala.annotation.nowarn
import scala.collection.immutable.{ArraySeq, TreeSet}
import scala.math.Ordering.Implicits.infixOrderingOps

/** Speedy builtins represent LF functional forms. As such, they *always* have a non-zero arity.
  *
  * Speedy builtins are stratified into two layers:
  *  Parent: `SBuiltin`, (which are effectful), and child: `SBuiltinPure` (which are pure).
  *
  *  Effectful builtin functions may ask questions of the ledger or change machine state.
  *  Pure builtins can be treated specially because their evaluation is immediate.
  *  This fact is used by the execution of the ANF expression form: `SELet1Builtin`.
  *
  *  Most builtins are pure, and so they extend `SBuiltinPure`
  */
private[speedy] sealed abstract class SBuiltinFun(val arity: Int) {

  // Helper for constructing expressions applying this builtin.
  // E.g. SBCons(SEVar(1), SEVar(2))

  // TODO: move this into the speedy compiler code
  private[lf] def apply(args: compileTime.SExpr*): compileTime.SExpr =
    compileTime.SEApp(compileTime.SEBuiltin(this), args.toList)

  // TODO: avoid constructing application expression at run time
  // This helper is used (only?) by TransactinVersionTest.
  private[lf] def apply(args: runTime.SExprAtomic*): runTime.SExpr =
    runTime.SEAppAtomic(runTime.SEBuiltinFun(this), args.to(ArraySeq))

  /** Execute the builtin with 'arity' number of arguments in 'args'.
    * Updates the machine state accordingly.
    */
  private[speedy] def execute[Q](args: ArraySeq[SValue], machine: Machine[Q]): Control[Q]
}

private[speedy] sealed abstract class SBuiltinPure(arity: Int) extends SBuiltinFun(arity) {

  /** Pure builtins do not modify the machine state and do not ask questions of the ledger. As a result, pure builtin
    * execution is immediate.
    *
    * @param args arguments for executing the pure builtin
    * @param machine the Speedy machine (machine state may be modified by the builtin)
    * @return the pure builtin's resulting value (wrapped as a Control value)
    */
  private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SValue

  private[speedy] final override def execute[Q](
      args: ArraySeq[SValue],
      machine: Machine[Q],
  ): Control.Value =
    Control.Value(executePure(args, machine))

}

private[speedy] sealed abstract class UpdateBuiltin(arity: Int)
    extends SBuiltinFun(arity)
    with Product {

  /** On ledger builtins may reference the Speedy machine's ledger state.
    *
    * @param args arguments for executing the builtin
    * @param machine the Speedy machine (machine state may be modified by the builtin)
    * @return the builtin execution's resulting control value
    */
  protected def executeUpdate(
      args: ArraySeq[SValue],
      machine: UpdateMachine,
  ): Control[Question.Update]

  override private[speedy] final def execute[Q](
      args: ArraySeq[SValue],
      machine: Machine[Q],
  ): Control[Q] =
    machine.asUpdateMachine(productPrefix)(executeUpdate(args, _))
}

private[lf] object SBuiltinFun {

  def executeExpression[Q](machine: Machine[Q], expr: SExpr)(
      f: SValue => Control[Q]
  ): Control[Q] = {
    machine.pushKont(KPure(f))
    Control.Expression(expr)
  }

  protected def crash(msg: String): Nothing =
    throw SErrorCrash(getClass.getCanonicalName, msg)

  protected def unexpectedType(i: Int, expected: String, found: SValue): Nothing =
    crash(s"type mismatch of argument $i: expect $expected but got $found")

  final protected def getSBool(args: ArraySeq[SValue], i: Int): Boolean =
    args(i) match {
      case SBool(value) => value
      case otherwise => unexpectedType(i, "SBool", otherwise)
    }

  final protected def getSUnit(args: ArraySeq[SValue], i: Int): Unit =
    args(i) match {
      case SUnit => ()
      case otherwise => unexpectedType(i, "SUnit", otherwise)
    }

  final protected def getSInt64(args: ArraySeq[SValue], i: Int): Long =
    args(i) match {
      case SInt64(value) => value
      case otherwise => unexpectedType(i, "SInt64", otherwise)
    }

  final protected def getSText(args: ArraySeq[SValue], i: Int): String =
    args(i) match {
      case SText(value) => value
      case otherwise => unexpectedType(i, "SText", otherwise)
    }

  final protected def getSNumeric(args: ArraySeq[SValue], i: Int): Numeric =
    args(i) match {
      case SNumeric(value) => value
      case otherwise => unexpectedType(i, "SNumeric", otherwise)
    }

  final protected def getSDate(args: ArraySeq[SValue], i: Int): Time.Date =
    args(i) match {
      case SDate(value) => value
      case otherwise => unexpectedType(i, "SDate", otherwise)
    }

  final protected def getSTimestamp(args: ArraySeq[SValue], i: Int): Time.Timestamp =
    args(i) match {
      case STimestamp(value) => value
      case otherwise => unexpectedType(i, "STimestamp", otherwise)
    }

  final protected def getSScale(args: ArraySeq[SValue], i: Int): Numeric.Scale =
    Numeric.scale(getSNumeric(args, i))

  final protected def getSParty(args: ArraySeq[SValue], i: Int): Party =
    args(i) match {
      case SParty(value) => value
      case otherwise => unexpectedType(i, "SParty", otherwise)
    }

  final protected def getSContractId(args: ArraySeq[SValue], i: Int): V.ContractId =
    args(i) match {
      case SContractId(value) => value
      case otherwise => unexpectedType(i, "SContractId", otherwise)
    }

  final protected def getSBigNumeric(args: ArraySeq[SValue], i: Int): java.math.BigDecimal =
    args(i) match {
      case SBigNumeric(value) => value
      case otherwise => unexpectedType(i, "SBigNumeric", otherwise)
    }

  final protected def getSList(args: ArraySeq[SValue], i: Int): FrontStack[SValue] =
    args(i) match {
      case SList(value) => value
      case otherwise => unexpectedType(i, "SList", otherwise)
    }

  final protected def getSOptional(args: ArraySeq[SValue], i: Int): Option[SValue] =
    args(i) match {
      case SOptional(value) => value
      case otherwise => unexpectedType(i, "SOptional", otherwise)
    }

  final protected def getSMap(args: ArraySeq[SValue], i: Int): SMap =
    args(i) match {
      case genMap: SMap => genMap
      case otherwise => unexpectedType(i, "SMap", otherwise)
    }

  final protected def getSMapKey(args: ArraySeq[SValue], i: Int): SValue = {
    val key = args(i)
    SMap.comparable(key)
    key
  }

  final protected def getSRecord(args: ArraySeq[SValue], i: Int): SRecord =
    args(i) match {
      case record: SRecord => record
      case otherwise => unexpectedType(i, "SRecord", otherwise)
    }

  final protected def getSStruct(args: ArraySeq[SValue], i: Int): SStruct =
    args(i) match {
      case struct: SStruct => struct
      case otherwise => unexpectedType(i, "SStruct", otherwise)
    }

  final protected def getSAny(args: ArraySeq[SValue], i: Int): SAny =
    args(i) match {
      case any: SAny => any
      case otherwise => unexpectedType(i, "SAny", otherwise)
    }

  final protected def getSTypeRep(args: ArraySeq[SValue], i: Int): Ast.Type =
    args(i) match {
      case STypeRep(ty) => ty
      case otherwise => unexpectedType(i, "STypeRep", otherwise)
    }

  final protected def getSAnyException(args: ArraySeq[SValue], i: Int): SRecord =
    args(i) match {
      case SAnyException(exception) => exception
      case otherwise => unexpectedType(i, "Exception", otherwise)
    }

  final protected def getSAnyContract(
      args: ArraySeq[SValue],
      i: Int,
  ): (TypeConId, SRecord) =
    args(i) match {
      case SAny(Ast.TTyCon(tyCon), record: SRecord) =>
        assert(tyCon == record.id)
        (tyCon, record)
      case otherwise => unexpectedType(i, "AnyContract", otherwise)
    }

  final protected def getSPAP(values: ArraySeq[SValue], i: Int): SPAP =
    values(i) match {
      case pap: SPAP => pap
      case otherwise => unexpectedType(i, "SPAP", otherwise)
    }

  final protected def checkToken(args: ArraySeq[SValue], i: Int): Unit =
    args(i) match {
      case SToken => ()
      case otherwise => unexpectedType(i, "SToken", otherwise)
    }

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

  sealed abstract class SBuiltinArithmetic(val name: String, arity: Int)
      extends SBuiltinFun(arity) {
    private[speedy] def compute(args: ArraySeq[SValue]): Option[SValue]

    private[speedy] def buildException[Q](machine: Machine[Q], args: ArraySeq[SValue]) =
      machine.sArithmeticError(
        name,
        args.view.map(litToText(getClass.getCanonicalName, _)).to(ImmArray),
      )

    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control[Nothing] =
      compute(args) match {
        case Some(value) =>
          Control.Value(value)
        case None =>
          machine.handleException(buildException(machine, args))
      }
  }

  sealed abstract class SBBinaryOpInt64(name: String, op: (Long, Long) => Option[Long])
      extends SBuiltinArithmetic(name, 2) {
    override private[speedy] def compute(args: ArraySeq[SValue]): Option[SValue] =
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
      extends SBuiltinArithmetic(name, 2) {
    override private[speedy] def compute(args: ArraySeq[SValue]): Option[SValue] = {
      val a = getSNumeric(args, 0)
      val b = getSNumeric(args, 1)
      op(a, b).map(SNumeric(_))
    }
  }

  sealed abstract class SBBinaryOpNumeric2(
      name: String,
      op: (Scale, Numeric, Numeric) => Option[Numeric],
  ) extends SBuiltinArithmetic(name, 3) {
    override private[speedy] def compute(args: ArraySeq[SValue]): Option[SValue] = {
      val scale = getSScale(args, 0)
      val a = getSNumeric(args, 1)
      val b = getSNumeric(args, 2)
      op(scale, a, b).map(SNumeric(_))
    }
  }

  final case object SBAddNumeric extends SBBinaryOpNumeric("ADD_NUMERIC", add)
  final case object SBSubNumeric extends SBBinaryOpNumeric("SUB_NUMERIC", subtract)
  final case object SBMulNumeric extends SBBinaryOpNumeric2("MUL_NUMERIC", multiply)
  final case object SBDivNumeric extends SBBinaryOpNumeric2("DIV_NUMERIC", divide)

  final case object SBRoundNumeric extends SBuiltinArithmetic("ROUND_NUMERIC", 2) {
    override private[speedy] def compute(args: ArraySeq[SValue]): Option[SNumeric] = {
      val prec = getSInt64(args, 0)
      val x = getSNumeric(args, 1)
      Numeric.round(prec, x).toOption.map(SNumeric(_))
    }
  }

  final case object SBCastNumeric extends SBuiltinArithmetic("CAST_NUMERIC", 2) {
    override private[speedy] def compute(args: ArraySeq[SValue]): Option[SNumeric] = {
      val outputScale = getSScale(args, 0)
      val x = getSNumeric(args, 1)
      Numeric.fromBigDecimal(outputScale, x).toOption.map(SNumeric(_))
    }
  }

  final case object SBShiftNumeric extends SBuiltinPure(2) {
    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SNumeric = {
      val outputScale = getSScale(args, 0)
      val x = getSNumeric(args, 1)

      machine.updateGasBudget(_.BShiftNumeric.cost(outputScale, x))

      val inputScale = x.scale
      SNumeric(
        Numeric.assertFromBigDecimal(outputScale, x.scaleByPowerOfTen(inputScale - outputScale))
      )
    }
  }

  //
  // Text functions
  //
  final case object SBExplodeText extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SList = {
      val arg0 = getSText(args, 0)

      machine.updateGasBudget(_.BExplodeText.cost(arg0))

      SList(FrontStack.from(Utf8.explode(arg0).map(SText)))
    }
  }

  final case object SBImplodeText extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SText = {
      val xs = getSList(args, 0)
      val ts = xs.map {
        case SText(t) => t
        case v => crash(s"type mismatch implodeText: expected SText, got $v")
      }

      machine.updateGasBudget(_.BImplodeText.cost(xs))

      SText(Utf8.implode(ts.toImmArray))
    }
  }

  final case object SBAppendText extends SBuiltinPure(2) {
    override private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SText = {
      val arg0 = getSText(args, 0)
      val arg1 = getSText(args, 1)

      machine.updateGasBudget(_.BAppendText.cost(arg0, arg1))

      SText(arg0 + arg1)
    }
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
      case _: SContractId | SToken | _: SAny | _: SEnum | _: SList | _: SMap | _: SOptional |
          _: SPAP | _: SRecord | _: SStruct | _: STypeRep | _: SVariant =>
        throw SErrorCrash(location, s"litToText: unexpected $x")
    }

  final case object SBToText extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SText = {

      SText(litToText(NameOf.qualifiedNameOfCurrentFunc, args(0)))
    }
  }

  final case object SBContractIdToText extends SBuiltinPure(1) {
    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SOptional = {
      val coid = getSContractId(args, 0).coid
      machine match {
        case _: PureMachine =>
          SOptional(Some(SText(coid)))
        case _: UpdateMachine =>
          SValue.SValue.None
      }
    }
  }

  final case object SBPartyToQuotedText extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SText = {
      val arg0 = getSParty(args, 0)

      machine.updateGasBudget(_.BPartyToText.cost(arg0))

      SText(s"'${arg0: String}'")
    }
  }

  final case object SBCodePointsToText extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SText = {
      val codePoints = getSList(args, 0).map(_.asInstanceOf[SInt64].value)

      machine.updateGasBudget(_.BCodePointsToText.cost(codePoints))

      Utf8.pack(codePoints.toImmArray) match {
        case Right(value) =>
          SText(value)
        case Left(cp) =>
          crash(s"invalid code point 0x${cp.toHexString}.")
      }
    }
  }

  final case object SBTextToParty extends SBuiltinPure(1) {
    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SOptional = {
      val arg0 = getSText(args, 0)

      machine.updateGasBudget(_.BTextToParty.cost(arg0))

      Party.fromString(arg0) match {
        case Left(_) => SV.None
        case Right(p) => SOptional(Some(SParty(p)))
      }
    }
  }

  final case object SBTextToInt64 extends SBuiltinPure(1) {
    private val pattern = """[+-]?\d+""".r.pattern

    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SOptional = {
      val s = getSText(args, 0)

      machine.updateGasBudget(_.BTextToInt64.cost(s))

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

    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SOptional = {
      val scale = getSScale(args, 0)
      val string = getSText(args, 1)

      machine.updateGasBudget(_.BTextToNumeric.cost(scale, string))

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
    override private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SList = {
      val string = getSText(args, 0)

      machine.updateGasBudget(_.BTextToCodePoints.cost(string))

      val codePoints = Utf8.unpack(string)
      SList(FrontStack.from(codePoints.map(SInt64)))
    }
  }

  final case object SBSHA256Text extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SText = {

      machine.updateGasBudget(_.BSHA256Text.cost(getSText(args, 0)))

      SText(Utf8.sha256(Utf8.getBytes(getSText(args, 0))))
    }
  }

  final case object SBSHA256Hex extends SBuiltinFun(1) {
    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control[Nothing] = {
      val hex = getSText(args, 0)

      machine.updateGasBudget(_.BSHA256Text.cost(hex))

      Ref.HexString.fromString(hex) match {
        case Right(s) =>
          Control.Value(SText(Utf8.sha256(Ref.HexString.decode(s))))
        case Left(_) =>
          Control.Error(
            IE.Crypto(
              IE.Crypto.MalformedByteEncoding(hex, "can not parse hex string")
            )
          )
      }
    }
  }

  final case object SBKECCAK256Text extends SBuiltinFun(1) {
    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control[Q] = {
      try {
        Control.Value(
          SText(crypto.MessageDigest.digest(Ref.HexString.assertFromString(getSText(args, 0))))
        )
      } catch {
        case _: IllegalArgumentException =>
          Control.Error(
            IE.Crypto(
              IE.Crypto.MalformedByteEncoding(getSText(args, 0), "can not parse hex string")
            )
          )
      }
    }
  }

  final case object SBSECP256K1Bool extends SBuiltinFun(3) {
    private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control[Q] = {
      val arg1 = getSText(args, 1)

      Ref.HexString.fromString(arg1) match {
        case Right(message) =>
          val messageDigest = Utf8.sha256(Ref.HexString.decode(message))
          SBSECP256K1WithEcdsaBool.execute(args.updated(1, SText(messageDigest)), machine)

        case Left(_) =>
          Control.Error(
            IE.Crypto(
              IE.Crypto.MalformedByteEncoding(
                arg1,
                cause = "can not parse message hex string",
              )
            )
          )
      }
    }
  }

  final case object SBSECP256K1WithEcdsaBool extends SBuiltinFun(3) {
    private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control[Q] = {
      try {
        val result = for {
          signature <- Ref.HexString
            .fromString(getSText(args, 0))
            .left
            .map(_ =>
              IE.Crypto(
                IE.Crypto.MalformedByteEncoding(
                  getSText(args, 0),
                  cause = "can not parse signature hex string",
                )
              )
            )
          message <- Ref.HexString
            .fromString(getSText(args, 1))
            .left
            .map(_ =>
              IE.Crypto(
                IE.Crypto.MalformedByteEncoding(
                  getSText(args, 1),
                  cause = "can not parse message hex string",
                )
              )
            )
          derEncodedPublicKey <- Ref.HexString
            .fromString(getSText(args, 2))
            .left
            .map(_ =>
              IE.Crypto(
                IE.Crypto.MalformedByteEncoding(
                  getSText(args, 2),
                  cause = "can not parse DER encoded public key hex string",
                )
              )
            )
          publicKey = extractPublicKey(derEncodedPublicKey)
        } yield {
          SBool(crypto.MessageSignature.verify(signature, message, publicKey))
        }

        result.fold(Control.Error, Control.Value)
      } catch {
        case _: IllegalArgumentException =>
          Control.Error(
            IE.Crypto(
              IE.Crypto.MalformedByteEncoding(
                getSText(args, 2),
                cause = "can not parse DER encoded public key hex string",
              )
            )
          )
        case exn: InvalidKeyException =>
          Control.Error(
            IE.Crypto(
              IE.Crypto.MalformedKey(getSText(args, 2), exn.getMessage)
            )
          )
        case exn: InvalidKeySpecException =>
          Control.Error(
            IE.Crypto(
              IE.Crypto.MalformedKey(getSText(args, 2), exn.getMessage)
            )
          )
        case exn: SignatureException =>
          Control.Error(
            IE.Crypto(
              IE.Crypto.MalformedSignature(getSText(args, 0), exn.getMessage)
            )
          )
        case _: NoSuchProviderException =>
          crash("JCE Provider BouncyCastle not found")
        case _: NoSuchAlgorithmException =>
          crash("BouncyCastle provider fails to support SECP256K1")
      }
    }

    @throws(classOf[IllegalArgumentException])
    @throws(classOf[NoSuchAlgorithmException])
    @throws(classOf[InvalidKeySpecException])
    private[speedy] def extractPublicKey(hexEncodedPublicKey: Ref.HexString): PublicKey = {
      val byteEncodedPublicKey = Ref.HexString.decode(hexEncodedPublicKey).toByteArray

      KeyFactory.getInstance("EC").generatePublic(new X509EncodedKeySpec(byteEncodedPublicKey))
    }
  }

  final case object SBSECP256K1ValidateKey extends SBuiltinFun(1) {
    private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control[Q] = {
      try {
        val result = for {
          derEncodedPublicKey <- Ref.HexString
            .fromString(getSText(args, 0))
            .left
            .map(_ =>
              IE.Crypto(
                IE.Crypto.MalformedByteEncoding(
                  getSText(args, 0),
                  cause = "can not parse DER encoded public key hex string",
                )
              )
            )
          publicKey = extractPublicKey(derEncodedPublicKey)
        } yield {
          SBool(crypto.MessageSignature.validateKey(publicKey))
        }

        result.fold(Control.Error, Control.Value)
      } catch {
        case _: IllegalArgumentException =>
          Control.Error(
            IE.Crypto(
              IE.Crypto.MalformedByteEncoding(
                getSText(args, 0),
                cause = "can not parse DER encoded public key hex string",
              )
            )
          )
        case exn: InvalidKeyException =>
          Control.Error(
            IE.Crypto(
              IE.Crypto.MalformedKey(getSText(args, 0), exn.getMessage)
            )
          )
        case exn: InvalidKeySpecException =>
          Control.Error(
            IE.Crypto(
              IE.Crypto.MalformedKey(getSText(args, 0), exn.getMessage)
            )
          )
        case _: NoSuchProviderException =>
          crash("JCE Provider BouncyCastle not found")
        case _: NoSuchAlgorithmException =>
          crash("BouncyCastle provider fails to support SECP256K1")
      }
    }

    @throws(classOf[IllegalArgumentException])
    @throws(classOf[NoSuchAlgorithmException])
    @throws(classOf[InvalidKeySpecException])
    private[speedy] def extractPublicKey(hexEncodedPublicKey: Ref.HexString): PublicKey = {
      val byteEncodedPublicKey = Ref.HexString.decode(hexEncodedPublicKey).toByteArray

      KeyFactory.getInstance("EC").generatePublic(new X509EncodedKeySpec(byteEncodedPublicKey))
    }
  }

  final case object SBDecodeHex extends SBuiltinFun(1) {
    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control[Q] = {
      val arg = getSText(args, 0)

      Ref.HexString
        .fromString(arg)
        .fold(
          _ =>
            Control.Error(
              IE.Crypto(
                IE.Crypto.MalformedByteEncoding(
                  arg,
                  cause = "can not parse hex string argument",
                )
              )
            ),
          hexArg => {
            val result =
              new String(Ref.HexString.decode(hexArg).toByteArray, StandardCharsets.UTF_8)
            Control.Value(SText(result))
          },
        )
    }
  }

  final case object SBEncodeHex extends SBuiltinPure(1) {
    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SValue = {
      val arg0 = getSText(args, 0)

      machine.updateGasBudget(_.BEncodeHex.cost(arg0))

      val hexArg = Ref.HexString.encode(Bytes.fromStringUtf8(arg0))
      SText(hexArg)
    }
  }

  final case object SBFoldl extends SBuiltinFun(3) {
    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control.Value = {
      val func = getSPAP(args, 0)
      val init = args(1)
      val list = getSList(args, 2)
      machine.pushKont(KFoldl(machine, func, list))
      Control.Value(init)
    }
  }

  final case object SBFoldr extends SBuiltinFun(3) {
    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control[Q] = {
      val func = getSPAP(args, 0)
      val init = args(1)
      val list = getSList(args, 2)
      val array = list.toImmArray
      machine.pushKont(KFoldr(machine, func, array, array.length))
      Control.Value(init)
    }
  }

  final case object SBMapToList extends SBuiltinPure(1) {

    override private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SList = {
      val arg0 = getSMap(args, 0)

      if (arg0.isTextMap) {
        machine.updateGasBudget(_.BTextMapToList.cost(arg0))
      } else {
        machine.updateGasBudget(_.BGenMapToList.cost(arg0))
      }

      SValue.toList(arg0.entries)
    }
  }

  final case object SBMapInsert extends SBuiltinPure(3) {
    override private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SMap = {
      val arg0 = getSMapKey(args, 0)
      val arg1 = args(1)
      val arg2 = getSMap(args, 2)

      if (arg2.isTextMap) {
        machine.updateGasBudget(_.BTextMapInsert.cost(arg0, arg1, arg2))
      } else {
        machine.updateGasBudget(_.BGenMapInsert.cost(arg0, arg1, arg2))
      }

      arg2.insert(arg0, arg1)
    }
  }

  final case object SBMapLookup extends SBuiltinPure(2) {
    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SOptional = {
      val arg0 = getSMapKey(args, 0)
      val arg1 = getSMap(args, 1)

      if (arg1.isTextMap) {
        machine.updateGasBudget(_.BTextMapLookup.cost(arg0, arg1))
      } else {
        machine.updateGasBudget(_.BGenMapLookup.cost(arg0, arg1))
      }

      SOptional(arg1.get(arg0))
    }
  }

  final case object SBMapDelete extends SBuiltinPure(2) {
    override private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SMap = {
      val arg0 = getSMapKey(args, 0)
      val arg1 = getSMap(args, 1)

      if (arg1.isTextMap) {
        machine.updateGasBudget(_.BTextMapDelete.cost(arg0, arg1))
      } else {
        machine.updateGasBudget(_.BGenMapDelete.cost(arg0, arg1))
      }

      arg1.delete(arg0)
    }
  }

  final case object SBMapKeys extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SList = {
      val arg0 = getSMap(args, 0)

      machine.updateGasBudget(_.BGenMapKeys.cost(arg0))

      SList(arg0.entries.keys.to(FrontStack))
    }
  }

  final case object SBMapValues extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SList = {
      val arg0 = getSMap(args, 0)

      machine.updateGasBudget(_.BGenMapValues.cost(arg0))

      SList(arg0.entries.values.to(FrontStack))
    }
  }

  final case object SBMapSize extends SBuiltinPure(1) {
    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SInt64 = {
      val arg0 = getSMap(args, 0)

      if (arg0.isTextMap) {
        machine.updateGasBudget(_.BTextMapSize.cost(arg0))
      } else {
        machine.updateGasBudget(_.BGenMapSize.cost(arg0))
      }

      SInt64(arg0.entries.size.toLong)
    }
  }

  //
  // Conversions
  //

  final case object SBInt64ToNumeric extends SBuiltinArithmetic("INT64_TO_NUMERIC", 2) {
    override private[speedy] def compute(args: ArraySeq[SValue]): Option[SNumeric] = {
      val scale = getSScale(args, 0)
      val x = getSInt64(args, 1)
      Numeric.fromLong(scale, x).toOption.map(SNumeric(_))
    }
  }

  final case object SBNumericToInt64 extends SBuiltinArithmetic("NUMERIC_TO_INT64", 1) {
    override private[speedy] def compute(args: ArraySeq[SValue]): Option[SInt64] = {
      val x = getSNumeric(args, 0)
      Numeric.toLong(x).toOption.map(SInt64)
    }
  }

  final case object SBDateToUnixDays extends SBuiltinPure(1) {
    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SInt64 = {
      val date = getSDate(args, 0)

      machine.updateGasBudget(_.BDateToUnixDays.cost(date))

      SInt64(date.days.toLong)
    }
  }

  final case object SBUnixDaysToDate extends SBuiltinArithmetic("UNIX_DAYS_TO_DATE", 1) {
    override private[speedy] def compute(args: ArraySeq[SValue]): Option[SDate] = {
      val days = getSInt64(args, 0)
      Time.Date.asInt(days).flatMap(Time.Date.fromDaysSinceEpoch).toOption.map(SDate)
    }
  }

  final case object SBTimestampToUnixMicroseconds extends SBuiltinPure(1) {
    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SInt64 = {
      val arg0 = getSTimestamp(args, 0)

      machine.updateGasBudget(_.BTimestampToUnixMicroseconds.cost(arg0))

      SInt64(arg0.micros)
    }
  }

  final case object SBUnixMicrosecondsToTimestamp
      extends SBuiltinArithmetic("UNIX_MICROSECONDS_TO_TIMESTAMP", 1) {
    override private[speedy] def compute(args: ArraySeq[SValue]): Option[STimestamp] = {
      val micros = getSInt64(args, 0)
      Time.Timestamp.fromLong(micros).toOption.map(STimestamp)
    }
  }

  //
  // Equality and comparisons
  //
  final case object SBEqual extends SBuiltinPure(2) {
    override private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SBool = {
      val arg0 = args(0)
      val arg1 = args(1)

      machine.updateGasBudget(_.BEqual.cost(arg0, arg1))

      SBool(svalue.Equality.areEqual(arg0, arg1))
    }
  }

  sealed abstract class SBCompare(pred: Int => Boolean) extends SBuiltinPure(2) {
    override private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SBool = {
      val arg0 = args(0)
      val arg1 = args(1)

      machine.updateGasBudget(_.BEqual.cost(arg0, arg1))

      SBool(pred(svalue.Ordering.compare(arg0, arg1)))
    }
  }

  final case object SBLess extends SBCompare(_ < 0)
  final case object SBLessEq extends SBCompare(_ <= 0)
  final case object SBGreater extends SBCompare(_ > 0)
  final case object SBGreaterEq extends SBCompare(_ >= 0)

  /** $consMany[n] :: a -> ... -> List a -> List a */
  final case class SBConsMany(n: Int) extends SBuiltinPure(1 + n) {
    override private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SList = {
      SList(args.view.slice(0, n).to(ImmArray) ++: getSList(args, n))
    }
  }

  /** $cons :: a -> List a -> List a */
  final case object SBCons extends SBuiltinPure(2) {
    override private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SList = {
      val headSValue = args(0)
      val tailSList = getSList(args, 1)
      SList(headSValue +: tailSList)
    }
  }

  /** $some :: a -> Optional a */
  final case object SBSome extends SBuiltinPure(1) {
    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SOptional = {
      SOptional(Some(args(0)))
    }
  }

  /** $rcon[R, fields] :: a -> b -> ... -> R */
  final case class SBRecCon(id: Identifier, fields: ImmArray[Name])
      extends SBuiltinPure(fields.length) {
    override private[speedy] final def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SValue = {
      SRecord(id, fields, args)
    }
  }

  /** $rupd[R, field] :: R -> a -> R */
  final case class SBRecUpd(id: Identifier, field: Int) extends SBuiltinPure(2) {
    override private[speedy] final def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SValue = {
      val record = getSRecord(args, 0)
      if (record.id != id) {
        crash(s"type mismatch on record update: expected $id, got record of type ${record.id}")
      }
      record.copy(values = record.values.updated(field, args(1)))
    }
  }

  /** $rupdmulti[R, [field_1, ..., field_n]] :: R -> a_1 -> ... -> a_n -> R */
  final case class SBRecUpdMulti(id: Identifier, updateFields: List[Int])
      extends SBuiltinPure(1 + updateFields.length) {
    override private[speedy] final def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SValue = {
      val record = getSRecord(args, 0)
      if (record.id != id) {
        crash(s"type mismatch on record update: expected $id, got record of type ${record.id}")
      }
      val values2 = record.values.toArray
      (updateFields.iterator zip args.iterator.drop(1)).foreach { case (updateField, arg) =>
        values2(updateField) = arg
      }
      record.copy(values = ArraySeq.unsafeWrapArray(values2))
    }
  }

  /** $rproj[R, field] :: R -> a */
  final case class SBRecProj(id: Identifier, field: Int) extends SBuiltinPure(1) {
    override private[speedy] final def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SValue = {
      getSRecord(args, 0).values(field)
    }
  }

  // SBStructCon sorts the field after evaluation of its arguments to preserve
  // evaluation order of unordered fields.
  /** $tcon[fields] :: a -> b -> ... -> Struct */
  final case class SBStructCon(inputFieldsOrder: Struct[Int])
      extends SBuiltinPure(inputFieldsOrder.size) {
    private[this] val fieldNames = inputFieldsOrder.mapValues(_ => ())
    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SStruct = {
      val sortedFields = new Array[SValue](inputFieldsOrder.size)
      var j = 0
      inputFieldsOrder.values.foreach { i =>
        sortedFields(j) = args(i)
        j += 1
      }
      SStruct(fieldNames, ArraySeq.unsafeWrapArray(sortedFields))
    }
  }

  /** $tproj[field] :: Struct -> a */
  final case class SBStructProj(field: Ast.FieldName) extends SBuiltinPure(1) {
    // The variable `fieldIndex` is used to cache the (logarithmic) evaluation
    // of `struct.fieldNames.indexOf(field)` at the first call in order to
    // avoid its reevaluations, hence obtaining an amortized constant
    // complexity.
    private[this] var fieldIndex = -1
    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SValue = {
      val struct = getSStruct(args, 0)
      if (fieldIndex < 0) fieldIndex = struct.fieldNames.indexOf(field)
      struct.values(fieldIndex)
    }
  }

  /** $tupd[field] :: Struct -> a -> Struct */
  final case class SBStructUpd(field: Ast.FieldName) extends SBuiltinPure(2) {

    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SStruct = {
      val struct = getSStruct(args, 0)

      machine.updateGasBudget(_.EStructUp.cost(struct.values.size))

      struct.copy(values = struct.values.updated(struct.fieldNames.indexOf(field), args(1)))
    }
  }

  /** $vcon[V, variant] :: a -> V */
  final case class SBVariantCon(id: Identifier, variant: Ast.VariantConName, constructorRank: Int)
      extends SBuiltinPure(1) {
    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SVariant = {

      machine.updateGasBudget(_.EVariantCon.cost)

      SVariant(id, variant, constructorRank, args(0))
    }
  }

  final object SBScaleBigNumeric extends SBuiltinPure(1) {
    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SInt64 = {
      val arg0 = getSBigNumeric(args, 0)

      SInt64(arg0.scale().toLong)
    }
  }

  final object SBPrecisionBigNumeric extends SBuiltinPure(1) {
    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SInt64 = {
      val arg0 = getSBigNumeric(args, 0)

      SInt64(arg0.precision().toLong)
    }
  }

  final object SBAddBigNumeric extends SBuiltinArithmetic("ADD_BIGNUMERIC", 2) {
    override private[speedy] def compute(args: ArraySeq[SValue]): Option[SBigNumeric] = {
      val x = getSBigNumeric(args, 0)
      val y = getSBigNumeric(args, 1)
      SBigNumeric.fromBigDecimal(x add y).toOption
    }
  }

  final object SBSubBigNumeric extends SBuiltinArithmetic("SUB_BIGNUMERIC", 2) {
    override private[speedy] def compute(args: ArraySeq[SValue]): Option[SBigNumeric] = {
      val x = getSBigNumeric(args, 0)
      val y = getSBigNumeric(args, 1)
      SBigNumeric.fromBigDecimal(x subtract y).toOption
    }
  }

  final object SBMulBigNumeric extends SBuiltinArithmetic("MUL_BIGNUMERIC", 2) {
    override private[speedy] def compute(args: ArraySeq[SValue]): Option[SBigNumeric] = {
      val x = getSBigNumeric(args, 0)
      val y = getSBigNumeric(args, 1)
      SBigNumeric.fromBigDecimal(x multiply y).toOption
    }
  }

  final object SBDivBigNumeric extends SBuiltinArithmetic("DIV_BIGNUMERIC", 4) {
    override private[speedy] def compute(args: ArraySeq[SValue]): Option[SBigNumeric] = {
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
    override private[speedy] def compute(args: ArraySeq[SValue]): Option[SBigNumeric] = {
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

  final object SBNumericToBigNumeric extends SBuiltinPure(1) {
    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SBigNumeric = {
      val x = getSNumeric(args, 0)

      SBigNumeric.fromNumeric(x)
    }
  }

  final object SBBigNumericToNumeric extends SBuiltinArithmetic("BIGNUMERIC_TO_NUMERIC", 2) {
    override private[speedy] def compute(args: ArraySeq[SValue]): Option[SNumeric] = {
      val scale = getSScale(args, 0)
      val x = getSBigNumeric(args, 1)
      Numeric.fromBigDecimal(scale, x).toOption.map(SNumeric(_))
    }
  }

  final case class SBUCreate(templateId: Identifier) extends UpdateBuiltin(1) {
    override protected def executeUpdate(
        args: ArraySeq[SValue],
        machine: UpdateMachine,
    ): Control[Question.Update] = {
      val templateArg: SValue = args(0)

      computeContractInfo(
        machine,
        templateId,
        templateArg,
        allowCatchingContractInfoErrors = true,
      ) { contract =>
        contract.keyOpt match {
          case Some(contractKey) if contractKey.maintainers.isEmpty =>
            Control.Error(
              IE.CreateEmptyContractKeyMaintainers(
                contract.templateId,
                contract.arg,
                contractKey.lfValue,
              )
            )
          case _ =>
            machine.ptx
              .insertCreate(
                preparationTime = machine.preparationTime,
                contract = contract,
                optLocation = machine.getLastLocation,
                contractIdVersion = machine.contractIdVersion,
              ) match {
              case Right((coid, newPtx)) =>
                machine.enforceLimitSignatoriesAndObservers(coid, contract)
                machine.storeLocalContract(coid, templateId, templateArg)
                machine.ptx = newPtx
                machine.insertContractInfoCache(coid, contract)
                machine.metrics.incrCount[TxNodeCount]()
                Control.Value(SContractId(coid))

              case Left((newPtx, err)) =>
                machine.ptx = newPtx // Seems wrong. But one test in ScriptService requires this.
                Control.Error(convTxError(err))
            }
        }
      }
    }
  }

  /** $beginExercise
    *    :: arg                                           0 (choice argument)
    *    -> ContractId arg                                1 (contract to exercise)
    *    -> List Party                                    2 (choice controllers)
    *    -> List Party                                    3 (choice observers)
    *    -> List Party                                    4 (choice authorizers)
    *    -> ()
    */
  final case class SBUBeginExercise(
      templateId: TypeConId,
      interfaceId: Option[TypeConId],
      choiceId: ChoiceName,
      consuming: Boolean,
      byKey: Boolean,
      explicitChoiceAuthority: Boolean,
  ) extends UpdateBuiltin(6) {

    override protected def executeUpdate(
        args: ArraySeq[SValue],
        machine: UpdateMachine,
    ): Control[Question.Update] = {

      val coid = getSContractId(args, 1)
      val templateArg: SValue = args(5)

      getContractInfo(
        machine,
        coid,
        templateId,
        templateArg,
      ) { contract =>
        val templateVersion = machine.tmplId2TxVersion(templateId)
        val pkgName = machine.tmplId2PackageName(templateId)
        val interfaceVersion = interfaceId.map(machine.tmplId2TxVersion)
        val exerciseVersion = interfaceVersion.fold(templateVersion)(_.max(templateVersion))
        val chosenValue = args(0).toNormalizedValue
        val controllers = extractParties(NameOf.qualifiedNameOfCurrentFunc, args(2))
        machine.enforceChoiceControllersLimit(
          controllers,
          coid,
          templateId,
          choiceId,
          chosenValue,
        )
        val obsrs = extractParties(NameOf.qualifiedNameOfCurrentFunc, args(3))
        machine.enforceChoiceObserversLimit(obsrs, coid, templateId, choiceId, chosenValue)
        val choiceAuthorizers =
          if (explicitChoiceAuthority) {
            val authorizers = extractParties(NameOf.qualifiedNameOfCurrentFunc, args(4))
            machine.enforceChoiceAuthorizersLimit(
              authorizers,
              coid,
              templateId,
              choiceId,
              chosenValue,
            )
            Some(authorizers)
          } else {
            require(args(4) == SValue.SValue.EmptyList)
            None
          }

        machine.ptx
          .beginExercises(
            packageName = pkgName,
            templateId = templateId,
            targetId = coid,
            contract = contract,
            interfaceId = interfaceId,
            choiceId = choiceId,
            optLocation = machine.getLastLocation,
            consuming = consuming,
            actingParties = controllers,
            choiceObservers = obsrs,
            choiceAuthorizers = choiceAuthorizers,
            byKey = byKey,
            chosenValue = chosenValue,
            version = exerciseVersion,
          ) match {
          case Right(ptx) =>
            machine.ptx = ptx
            machine.metrics.incrCount[TxNodeCount]()
            Control.Value(SUnit)
          case Left(err) =>
            Control.Error(convTxError(err))
        }
      }
    }
  }

  private[this] def getInterfaceInstance(
      machine: Machine[_],
      interfaceId: TypeConId,
      templateId: TypeConId,
  ): Option[InterfaceInstanceDefRef] = {
    def mkRef(parent: TypeConId) =
      InterfaceInstanceDefRef(parent, interfaceId, templateId)

    List(mkRef(templateId), mkRef(interfaceId)) find { ref =>
      machine.compiledPackages.getDefinition(ref).nonEmpty
    }
  }

  private[this] def interfaceInstanceExists(
      machine: Machine[_],
      interfaceId: TypeConId,
      templateId: TypeConId,
  ): Boolean =
    getInterfaceInstance(machine, interfaceId, templateId).nonEmpty

  // Precondition: the package of tplId is loaded in the machine
  private[this] def ensureTemplateImplementsInterface[Q](
      machine: Machine[_],
      ifaceId: TypeConId,
      coid: V.ContractId,
      tplId: TypeConId,
  )(k: => Control[Q]): Control[Q] = {
    if (!interfaceInstanceExists(machine, ifaceId, tplId)) {
      Control.Error(IE.ContractDoesNotImplementInterface(ifaceId, coid, tplId))
    } else {
      k
    }
  }

  final case object SBExtractSAnyValue extends UpdateBuiltin(1) {
    override protected def executeUpdate(
        args: ArraySeq[SValue],
        machine: UpdateMachine,
    ): Control[Question.Update] = {
      val (_, record) = getSAnyContract(args, 0)
      Control.Value(record)
    }
  }

  /** Fetches the requested contract ID, casts its to the requested interface, computes its view and returns it as an
    * SAny. In addition, if [[soft]] is true, then upgrades the contract to the preferred template version for the same
    * package name, and compares its computed view to that of the old contract. If the two views agree then the upgraded
    * contract is cached and returned.
    */
  final case class SBFetchInterface(interfaceId: TypeConId) extends UpdateBuiltin(1) {
    override protected def executeUpdate(
        args: ArraySeq[SValue],
        machine: UpdateMachine,
    ): Control[Question.Update] = {
      val coid = getSContractId(args, 0)
      fetchInterface(machine, coid, interfaceId)(Control.Value)
    }
  }

  /** Fetches the requested contract ID and:
    *  - authenticates the contract against its contract ID if the contract ID uses a legacy hashing method
    *  - ensures that the contract is still active according to the contract state machine
    *  - pulls the preferred template with the same package name and qualified ID as that of the contract
    *  - loads the package of the preferred template
    *  - verifies that the preferred template implements the requested interface
    *  - typechecks and converts to an SValue the argument of the source contract according to the preferred template
    *  - computes the metadata of the contract according to the preferred template (including the ensure clause),
    *    caches the result, and verifies that it matches the metadata of the source contract
    *  - authenticates the contract against its contract ID if the contract ID uses the TypedNormalForm hashing method
    *  - returns the converted argument wrapped in an SAny
    */
  private[this] def fetchInterface(
      machine: UpdateMachine,
      coid: V.ContractId,
      interfaceId: TypeConId,
  )(k: SAny => Control[Question.Update]): Control[Question.Update] = {

    def processSrcContract(
        srcPackageName: Ref.PackageName,
        srcTmplId: TypeConId,
        srcMetadata: ContractMetadata,
        srcArg: V,
        mbTypedNormalFormAuthenticator: Option[Hash => Boolean],
        forbidLocalContractIds: Boolean,
        forbidTrailingNones: Boolean,
    ): Control[Question.Update] = {
      resolvePackageName(machine, srcPackageName) { pkgId =>
        val dstTmplId = srcTmplId.copy(pkg = pkgId)
        machine.ensurePackageIsLoaded(
          NameOf.qualifiedNameOfCurrentFunc,
          dstTmplId.packageId,
          language.Reference.Template(dstTmplId.toRef),
        ) { () =>
          ensureTemplateImplementsInterface(machine, interfaceId, coid, dstTmplId) {
            importCreateArg(
              machine,
              Some(coid),
              srcTmplId,
              dstTmplId,
              srcArg,
              forbidLocalContractIds = forbidLocalContractIds,
              forbidTrailingNones = forbidTrailingNones,
            ) { dstSArg =>
              fetchValidateDstContract(
                machine,
                coid,
                srcTmplId,
                srcPackageName,
                srcMetadata,
                dstTmplId,
                dstSArg,
                mbTypedNormalFormAuthenticator,
              ) { case (dstTmplId, dstArg, _) =>
                k(SAny(Ast.TTyCon(dstTmplId), dstArg))
              }
            }
          }
        }
      }
    }

    machine.getIfLocalContract(coid) match {
      case Some((srcTmplId, srcSArg)) =>
        ensureContractActive(machine, coid, srcTmplId) {
          // We retrieve (or compute for the first time) the contract info of the local contract in order to extract
          // its metadata.
          // We do not need to load the package of srcTmplId because if the contract was created locally, then the
          // package is already loaded.
          getContractInfo(
            machine,
            coid,
            srcTmplId,
            srcSArg,
          ) { srcContractInfo =>
            processSrcContract(
              srcPackageName = srcContractInfo.packageName,
              srcTmplId = srcTmplId,
              srcMetadata = srcContractInfo.metadata,
              srcArg = srcContractInfo.arg,
              mbTypedNormalFormAuthenticator = Some(_ == srcContractInfo.valueHash),
              forbidLocalContractIds = false,
              forbidTrailingNones = true,
            )
          }
        }
      case None =>
        machine.lookupContract(coid)((coinst, hashingMethod, authenticator) =>
          // If hashingMethod is one of the legacy methods, we need to authenticate the contract before normalizing it.
          authenticateIfLegacyContract(coid, coinst, hashingMethod, authenticator) { () =>
            ensureContractActive(machine, coid, coinst.templateId) {
              processSrcContract(
                srcPackageName = coinst.packageName,
                srcTmplId = coinst.templateId,
                srcMetadata = ContractMetadata(
                  coinst.signatories,
                  coinst.nonSignatoryStakeholders,
                  coinst.contractKeyWithMaintainers,
                ),
                srcArg = coinst.createArg,
                mbTypedNormalFormAuthenticator = hashingMethod match {
                  case HashingMethod.TypedNormalForm => Some(authenticator)
                  case HashingMethod.Legacy | HashingMethod.UpgradeFriendly => None
                },
                forbidLocalContractIds = true,
                forbidTrailingNones = hashingMethod match {
                  case HashingMethod.Legacy => false
                  case HashingMethod.UpgradeFriendly | HashingMethod.TypedNormalForm => true
                },
              )
            }
          }
        )
    }
  }

  private[this] def resolvePackageName[Q](machine: UpdateMachine, pkgName: Ref.PackageName)(
      k: PackageId => Control[Q]
  ): Control[Q] = {
    machine.packageResolution.get(pkgName) match {
      case None => Control.Error(IE.UnresolvedPackageName(pkgName))
      case Some(pkgId) => k(pkgId)
    }
  }

  /** $fetchTemplate[T]
    *    :: ContractId a
    *    -> Optional {key: key, maintainers: List Party} (template key, if present)
    *    -> a
    */

  final case class SBFetchTemplate(templateId: TypeConId) extends UpdateBuiltin(1) {
    override protected def executeUpdate(
        args: ArraySeq[SValue],
        machine: UpdateMachine,
    ): Control[Question.Update] = {
      val coid = getSContractId(args, 0)
      fetchTemplate(machine, templateId, coid)(Control.Value)
    }
  }

  final case class SBApplyChoiceGuard(
      choiceName: ChoiceName,
      byInterface: Option[TypeConId],
  ) extends UpdateBuiltin(3) {
    override protected def executeUpdate(
        args: ArraySeq[SValue],
        machine: UpdateMachine,
    ): Control.Expression = {
      val guard = args(0)
      val (templateId, record) = getSAnyContract(args, 1)
      val coid = getSContractId(args, 2)

      val e = SEAppAtomic(SEValue(guard), ArraySeq(SEValue(SAnyContract(templateId, record))))
      machine.pushKont(KCheckChoiceGuard(coid, templateId, choiceName, byInterface))
      Control.Expression(e)
    }
  }

  final case object SBGuardConstTrue extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SBool = {
      discard(getSAnyContract(args, 0))
      SValue.SValue.True
    }
  }

  final case class SBResolveSBUBeginExercise(
      interfaceId: TypeConId,
      choiceName: ChoiceName,
      consuming: Boolean,
      byKey: Boolean,
      explicitChoiceAuthority: Boolean,
  ) extends SBuiltinFun(1) {
    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control.Expression = {
      val e = SEBuiltinFun(
        SBUBeginExercise(
          templateId = getSAnyContract(args, 0)._1,
          interfaceId = Some(interfaceId),
          choiceId = choiceName,
          consuming = consuming,
          byKey = false,
          explicitChoiceAuthority = explicitChoiceAuthority,
        )
      )
      Control.Expression(e)
    }
  }

  final case class SBResolveSBUInsertFetchNode(
      interfaceId: TypeConId
  ) extends SBuiltinFun(1) {
    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control.Expression = {
      val e = SEBuiltinFun(
        SBUInsertFetchNode(
          getSAnyContract(args, 0)._1,
          byKey = false,
          interfaceId = Some(interfaceId),
        )
      )
      Control.Expression(e)
    }
  }

  // Return a definition matching the templateId of a given payload
  sealed class SBResolveVirtual(toDef: Ref.Identifier => SDefinitionRef) extends SBuiltinFun(1) {
    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control.Expression = {
      val (ty, record) = getSAnyContract(args, 0)
      val e = SEApp(SEVal(toDef(ty)), ArraySeq(record))
      Control.Expression(e)
    }
  }

  final case object SBResolveCreate extends SBResolveVirtual(CreateDefRef)

  final case class SBSignatoryInterface(ifaceId: TypeConId)
      extends SBResolveVirtual(SignatoriesDefRef)

  final case class SBObserverInterface(ifaceId: TypeConId) extends SBResolveVirtual(ObserversDefRef)

  // This wraps a contract record into an SAny where the type argument corresponds to
  // the record's templateId.
  final case class SBToAnyContract(tplId: TypeConId) extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SAny = {
      SAnyContract(tplId, getSRecord(args, 0))
    }
  }

  // Convert an interface to a given template type if possible. Since interfaces are represented
  // by an SAny wrapping the underlying template, we need to check that the SAny type constructor
  // matches the template type, and then return the SAny internal value.
  final case class SBFromInterface(
      dstTplId: TypeConId
  ) extends SBuiltinFun(1) {
    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control[Q] = {
      val (srcTplId, srcArg) = getSAnyContract(args, 0)
      fromInterface(machine, srcTplId, srcArg, dstTplId)
    }
  }

  private[this] def fromInterface[Q](
      machine: Machine[Q],
      srcTplId: TypeConId,
      srcArg: SRecord,
      dstTplId: TypeConId,
  ): Control[Q] = {
    if (dstTplId == srcTplId) {
      Control.Value(SOptional(Some(srcArg)))
    } else if (dstTplId.qualifiedName == srcTplId.qualifiedName) {
      val srcPkgName = machine.tmplId2PackageName(dstTplId)
      val dstPkgName = machine.tmplId2PackageName(srcTplId)
      if (srcPkgName == dstPkgName) {
        // This isn't ideal as it's a large uncached computation in a non Update primitive.
        // Ideally this would run in Update, and not iterate the value twice
        // i.e. using an upgrade transformation function directly on SValues
        importCreateArg(
          machine,
          None,
          srcTplId,
          dstTplId,
          srcArg.toNormalizedValue,
          forbidLocalContractIds = false,
          forbidTrailingNones = true,
        ) { templateArg =>
          Control.Value(SOptional(Some(templateArg)))
        }
      } else {
        Control.Value(SValue.SValue.None)
      }
    } else {
      Control.Value(SValue.SValue.None)
    }
  }

  // Convert an interface to a given template type if possible. Since interfaces are represented
  // by an SAny wrapping the underlying template, we need to check that the SAny type constructor
  // matches the template type, and then return the SAny internal value.
  final case class SBUnsafeFromInterface(
      tplId: TypeConId
  ) extends SBuiltinFun(2) {
    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control[Nothing] = {
      val coid = getSContractId(args, 0)
      val (tyCon, record) = getSAnyContract(args, 1)
      val warning = Warning(
        machine.getLastLocation,
        "unsafeFromInterface is deprecated, use fromInterface instead.",
      )
      machine.warningLog.add(warning)(machine.loggingContext)
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
      requiringIfaceId: TypeConId
  ) extends SBuiltinPure(1) {
    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SOptional = {
      val (actualTemplateId, record) = getSAnyContract(args, 0)
      if (interfaceInstanceExists(machine, requiringIfaceId, actualTemplateId))
        SOptional(Some(SAnyContract(actualTemplateId, record)))
      else
        SOptional(None)
    }
  }

  // Convert an interface `requiredIfaceId`  to another interface `requiringIfaceId`, if
  // the underlying template implements `requiringIfaceId`. Else throw a fatal
  // `ContractDoesNotImplementRequiringInterface` exception.
  final case class SBUnsafeFromRequiredInterface(
      requiredIfaceId: TypeConId,
      requiringIfaceId: TypeConId,
  ) extends SBuiltinFun(2) {

    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control[Nothing] = {
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
      ifaceId: TypeConId,
      methodName: MethodName,
  ) extends SBuiltinFun(1) {
    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control[Nothing] = {
      val (templateId, record) = getSAnyContract(args, 0)
      val ref = getInterfaceInstance(machine, ifaceId, templateId).fold(
        crash(
          s"Attempted to call interface ${ifaceId} method ${methodName} on a wrapped " +
            s"template of type ${ifaceId}, but there's no matching interface instance."
        )
      )(iiRef => InterfaceInstanceMethodDefRef(iiRef, methodName))
      val e = SEApp(SEVal(ref), ArraySeq(record))
      Control.Expression(e)
    }
  }

  final case class SBViewInterface(
      ifaceId: TypeConId
  ) extends SBuiltinFun(1) {
    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control[Q] = {
      val (templateId, record) = getSAnyContract(args, 0)

      val ref = getInterfaceInstance(machine, ifaceId, templateId).fold(
        crash(
          s"Attempted to call view for interface ${ifaceId} on a wrapped " +
            s"template of type ${ifaceId}, but there's no matching interface instance."
        )
      )(iiRef => InterfaceInstanceViewDefRef(iiRef))
      executeExpression(machine, SEApp(SEVal(ref), ArraySeq(record))) { view =>
        // Check that the view is serializable
        val _ = view.toNormalizedValue
        Control.Value(view)
      }
    }
  }

  /** $insertFetch[tid]
    *    :: ContractId a
    *    -> Optional {key: key, maintainers: List Party}  (template key, if present)
    *    -> a
    */
  final case class SBUInsertFetchNode(
      templateId: TypeConId,
      byKey: Boolean,
      interfaceId: Option[TypeConId],
  ) extends UpdateBuiltin(1) {

    protected def executeUpdate(
        args: ArraySeq[SValue],
        machine: UpdateMachine,
    ): Control[Question.Update] = {
      val coid = getSContractId(args, 0)
      fetchTemplate(machine, templateId, coid) { templateArg =>
        getContractInfo(
          machine,
          coid,
          templateId,
          templateArg,
        ) { contract =>
          val version = machine.tmplId2TxVersion(templateId)
          machine.ptx.insertFetch(
            coid = coid,
            contract = contract,
            optLocation = machine.getLastLocation,
            byKey = byKey,
            version = version,
            interfaceId = interfaceId,
          ) match {
            case Right(ptx) =>
              machine.ptx = ptx
              machine.metrics.incrCount[TxNodeCount]()
              Control.Value(templateArg)
            case Left(err) =>
              Control.Error(convTxError(err))
          }
        }
      }
    }

  }

  /** $insertLookup[T]
    *    :: { key : key, maintainers: List Party}
    *    -> Maybe (ContractId T)
    *    -> ()
    */
  final case class SBUInsertLookupNode(templateId: TypeConId) extends UpdateBuiltin(2) {
    override protected def executeUpdate(
        args: ArraySeq[SValue],
        machine: UpdateMachine,
    ): Control[Nothing] = {
      val keyVersion = machine.tmplId2TxVersion(templateId)
      val pkgName = machine.tmplId2PackageName(templateId)
      val cachedKey =
        extractKey(NameOf.qualifiedNameOfCurrentFunc, pkgName, templateId, args(0))
      val mbCoid = args(1) match {
        case SOptional(mb) =>
          mb.map {
            case SContractId(coid) => coid
            case _ => crash(s"Non contract id value when inserting lookup node")
          }
        case _ => crash(s"Non option value when inserting lookup node")
      }
      machine.ptx.insertLookup(
        optLocation = machine.getLastLocation,
        key = cachedKey,
        result = mbCoid,
        keyVersion = keyVersion,
      ) match {
        case Right(ptx) =>
          machine.ptx = ptx
          machine.metrics.incrCount[TxNodeCount]()
          Control.Value(SUnit)
        case Left(err) =>
          Control.Error(convTxError(err))
      }
    }
  }

  private[this] abstract class KeyOperation {
    val templateId: TypeConId

    // Callback from the engine returned NotFound
    def handleKeyFound(cid: V.ContractId): Control.Value
    // We already saw this key, but it was undefined or was archived
    def handleKeyNotFound(gkey: GlobalKey): (Control[Nothing], Boolean)

    final def handleKnownInputKey(
        gkey: GlobalKey,
        keyMapping: ContractStateMachine.KeyMapping,
    ): Control[Nothing] =
      keyMapping match {
        case ContractStateMachine.KeyActive(cid) =>
          handleKeyFound(cid)
        case ContractStateMachine.KeyInactive =>
          val (control, _) = handleKeyNotFound(gkey)
          control
      }
  }

  private[this] object KeyOperation {
    final class Fetch(override val templateId: TypeConId) extends KeyOperation {
      override def handleKeyFound(cid: V.ContractId): Control.Value = {
        Control.Value(SContractId(cid))
      }
      override def handleKeyNotFound(gkey: GlobalKey): (Control[Nothing], Boolean) = {
        (Control.Error(IE.ContractKeyNotFound(gkey)), false)
      }
    }

    final class Lookup(override val templateId: TypeConId) extends KeyOperation {
      override def handleKeyFound(cid: V.ContractId): Control.Value = {
        Control.Value(SOptional(Some(SContractId(cid))))
      }
      override def handleKeyNotFound(key: GlobalKey): (Control[Nothing], Boolean) = {
        (Control.Value(SValue.SValue.None), true)
      }
    }
  }

  private[speedy] sealed abstract class SBUKeyBuiltin(
      operation: KeyOperation
  ) extends UpdateBuiltin(1)
      with Product {
    override protected def executeUpdate(
        args: ArraySeq[SValue],
        machine: UpdateMachine,
    ): Control[Question.Update] = {

      val templateId = operation.templateId

      val keyValue = args(0)
      val pkgName = machine.tmplId2PackageName(templateId)
      val cachedKey =
        extractKey(NameOf.qualifiedNameOfCurrentFunc, pkgName, templateId, keyValue)
      if (cachedKey.maintainers.isEmpty) {
        Control.Error(
          IE.FetchEmptyContractKeyMaintainers(
            cachedKey.templateId,
            cachedKey.lfValue,
            cachedKey.packageName,
          )
        )
      } else {
        val gkey = cachedKey.globalKey
        machine.ptx.contractState.resolveKey(gkey) match {
          case Right((keyMapping, next)) =>
            machine.ptx = machine.ptx.copy(contractState = next)
            keyMapping match {
              case ContractStateMachine.KeyActive(coid) =>
                fetchTemplate(machine, templateId, coid) { templateArg =>
                  getContractInfo(
                    machine,
                    coid,
                    templateId,
                    templateArg,
                  )(_ => operation.handleKeyFound(coid))
                }

              case ContractStateMachine.KeyInactive =>
                operation.handleKnownInputKey(gkey, keyMapping)
            }

          case Left(handle) =>
            def continue: Option[V.ContractId] => (Control[Question.Update], Boolean) = { result =>
              val (keyMapping, next) = handle(result)
              machine.ptx = machine.ptx.copy(contractState = next)
              keyMapping match {
                case ContractStateMachine.KeyActive(coid) =>
                  val c =
                    fetchTemplate(machine, templateId, coid) { templateArg =>
                      getContractInfo(
                        machine,
                        coid,
                        templateId,
                        templateArg,
                      )(_ => operation.handleKeyFound(coid))
                    }
                  (c, true)
                case ContractStateMachine.KeyInactive =>
                  operation.handleKeyNotFound(gkey)
              }
            }

            machine.needKey(
              NameOf.qualifiedNameOfCurrentFunc,
              GlobalKeyWithMaintainers(gkey, cachedKey.maintainers),
              continue,
            )
        }
      }
    }
  }

  /** $fetchKey[T]
    *   :: { key: key, maintainers: List Party }
    *   -> ContractId T
    */
  final case class SBUFetchKey(
      templateId: TypeConId
  ) extends SBUKeyBuiltin(new KeyOperation.Fetch(templateId))

  /** $lookupKey[T]
    *   :: { key: key, maintainers: List Party }
    *   -> Maybe (ContractId T)
    */
  final case class SBULookupKey(
      templateId: TypeConId
  ) extends SBUKeyBuiltin(new KeyOperation.Lookup(templateId))

  /** $getTime :: Token -> Timestamp */
  final case object SBUGetTime extends UpdateBuiltin(1) {
    override protected def executeUpdate(
        args: ArraySeq[SValue],
        machine: UpdateMachine,
    ): Control[Question.Update] = {
      checkToken(args, 0)
      machine.needTime(time => {
        machine.setTimeBoundaries(Time.Range(time, time))

        Control.Value(STimestamp(time))
      })
    }
  }

  /** $ledgerTimeLT: Timestamp -> Token -> Bool */
  final case object SBULedgerTimeLT extends UpdateBuiltin(2) {
    override protected def executeUpdate(
        args: ArraySeq[SValue],
        machine: UpdateMachine,
    ): Control[Question.Update] = {
      checkToken(args, 1)

      val time = getSTimestamp(args, 0)

      machine.needTime(now => {
        val Time.Range(lb, ub) = machine.getTimeBoundaries

        if (now < time) {
          machine.setTimeBoundaries(
            Time.Range(lb, ub.min(time.subtract(Duration.of(1, ChronoUnit.MICROS))))
          )

          Control.Value(SBool(true))
        } else {
          machine.setTimeBoundaries(Time.Range(lb.max(time), ub))

          Control.Value(SBool(false))
        }
      })
    }
  }

  /** $pure :: a -> Token -> a */
  final case object SBPure extends SBuiltinFun(2) {
    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control.Value = {
      checkToken(args, 1)
      Control.Value(args(0))
    }
  }

  /** $trace :: Text -> a -> a */
  final case object SBTrace extends SBuiltinFun(2) {
    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control.Value = {
      val message = getSText(args, 0)
      machine.traceLog.add(message, machine.getLastLocation)(machine.loggingContext)
      Control.Value(args(1))
    }
  }

  /** $userError :: Text -> Error */
  final case object SBUserError extends SBuiltinFun(1) {

    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control.Error = {
      Control.Error(IE.UserError(getSText(args, 0)))
    }
  }

  /** $templatePreconditionViolated[T] :: T -> Error */
  final case class SBTemplatePreconditionViolated(templateId: Identifier) extends SBuiltinFun(1) {

    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control.Error = {
      Control.Error(
        IE.TemplatePreconditionViolated(templateId, None, args(0).toUnnormalizedValue)
      )
    }
  }

  /** $throw :: AnyException -> a */
  final case object SBThrow extends SBuiltinFun(1) {
    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control[Nothing] = {
      val excep = getSAny(args, 0)
      machine.handleException(excep)
    }
  }

  /** $try-handler :: Optional (Token -> a) -> AnyException -> Token -> a (or re-throw) */
  final case object SBTryHandler extends SBuiltinFun(3) {
    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control[Q] = {
      val opt = getSOptional(args, 0)
      val excep = getSAny(args, 1)
      checkToken(args, 2)
      opt match {
        case None =>
          machine.handleException(excep) // re-throw
        case Some(handler) =>
          handler match {
            case handler: SPAP => machine.enterApplication(handler, ArraySeq(SEValue(SToken)))
            case _ => crash(s"Expected SPAP, got $handler")
          }
      }
    }
  }

  /** $any-exception-message :: AnyException -> Text */
  final case object SBAnyExceptionMessage extends SBuiltinFun(1) {
    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control[Nothing] = {
      val exception = getSAnyException(args, 0)
      exception.id match {
        case machine.valueArithmeticError.tyCon =>
          Control.Value(exception.values(0))
        case tyCon =>
          val e = SEApp(SEVal(ExceptionMessageDefRef(tyCon)), ArraySeq(exception))
          Control.Expression(e)
      }
    }
  }

  /** $to_any
    *    :: t
    *    -> Any (where t = ty)
    */
  final case class SBToAny(ty: Ast.Type) extends SBuiltinPure(1) {
    override private[speedy] def executePure(args: ArraySeq[SValue], machine: Machine[_]): SAny = {
      SAny(ty, args(0))
    }
  }

  /** $from_any
    *    :: Any
    *    -> Optional t (where t = expectedType)
    */
  final case class SBFromAny(expectedTy: Ast.Type) extends SBuiltinPure(1) {
    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SOptional = {
      val any = getSAny(args, 0)
      if (any.ty == expectedTy) SOptional(Some(any.value)) else SValue.SValue.None
    }
  }

  /** $interface_template_type_rep
    *    :: t
    *    -> TypeRep (where t = TTyCon(_))
    */
  final case class SBInterfaceTemplateTypeRep(tycon: TypeConId) extends SBuiltinPure(1) {
    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): STypeRep = {
      val (tyCon, _) = getSAnyContract(args, 0)
      STypeRep(Ast.TTyCon(tyCon))
    }
  }

  /** $type_rep_ty_con_name
    *    :: TypeRep
    *    -> Optional Text
    */
  final case object SBTypeRepTyConName extends SBuiltinPure(1) {
    override private[speedy] def executePure(
        args: ArraySeq[SValue],
        machine: Machine[_],
    ): SOptional = {
      getSTypeRep(args, 0) match {
        case Ast.TTyCon(name) => SOptional(Some(SText(name.toString)))
        case _ => SOptional(None)
      }
    }
  }

  /** EQUAL_LIST :: (a -> a -> Bool) -> [a] -> [a] -> Bool */
  final case object SBEqualList extends SBuiltinFun(3) {

    private val equalListBody: SExpr =
      SECaseAtomic( // case xs of
        SELocA(1),
        ArraySeq(
          SCaseAlt(
            SCPNil, // nil ->
            SECaseAtomic( // case ys of
              SELocA(2),
              ArraySeq(
                SCaseAlt(SCPNil, SEValue.True), // nil -> True
                SCaseAlt(SCPDefault, SEValue.False),
              ),
            ), // default -> False
          ),
          SCaseAlt( // cons x xss ->
            SCPCons,
            SECaseAtomic( // case ys of
              SELocA(2),
              ArraySeq(
                SCaseAlt(SCPNil, SEValue.False), // nil -> False
                SCaseAlt( // cons y yss ->
                  SCPCons,
                  SELet1( // let sub = (f y x) in
                    SEAppAtomicGeneral(
                      SELocA(0), // f
                      ArraySeq(
                        SELocS(2), // y
                        SELocS(4),
                      ),
                    ), // x
                    SECaseAtomic( // case (f y x) of
                      SELocS(1),
                      ArraySeq(
                        SCaseAlt(
                          SCPBuiltinCon(Ast.BCTrue), // True ->
                          SEAppAtomicGeneral(
                            SEBuiltinFun(SBEqualList), // single recursive occurrence
                            ArraySeq(
                              SELocA(0), // f
                              SELocS(2), // yss
                              SELocS(4),
                            ),
                          ), // xss
                        ),
                        SCaseAlt(SCPBuiltinCon(Ast.BCFalse), SEValue.False), // False -> False
                      ),
                    ),
                  ),
                ),
              ),
            ),
          ),
        ),
      )

    private val closure: SPAP = {
      val frame = ArraySeq.empty[SValue]
      val arity = 3
      SPAP(PClosure(Profile.LabelUnset, equalListBody, frame), ArraySeq.empty, arity)
    }

    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control[Q] = {
      val f = args(0)
      val xs = args(1)
      val ys = args(2)
      machine.enterApplication(
        closure,
        ArraySeq(SEValue(f), SEValue(xs), SEValue(ys)),
      )
    }

  }

  /** $failWithStatus :: Text -> FailureCategory (Int64) -> Text -> TextMap Text -> a */
  final case object SBFailWithStatus extends SBuiltinFun(4) {
    override private[speedy] def execute[Q](
        args: ArraySeq[SValue],
        machine: Machine[Q],
    ): Control[Nothing] = {
      val errorId = getSText(args, 0)
      val categoryId = getSInt64(args, 1)
      val errorMessage = getSText(args, 2)
      val meta = getSMap(args, 3) match {
        case smap @ SMap(true, treeMap) =>
          treeMap.toMap.map {
            case (SText(key), SText(value)) => (key, value)
            case _ => unexpectedType(0, "TextMap Text", smap)
          }
        case otherwise => unexpectedType(0, "TextMap Text", otherwise)
      }

      Control.Error(IE.FailureStatus(errorId, categoryId.toInt, errorMessage, meta))
    }
  }

  object SBExperimental {

    private object SBExperimentalAnswer extends SBuiltinFun(1) {
      override private[speedy] def execute[Q](
          args: ArraySeq[SValue],
          machine: Machine[Q],
      ): Control.Value = {
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
        SBUserError(compileTime.SEValue(SText(s"experimental $name not supported."))),
      )

  }

  final case class SBUSetLastCommand(cmd: Command) extends UpdateBuiltin(1) {
    override protected def executeUpdate(
        args: ArraySeq[SValue],
        machine: UpdateMachine,
    ): Control[Question.Update] = {
      machine.lastCommand = Some(cmd)
      Control.Value(args(0))
    }
  }

  private[speedy] def convTxError(err: TxErr.TransactionError): IE = {
    err match {
      case TxErr.AuthFailureDuringExecutionTxError(AuthFailureDuringExecution(nid, fa)) =>
        IE.FailedAuthorization(nid, fa)
      case TxErr.DuplicateContractIdTxError(DuplicateContractId(contractId)) =>
        crash(s"Unexpected duplicate contract ID ${contractId}")
      case TxErr.DuplicateContractKeyTxError(DuplicateContractKey(key)) =>
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

  private[this] def extractKey(
      location: String,
      pkgName: PackageName,
      templateId: TypeConId,
      v: SValue,
  ) =
    v match {
      case SStruct(_, vals) =>
        val keyValue = vals(keyIdx)
        val gkey = Speedy.Machine.assertGlobalKey(pkgName, templateId, keyValue)
        CachedKey(
          packageName = pkgName,
          globalKeyWithMaintainers = GlobalKeyWithMaintainers(
            gkey,
            extractParties(NameOf.qualifiedNameOfCurrentFunc, vals(maintainerIdx)),
          ),
          key = keyValue,
        )
      case _ => throw SErrorCrash(location, s"Invalid key with maintainers: $v")
    }

  private[this] val contractInfoStructFieldNames =
    List("type", "value", "signatories", "observers", "mbKey").map(
      Ref.Name.assertFromString
    )

  private[this] val contractInfoPositionStruct =
    Struct.assertFromSeq(contractInfoStructFieldNames.zipWithIndex)

  private[this] val List(
    contractInfoStructTypeFieldIdx,
    contractInfoStructArgIdx,
    contractInfoStructSignatoriesIdx,
    contractInfoStructObserversIdx,
    contractInfoStructKeyIdx,
  ) = contractInfoStructFieldNames.map(contractInfoPositionStruct.indexOf): @nowarn(
    "msg=match may not be exhaustive"
  )

  private[speedy] val SBuildContractInfoStruct =
    SBuiltinFun.SBStructCon(contractInfoPositionStruct)

  private def extractContractInfo(
      tmplId2TxVersion: TypeConId => SerializationVersion,
      tmplId2PackageName: TypeConId => PackageName,
      contractInfoStruct: SValue,
  ): ContractInfo = {
    contractInfoStruct match {
      case SStruct(_, vals) if vals.size == contractInfoPositionStruct.size =>
        val templateId = vals(contractInfoStructTypeFieldIdx) match {
          case STypeRep(Ast.TTyCon(tycon)) => tycon
          case v =>
            throw SErrorCrash(
              NameOf.qualifiedNameOfCurrentFunc,
              s"Invalid contract info struct: $v",
            )
        }
        val version = tmplId2TxVersion(templateId)
        val pkgName = tmplId2PackageName(templateId)
        val mbKey = vals(contractInfoStructKeyIdx) match {
          case SOptional(mbKey) =>
            mbKey.map(
              extractKey(NameOf.qualifiedNameOfCurrentFunc, pkgName, templateId, _)
            )
          case v =>
            throw SErrorCrash(
              NameOf.qualifiedNameOfCurrentFunc,
              s"Expected optional key with maintainers, got: $v",
            )
        }
        ContractInfo(
          version = version,
          packageName = pkgName,
          templateId = templateId,
          value = vals(contractInfoStructArgIdx),
          signatories = extractParties(
            NameOf.qualifiedNameOfCurrentFunc,
            vals(contractInfoStructSignatoriesIdx),
          ),
          observers = extractParties(
            NameOf.qualifiedNameOfCurrentFunc,
            vals(contractInfoStructObserversIdx),
          ),
          keyOpt = mbKey,
        )
      case v =>
        throw SErrorCrash(NameOf.qualifiedNameOfCurrentFunc, s"Invalid contract info struct: $v")
    }
  }

  /** Fetches the requested contract ID and:
    *  - authenticates the contract against its contract ID if the contract ID uses a legacy hashing method
    *  - ensures that the contract is still active according to the contract state machine
    *  - verifies that the source template's qualified name matches that of the target template
    *  - loads the package of the target template
    *  - typechecks and converts to an SValue the argument of the source contract according to the target template
    *  - computes the metadata of the contract according to the target template (including the ensure clause),
    *    caches the result, and verifies that it matches the metadata of the source contract
    *  - authenticates the contract against its contract ID if the contract ID uses the TypedNormalForm hashing method
    *  - returns the converted argument
    */
  private def fetchTemplate(
      machine: UpdateMachine,
      dstTmplId: TypeConId,
      coid: V.ContractId,
  )(k: SValue => Control[Question.Update]): Control[Question.Update] = {

    def processSrcContract(
        srcTmplId: TypeConId,
        srcPkgName: Ref.PackageName,
        srcMetadata: ContractMetadata,
        srcArg: V,
        mbTypedNormalFormAuthenticator: Option[Hash => Boolean],
        forbidLocalContractIds: Boolean,
        forbidTrailingNones: Boolean,
    ): Control[Question.Update] = {
      if (srcTmplId.qualifiedName != dstTmplId.qualifiedName)
        Control.Error(
          IE.WronglyTypedContract(coid, dstTmplId, srcTmplId)
        )
      else
        machine.ensurePackageIsLoaded(
          NameOf.qualifiedNameOfCurrentFunc,
          dstTmplId.packageId,
          language.Reference.Template(dstTmplId.toRef),
        )(() => {
          importCreateArg(
            machine,
            Some(coid),
            srcTmplId,
            dstTmplId,
            srcArg,
            forbidLocalContractIds = forbidLocalContractIds,
            forbidTrailingNones = forbidTrailingNones,
          ) { dstSArg =>
            fetchValidateDstContract(
              machine,
              coid,
              srcTmplId,
              srcPkgName,
              srcMetadata,
              dstTmplId,
              dstSArg,
              mbTypedNormalFormAuthenticator,
            )({ case (_, _, dstContract) =>
              k(dstContract.value)
            })
          }
        })
    }

    machine.getIfLocalContract(coid) match {
      case Some((srcTmplId, srcSArg)) =>
        ensureContractActive(machine, coid, srcTmplId) {
          // If the local contract has the same package ID as the target template ID, then we don't need to
          // import its value and validate its contract info again.
          if (srcTmplId == dstTmplId)
            k(srcSArg)
          else {
            // We retrieve (or compute for the first time) the contract info of the local contract in order to extract
            // its metadata.
            // We do not need to load the package of srcTmplId because if the contract was created locally, then the
            // package is already loaded.
            getContractInfo(
              machine,
              coid,
              srcTmplId,
              srcSArg,
            ) { srcContractInfo =>
              processSrcContract(
                srcTmplId = srcTmplId,
                srcPkgName = srcContractInfo.packageName,
                srcMetadata = srcContractInfo.metadata,
                srcArg = srcContractInfo.arg,
                mbTypedNormalFormAuthenticator = Some(_ == srcContractInfo.valueHash),
                forbidLocalContractIds = false,
                forbidTrailingNones = true,
              )
            }
          }
        }
      case None =>
        machine.lookupContract(coid)((coinst, hashingMethod, authenticator) =>
          // If hashingMethod is one of the legacy methods, we need to authenticate the contract before normalizing it.
          authenticateIfLegacyContract(coid, coinst, hashingMethod, authenticator) { () =>
            ensureContractActive(machine, coid, coinst.templateId) {
              processSrcContract(
                srcTmplId = coinst.templateId,
                srcPkgName = coinst.packageName,
                srcMetadata = ContractMetadata(
                  coinst.signatories,
                  coinst.nonSignatoryStakeholders,
                  coinst.contractKeyWithMaintainers,
                ),
                srcArg = coinst.createArg,
                mbTypedNormalFormAuthenticator = hashingMethod match {
                  case HashingMethod.TypedNormalForm => Some(authenticator)
                  case HashingMethod.Legacy | HashingMethod.UpgradeFriendly => None
                },
                forbidLocalContractIds = true,
                forbidTrailingNones = hashingMethod match {
                  case HashingMethod.Legacy => false
                  case HashingMethod.UpgradeFriendly | HashingMethod.TypedNormalForm => true
                },
              )
            }
          }
        )
    }
  }

  /** A method called after fetching and upgrading a contract, which:
    * - computes the metadata of the contract according to the target template (including the ensure clause),
    *   caches the result, and verifies that it matches the metadata of the source contract
    * - enforces limits on input contracts, signatories, and observers
    * - if [mbTypedNormalFormAuthenticator] is defined, authenticates the contract info using SValueHash
    *
    * Assumes that the package of [dstTmplId] is already loaded.
    */
  private def fetchValidateDstContract(
      machine: UpdateMachine,
      coid: V.ContractId,
      srcTmplId: TypeConId,
      srcPkgName: Ref.PackageName,
      srcContractMetadata: ContractMetadata,
      dstTmplId: TypeConId,
      dstTmplArg: SValue,
      mbTypedNormalFormAuthenticator: Option[Hash => Boolean],
  )(k: (TypeConId, SValue, ContractInfo) => Control[Question.Update]): Control[Question.Update] =
    getContractInfo(
      machine,
      coid,
      dstTmplId,
      dstTmplArg,
    ) { dstContract =>
      ensureContractActive(machine, coid, dstContract.templateId) {
        machine.enforceLimitAddInputContract()
        machine.enforceLimitSignatoriesAndObservers(coid, dstContract)
        checkContractUpgradable(
          coid,
          srcTmplId,
          dstTmplId,
          srcPkgName,
          dstContract.packageName,
          srcContractMetadata,
          dstContract.metadata,
        ) { () =>
          mbTypedNormalFormAuthenticator match {
            case Some(authenticator) =>
              authenticateContractInfo(authenticator, coid, srcTmplId, dstContract) { () =>
                k(dstTmplId, dstTmplArg, dstContract)
              }
            case None => k(dstTmplId, dstTmplArg, dstContract)
          }
        }
      }
    }

  /** Authenticates the provided FatContractInstance using [hashingMethod] if [hashingMethod] is
    * one of [[HashingMethod.Legacy]] or [[HashingMethod.UpgradeFriendly]]. Does nothing if the
    * hashing method is [[HashingMethod.TypedNormalForm]].
    */
  private def authenticateIfLegacyContract(
      coid: V.ContractId,
      coinst: FatContractInstance,
      hashingMethod: Hash.HashingMethod,
      authenticator: Hash => Boolean,
  )(k: () => Control[Question.Update]): Control[Question.Update] = {
    val mbValueHash = hashingMethod match {
      case HashingMethod.Legacy =>
        Some(
          hashContractInstance(
            coinst.templateId,
            coinst.createArg,
            coinst.packageName,
            upgradeFriendly = false,
          )
        )
      case HashingMethod.UpgradeFriendly =>
        Some(
          hashContractInstance(
            coinst.templateId,
            coinst.createArg,
            coinst.packageName,
            upgradeFriendly = true,
          )
        )
      case HashingMethod.TypedNormalForm =>
        None
    }
    mbValueHash match {
      case Some(errorOrHash) =>
        errorOrHash match {
          case Right(hash) =>
            if (authenticator(hash)) {
              k()
            } else {
              Control.Error(
                IE.Upgrade(
                  IE.Upgrade
                    .AuthenticationFailed(
                      coid = coid,
                      srcTemplateId = coinst.templateId,
                      dstTemplateId = coinst.templateId,
                      createArg = coinst.createArg,
                      msg = "failed to authenticate contract",
                    )
                )
              )
            }
          case Left(msg) =>
            Control.Error(
              IE.Upgrade(
                IE.Upgrade.AuthenticationFailed(
                  coid = coid,
                  srcTemplateId = coinst.templateId,
                  dstTemplateId = coinst.templateId,
                  createArg = coinst.createArg,
                  msg = msg,
                )
              )
            )
        }
      // This is not a legacy contract, we do nothing. It will be authenticated after translation to SValue.
      case None => k()
    }
  }

  /** Authenticates the provided contractInfo using [authenticator] */
  private def authenticateContractInfo(
      authenticator: Hash => Boolean,
      coid: V.ContractId,
      srcTemplateId: TypeConId,
      contractInfo: ContractInfo,
  )(k: () => Control[Question.Update]) =
    if (
      authenticator(
        SValueHash.assertHashContractInstance(
          contractInfo.packageName,
          contractInfo.templateId.qualifiedName,
          contractInfo.value,
        )
      )
    ) {
      k()
    } else {
      Control.Error(
        IE.Upgrade(
          IE.Upgrade.AuthenticationFailed(
            coid = coid,
            srcTemplateId = srcTemplateId,
            dstTemplateId = contractInfo.templateId,
            createArg = contractInfo.value.toNormalizedValue,
            msg = s"failed to authenticate contract",
          )
        )
      )
    }

  /** Checks that the metadata of [original] and [recomputed] are the same, fails with a [Control.Error] if not. */
  private def checkContractUpgradable(
      coid: V.ContractId,
      srcTemplateId: TypeConId,
      recomputedTemplateId: TypeConId,
      srcPkgName: Ref.PackageName,
      dstPkgName: Ref.PackageName,
      original: ContractMetadata,
      recomputed: ContractMetadata,
  )(
      k: () => Control[Question.Update]
  ): Control[Question.Update] = {

    def check[T](getter: ContractMetadata => T, desc: String): Option[String] =
      Option.when(getter(recomputed) != getter(original))(
        s"$desc mismatch: $original vs $recomputed"
      )

    List(
      check(_.signatories, "signatories"),
      // Comparing stakeholders allows observers to lose parties that are signatories
      check(_.stakeholders, "stakeholders"),
      check(_.keyOpt.map(_.maintainers), "key maintainers"),
      check(_.keyOpt.map(_.globalKey.key), "key value"),
      Option.when(srcPkgName != dstPkgName)(
        s"package name mismatch: $srcPkgName vs $dstPkgName"
      ),
    ).flatten match {
      case Nil => k()
      case errors =>
        Control.Error(
          IE.Upgrade(
            IE.Upgrade.ValidationFailed(
              coid = coid,
              srcTemplateId = srcTemplateId,
              dstTemplateId = recomputedTemplateId,
              srcPackageName = srcPkgName,
              dstPackageName = dstPkgName,
              originalSignatories = original.signatories,
              originalObservers = original.observers,
              originalKeyOpt = original.keyOpt,
              recomputedSignatories = recomputed.signatories,
              recomputedObservers = recomputed.observers,
              recomputedKeyOpt = recomputed.keyOpt,
              msg = errors.mkString("['", "', '", "']"),
            )
          )
        )
    }
  }

  /** Type-checks [createArg] against [dstTmplId] and converts it to an SValue. The [coid] and [srcTmplId] parameters
    *  are used for error reporting only.
    */
  private def importCreateArg[Q](
      machine: Machine[Q],
      coidOpt: Option[V.ContractId],
      srcTmplId: TypeConId,
      dstTmplId: TypeConId,
      createArg: V,
      forbidLocalContractIds: Boolean,
      forbidTrailingNones: Boolean,
  )(
      k: SValue => Control[Q]
  ): Control[Q] = {
    new ValueTranslator(
      machine.compiledPackages.pkgInterface,
      forbidLocalContractIds = forbidLocalContractIds,
      forbidTrailingNones = forbidTrailingNones,
    )
      .translateValue(Ast.TTyCon(dstTmplId), createArg)
      .fold(
        translationError =>
          Control.Error(
            IE.Upgrade(
              IE.Upgrade
                .TranslationFailed(coidOpt, srcTmplId, dstTmplId, createArg, translationError)
            )
          ),
        k,
      )
  }

  // Get the contract info for a contract, computing if not in our cache
  private def getContractInfo(
      machine: UpdateMachine,
      coid: V.ContractId,
      templateId: Identifier,
      templateArg: SValue,
  )(f: ContractInfo => Control[Question.Update]): Control[Question.Update] = {
    machine.contractInfoCache.get((coid, templateId.packageId)) match {
      case Some(contract) =>
        // sanity check
        assert(contract.templateId == templateId)
        f(contract)
      case None =>
        computeContractInfo(
          machine,
          templateId,
          templateArg,
          allowCatchingContractInfoErrors = false,
        ) { contract =>
          machine.insertContractInfoCache(coid, contract)
          f(contract)
        }
    }
  }

  private def computeContractInfo[Q](
      machine: Machine[Q],
      templateId: Identifier,
      templateArg: SValue,
      allowCatchingContractInfoErrors: Boolean,
  )(f: ContractInfo => Control[Q]): Control[Q] = {
    val e: SExpr = SEApp(
      SEVal(ToContractInfoDefRef(templateId)),
      ArraySeq(templateArg),
    )
    executeExpression(machine, if (allowCatchingContractInfoErrors) e else SEPreventCatch(e)) {
      contractInfoStruct =>
        val contract = extractContractInfo(
          machine.tmplId2TxVersion,
          machine.tmplId2PackageName,
          contractInfoStruct,
        )
        f(contract)
    }
  }

  private def ensureContractActive(
      machine: UpdateMachine,
      coid: V.ContractId,
      templateId: Identifier,
  )(body: => Control[Question.Update]): Control[Question.Update] = {
    machine.ptx.consumedByOrInactive(coid) match {
      case Some(Left(nid)) =>
        Control.Error(IE.ContractNotActive(coid, templateId, nid))
      case Some(Right(())) =>
        Control.Error(IE.ContractNotFound(coid))
      case None =>
        body
    }
  }
}
