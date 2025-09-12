// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.value.Value.ContractId
import data.{FrontStack, Ref}

abstract class CostModel {

  import CostModel._

  /* Builtins */
  val AddNumeric: CostFunction2[Numeric, Numeric]
  val SubNumeric: CostFunction2[Numeric, Numeric]
  val MulNumeric: CostFunction3[Numeric, Numeric, Numeric]
  val DivNumeric: CostFunction3[Numeric, Numeric, Numeric]
  val BRoundNumeric: CostFunction2[Int, Numeric]
  val BCastNumeric: CostFunction3[Numeric, Numeric, Numeric]
  val BShiftNumeric: CostFunction2[Int, Numeric]
  val BAddInt64: CostFunction2[Int64, Int64]
  val BSubInt64: CostFunction2[Int64, Int64]
  val BMulInt64: CostFunction2[Int64, Int64]
  val BDivInt64: CostFunction2[Int64, Int64]
  val BModInt64: CostFunction2[Int64, Int64]
  val BExpInt64: CostFunction2[Int64, Int64]
  val BInt64ToNumeric: CostFunction2[Numeric, Int64]
  val BNumericToInt64: CostFunction1[Numeric]
  val BFoldl = NotDefined
  val BFoldr = NotDefined
  lazy val BTextMapEmpty = BGenMapEmpty
  lazy val BTextMapInsert: CostFunction3[Value, Value, TextMap] = BGenMapInsert
  lazy val BTextMapLookup: CostFunction2[Value, TextMap] = BGenMapLookup
  lazy val BTextMapDelete: CostFunction2[Value, TextMap] = BGenMapDelete
  val BTextMapToList: CostFunction1[TextMap] = BGenMapToList
  lazy val BTextMapSize = BGenMapSize
  val BGenMapEmpty = NotDefined
  val BGenMapInsert: CostFunction3[Value, Value, GenMap]
  val BGenMapLookup: CostFunction2[Value, GenMap]
  val BGenMapDelete: CostFunction2[Value, GenMap]
  val BGenMapToList: CostFunction1[GenMap]
  val BGenMapKeys: CostFunction1[GenMap]
  val BGenMapValues: CostFunction1[GenMap]
  val BGenMapSize: CostFunction1[GenMap]
  val BAppendText: CostFunction2[Text, Text]
  val BError = NotDefined
  val BInt64ToText: CostFunction1[Int64]
  val BNumericToText: CostFunction1[Numeric]
  val BTimestampToText: CostFunction1[Timestamp]
  val BPartyToText: CostFunction1[Party]
  val BContractIdToText: CostFunction1[ContractId]
  val BCodePointsToText: CostFunction1[FrontStack[Long]]
  val BTextToParty: CostFunction1[Text]
  val BTextToInt64: CostFunction1[Text]
  val BTextToNumeric: CostFunction2[Int, Text]
  val BTextToCodePoints: CostFunction1[Text]
  val BSHA256Text: CostFunction1[Text]
  val BSHA256Hex: CostFunction1[Text]
  val BKECCAK256Text: CostFunction1[Text]
  val BSECP256K1Bool: CostFunction3[Text, Text, Text]
  val BSECP256K1WithEcdsaBool: CostFunction3[Text, Text, Text]
  val BDecodeHex: CostFunction1[Text]
  val BEncodeHex: CostFunction1[Text]
  val BTextToContractId: CostFunction1[Text]
  val BDateToUnixDays: CostFunction1[Date]
  val BExplodeText: CostFunction1[Text]
  val BImplodeText: CostFunction1[FrontStack[SValue]]
  val BTimestampToUnixMicroseconds: CostFunction1[Timestamp]
  val BDateToText: CostFunction1[Date]
  val BUnixDaysToDate: CostFunction1[Int64]
  val BUnixMicrosecondsToTimestamp: CostFunction1[Int64]
  val BEqual: CostFunction2[Value, Value]
  val BLess: CostFunction2[Value, Value]
  val BLessEq: CostFunction2[Value, Value]
  val BGreater: CostFunction2[Value, Value]
  val BGreaterEq: CostFunction2[Value, Value]
  val BEqualList = NotDefined
  val BTrace = NotDefined
  val BCoerceContractId: CostFunction1[ContractId]
  val BScaleBigNumeric = NotDefined // Available in 2.dev
  val BPrecisionBigNumeric = NotDefined // Available in 2.dev
  val BAddBigNumeric = NotDefined // Available in 2.dev
  val BSubBigNumeric = NotDefined // Available in 2.dev
  val BMulBigNumeric = NotDefined // Available in 2.dev
  val BDivBigNumeric = NotDefined // Available in 2.dev
  val BShiftRightBigNumeric = NotDefined // Available in 2.dev
  val BBigNumericToNumeric = NotDefined // Available in 2.dev
  val BNumericToBigNumeric = NotDefined // Available in 2.dev
  val BBigNumericToText = NotDefined // Available in 2.dev
  val BAnyExceptionMessage = NotDefined
  val BTypeRepTyConName = NotDefined
  val BFailWithStatus = NotDefined

  /* Expr */

  val EVar: CostConstant = CostConstant.Null
  val EVal: CostConstant = CostConstant.Null
  val EBuiltinFun: CostConstant = CostConstant.Null
  val EBuiltinLit: CostConstant = CostConstant.Null
  val ERecCon: CostFunction1[Int] = CostFunction1.Null
  val ERecProj: CostFunction1[Int] = CostFunction1.Null
  val ERecUp: CostFunction1[Int] = CostFunction1.Null
  val EVariantCon: CostConstant = CostConstant.Null
  val EEnumCon: CostConstant = CostConstant.Null
  val EStructCon: CostFunction1[Int] = CostFunction1.Null
  val EStructProj: CostFunction1[Int] = CostFunction1.Null
  val EStructUp: CostFunction1[Int] = CostFunction1.Null
  val EApp: CostConstant = CostConstant.Null
  val EAbs: CostConstant = CostConstant.Null
  val ECase: NotDefined = NotDefined
  val ELet: CostConstant = CostConstant.Null
  val ENil: CostConstant = CostConstant.Null
  val ECons: CostConstant = CostConstant.Null
  val ENone: CostConstant = CostConstant.Null
  val ESome: CostConstant = CostConstant.Null
  val EToAny: CostConstant = CostConstant.Null
  val EFromAny: CostConstant = CostConstant.Null
  val ETypeRep: CostConstant = CostConstant.Null
  val EToInterface: CostConstant = CostConstant.Null
  val EFromInterface: CostConstant = CostConstant.Null
  val EUnsafeFromInterface: CostConstant = CostConstant.Null
  val EToRequiredInterface: CostConstant = CostConstant.Null
  val EFromRequiredInterface: CostConstant = CostConstant.Null
  val EUnsafeFromRequiredInterface: CostConstant = CostConstant.Null
  val ECallInterface: CostConstant = EVal
  val EInterfaceTemplateTypeRep: CostConstant = CostConstant.Null
  val ESignatoryInterface = EVal
  val EObserverInterface = EVal
  val EViewInterface = EVal

  /* Kontinuation */

  val KPure: NotDefined = NotDefined
  val KOverApp: NotDefined = NotDefined
  val KPushTo: NotDefined = NotDefined
  val KFoldl: NotDefined = NotDefined
  val KFoldr: NotDefined = NotDefined
  val KCacheVal: NotDefined = NotDefined
  val KCloseExercise: NotDefined = NotDefined
  val KCheckChoiceGuard: NotDefined = NotDefined // use in 2.dev
  val KLabelClosure: NotDefined = NotDefined // only use when profiling is on
  val KLeaveClosure: NotDefined = NotDefined // only use when profiling is on

  val KTryCatchHandler: NotDefined = NotDefined
  val KPreventException: NotDefined = NotDefined // does nothing when executed
  val KConvertingException: NotDefined = NotDefined // does nothing when executed

  // cost to add n cells to the continuation stack
  val KontStackIncrease: CostFunction1[Int] = CostFunction1.Null
  // cost to add n cells to the env
  val EnvIncrease: CostFunction1[Int] = CostFunction1.Null

}

object CostModel {

  type Cost = Long

  type Int64 = Long
  type Numeric = data.Numeric
  type Text = String
  type Date = data.Time.Date
  type Timestamp = data.Time.Timestamp
  type TextMap = SMap
  type GenMap = SMap
  type List = SList
  type ContractId = value.Value.ContractId
  type Value = SValue
  type Party = Ref.Party

  object Cost {
    val MaxValue: Cost = Long.MaxValue
  }

  val NotDefined = CostConstant.Null
  type NotDefined = NotDefined.type

  val CostAware = CostConstant.Null
  type CostAware = CostAware.type

  final case class CostConstant(cost: Cost)

  object CostConstant {
    val Null = CostConstant(0)
  }

  abstract class CostFunction1[-X] {
    def cost(x: X): Cost
  }

  object CostFunction1 {
    final case class Constant(c: Cost) extends CostFunction1[Any] {
      override def cost(x: Any): Cost = c
    }

    val Null: CostFunction1[Any] = Constant(0)
  }

  abstract class CostFunction2[-X, -Y] {
    def cost(x: X, y: Y): Cost
  }

  object CostFunction2 {
    final case class Constant(c: Cost) extends CostFunction2[Any, Any] {
      override def cost(x: Any, y: Any): Cost = c
    }

    val Null: CostFunction2[Any, Any] = Constant(0)
  }

  abstract class CostFunction3[-X, -Y, -Z] {
    def cost(x: Y, y: Y, z: Z): Cost
  }

  object CostFunction3 {
    final case class Constant(c: Cost) extends CostFunction3[Any, Any, Any] {
      override def cost(x: Any, y: Any, z: Any): Cost = c
    }

    val Null: CostFunction3[Any, Any, Any] = Constant(0)
  }

  val Empty = new EmptyModel

  class EmptyModel extends CostModel {
    override val AddNumeric: CostFunction2[Numeric, Numeric] = CostFunction2.Null
    override val SubNumeric: CostFunction2[Numeric, Numeric] = CostFunction2.Null
    override val MulNumeric: CostFunction3[Numeric, Numeric, Numeric] = CostFunction3.Null
    override val DivNumeric: CostFunction3[Numeric, Numeric, Numeric] = CostFunction3.Null
    override val BRoundNumeric: CostFunction2[Int, Numeric] = CostFunction2.Null
    override val BCastNumeric: CostFunction3[Numeric, Numeric, Numeric] = CostFunction3.Null
    override val BShiftNumeric: CostFunction2[Int, Numeric] = CostFunction2.Null
    override val BAddInt64: CostFunction2[Int64, Int64] = CostFunction2.Null
    override val BSubInt64: CostFunction2[Int64, Int64] = CostFunction2.Null
    override val BMulInt64: CostFunction2[Int64, Int64] = CostFunction2.Null
    override val BDivInt64: CostFunction2[Int64, Int64] = CostFunction2.Null
    override val BModInt64: CostFunction2[Int64, Int64] = CostFunction2.Null
    override val BExpInt64: CostFunction2[Int64, Int64] = CostFunction2.Null
    override val BInt64ToNumeric: CostFunction2[Numeric, Int64] = CostFunction2.Null
    override val BNumericToInt64: CostFunction1[Numeric] = CostFunction1.Null
    override val BTextMapToList: CostFunction1[TextMap] = CostFunction1.Null
    override val BGenMapInsert: CostFunction3[Value, Value, GenMap] = CostFunction3.Null
    override val BGenMapLookup: CostFunction2[Value, GenMap] = CostFunction2.Null
    override val BGenMapDelete: CostFunction2[Value, GenMap] = CostFunction2.Null
    override val BGenMapToList: CostFunction1[GenMap] = CostFunction1.Null
    override val BGenMapKeys: CostFunction1[GenMap] = CostFunction1.Null
    override val BGenMapValues: CostFunction1[GenMap] = CostFunction1.Null
    override val BGenMapSize: CostFunction1[GenMap] = CostFunction1.Null
    override val BAppendText: CostFunction2[Text, Text] = CostFunction2.Null
    override val BInt64ToText: CostFunction1[Int64] = CostFunction1.Null
    override val BNumericToText: CostFunction1[Numeric] = CostFunction1.Null
    override val BTimestampToText: CostFunction1[Timestamp] = CostFunction1.Null
    override val BPartyToText: CostFunction1[Party] = CostFunction1.Null
    override val BContractIdToText: CostFunction1[ContractId] = CostFunction1.Null
    override val BCodePointsToText: CostFunction1[FrontStack[Long]] = CostFunction1.Null
    override val BTextToParty: CostFunction1[Text] = CostFunction1.Null
    override val BTextToInt64: CostFunction1[Text] = CostFunction1.Null
    override val BTextToNumeric: CostFunction2[Int, Text] = CostFunction2.Null
    override val BTextToCodePoints: CostFunction1[Text] = CostFunction1.Null
    override val BSHA256Text: CostFunction1[Text] = CostFunction1.Null
    override val BSHA256Hex: CostFunction1[Text] = CostFunction1.Null
    override val BKECCAK256Text: CostFunction1[Text] = CostFunction1.Null
    override val BSECP256K1Bool: CostFunction3[Text, Text, Text] = CostFunction3.Null
    override val BSECP256K1WithEcdsaBool: CostFunction3[Text, Text, Text] = CostFunction3.Null
    override val BDecodeHex: CostFunction1[Text] = CostFunction1.Null
    override val BEncodeHex: CostFunction1[Text] = CostFunction1.Null
    override val BTextToContractId: CostFunction1[Text] = CostFunction1.Null
    override val BDateToUnixDays: CostFunction1[Date] = CostFunction1.Null
    override val BExplodeText: CostFunction1[Text] = CostFunction1.Null
    override val BImplodeText: CostFunction1[FrontStack[SValue]] = CostFunction1.Null
    override val BTimestampToUnixMicroseconds: CostFunction1[Timestamp] = CostFunction1.Null
    override val BDateToText: CostFunction1[Date] = CostFunction1.Null
    override val BUnixDaysToDate: CostFunction1[Int64] = CostFunction1.Null
    override val BUnixMicrosecondsToTimestamp: CostFunction1[Int64] = CostFunction1.Null
    override val BEqual: CostFunction2[Value, Value] = CostFunction2.Null
    override val BLess: CostFunction2[Value, Value] = CostFunction2.Null
    override val BLessEq: CostFunction2[Value, Value] = CostFunction2.Null
    override val BGreater: CostFunction2[Value, Value] = CostFunction2.Null
    override val BGreaterEq: CostFunction2[Value, Value] = CostFunction2.Null
    override val BCoerceContractId: CostFunction1[ContractId] = CostFunction1.Null

    override val KontStackIncrease: CostFunction1[Int] = CostFunction1.Null
    override val EnvIncrease: CostFunction1[Int] = CostFunction1.Null
  }

  /** Represents a first-degree polynomial (linear): `f(n) = A + B*n`.
    *
    * @param a The fixed, base memory overhead (the y-intercept).
    * @param b The per-element memory overhead (the slope).
    */
  case class LinearPolynomial(a: Long, b: Long) {
    def calculate(n: Int) = a + b * n.toLong
  }

  def roundTo8(l: Long) = (l + 7) & ~7L

  /** A conservative cost model based on a 64-bit JVM with memory optimizations
    * like CompressedOops disabled (`-XX:-UseCompressedOops`).
    */
  object CostModelV0 extends CostModel {

    // Base memory constants for the conservative model
    val OBJECT_HEADER_BYTES = 16L
    val BOOL_BYTES = 1L
    val BYTE_BYTES = 1L
    val CHAR_BYTES = 2L
    val INT_BYTES = 4L
    val LONG_BYTES = 8L
    val REFERENCE_BYTES = 8L

    /** The memory overhead of an empty array shell. */
    private val ARRAY_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + //  Header
        LONG_BYTES // length
    )

    /** The memory model for a `Array[AnyRef]` of size `n`. */
    val AnyRefArraySize = LinearPolynomial(
      ARRAY_SHELL_BYTES +
        (8 - REFERENCE_BYTES),
      REFERENCE_BYTES,
    )

    /** The memory model for a `Array[Byte]` of size `n`. */
    val ByteArraySize = LinearPolynomial(
      ARRAY_SHELL_BYTES +
        (8 - BYTE_BYTES), // padding overapproximation
      BYTE_BYTES,
    )

    /** The memory model for a `Array[Int]` of size `n`. */
    val IntArraySize = LinearPolynomial(
      ARRAY_SHELL_BYTES +
        (8 - INT_BYTES), // padding overapproximation
      INT_BYTES,
    )

    /** The memory overhead of ArraySeq wrapper. */
    private val ARRAYSEQ_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES +
        REFERENCE_BYTES // .unsafeArray
    )

    /** Memory model for an `Array[AnyRef]` of size `n`. */
    val AnyRefArraySeqSize = LinearPolynomial(
      ARRAYSEQ_SHELL_BYTES + AnyRefArraySize.a,
      AnyRefArraySize.b,
    )

    /** The memory overhead of the `java.lang.String` object shell itself. */
    private val STRING_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES + // .value
        INT_BYTES + // .hash
        BYTE_BYTES + // .coder
        BOOL_BYTES // .hashIsZero
    )

    /** Memory model for a `java.lang.String` of length `n`.
      * We conservatively assume 2 bytes per character (UTF-16), which is the
      * worst case for JVM strings since Java 9.
      */
    val StringSize = LinearPolynomial(
      STRING_SHELL_BYTES +
        ByteArraySize.a,
      ByteArraySize.b * 2, // Each char takes 2 bytes if string is not LATIN1.
    )

    /** Memory model for an Ascii `java.lang.String` of length `n`.
      * We know that each char takes 1 byte.
      */
    val AsciiStringSize =
      LinearPolynomial(STRING_SHELL_BYTES + ByteArraySize.a, ByteArraySize.b)

    /** The memory overhead of a protobuf `ByteString` object shell. */
    private val BYTESTRING_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        INT_BYTES + // .hash
        REFERENCE_BYTES // .bytes
    )

    /** Memory model for a protobuf `ByteString` of length `n`. */
    val ByteStringSize = LinearPolynomial(
      BYTESTRING_SHELL_BYTES +
        ByteArraySize.a +
        (8 - INT_BYTES), // padding overapproximation
      ByteArraySize.b,
    )

    /* Memory model for the None object */
    val NONE_BYTES = 0L // we do not charge constant

    /** Memory overhead for a `Some` object. */
    private val SOME_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES // .value
    )

    /** Memory model of an `Option` object. */
    val OptionSize = SOME_BYTES max NONE_BYTES

    /** Memory model for the Nil object */
    private val NIL_BYTES = 0L // we do not charge constant

    /** Memory overhead for a `::` object. */
    private val CONS_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES + // .head
        REFERENCE_BYTES // .tail
    )

    /** Memory model of a `List` object of lenght `n`. */
    val ListSize = LinearPolynomial(NIL_BYTES, CONS_BYTES)

    /** Memory model of Numeric object */
    // overapproximation got empirically
    val NumericSize = 232L

    /** Memory model of Data object */
    val DateSize = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        INT_BYTES // value
    )

    /** Memory model of Timestemap object */
    val TimestampSize = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        LONG_BYTES // value
    )

    /** Memory model of the Byte wrapper */
    val BytesWrapperSize =
      OBJECT_HEADER_BYTES +
        REFERENCE_BYTES

    /** Memoery model of a Bytes of length `n` */
    val BytesSize = LinearPolynomial(
      BytesWrapperSize + ByteStringSize.a,
      ByteStringSize.b,
    )

    // wrappers around Bytes
    val HashWrapperSize =
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES // .value

    val HashSize =
      HashWrapperSize +
        BytesSize.calculate(crypto.Hash.underlyingHashLength)

    val CONTRACTID_V1_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES + // discriminator
        REFERENCE_BYTES // .suffix
    )

    val ContractIdSize =
      CONTRACTID_V1_SHELL_BYTES +
        HashSize +
        BytesSize.calculate(ContractId.V1.MaxSuffixLength)

    val IMMARRAY_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        INT_BYTES + // .start
        INT_BYTES + // .length
        REFERENCE_BYTES // .array
    )

    /** Memory model of an `ImmArray` of length `n` */
    // This is not a proper overapproximation because ImmArray can be slice
    val ImmArraySize = LinearPolynomial(
      IMMARRAY_SHELL_BYTES + AnyRefArraySeqSize.a,
      AnyRefArraySeqSize.b,
    )

    /** Memory model of an `Frontstack` of length `n` */
    // Get empirically, not a proper overapproximation because can contain ImmArray
    val FrontStackSize = LinearPolynomial(48, 32)

    /** Memory model of a SUnit object */
    val SUnitSize = 0L // we do not charge constant

    /** Memory model of a SBool object */
    val SBoolSize = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        BOOL_BYTES // value
    )

    /** Memory model of a SInt object */
    val SInt64Size = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        LONG_BYTES // value
    )

    /** Memory model of a SDate wrapper */
    val SDateWrapperSize = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES // value
    )

    /** Memory model of a STimestamp wrapper */
    val STimestampWrapperSize = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES // value
    )

    /** Memory model of a SNumeric wrapper */
    val SNumericWrapperSize = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES // .value
    )

    /** Memory model of a SText wrapper */
    val STextWrapperSize = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES // .value
    )

    /** Memory model of a SParty wrapper */
    val SPartyWrapperSize = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES // .value
    )

    val SContractIdWrapperSize = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES // .value
    )

    val SOptionalWrapperSize = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES // .value
    )

    val SListWrapperSize = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES // .value
    )

    override val AddNumeric: CostFunction2[Numeric, Numeric] =
      CostFunction2.Constant(NumericSize + SNumericWrapperSize)
    override val SubNumeric: CostFunction2[Numeric, Numeric] =
      CostFunction2.Constant(NumericSize + SNumericWrapperSize)
    override val MulNumeric: CostFunction3[Numeric, Numeric, Numeric] =
      CostFunction3.Constant(NumericSize + SNumericWrapperSize)
    override val DivNumeric: CostFunction3[Numeric, Numeric, Numeric] =
      CostFunction3.Constant(NumericSize + SNumericWrapperSize)
    override val BRoundNumeric: CostFunction2[Int, Numeric] =
      CostFunction2.Constant(NumericSize + SNumericWrapperSize)
    override val BCastNumeric: CostFunction3[Numeric, Numeric, Numeric] =
      CostFunction3.Constant(NumericSize + SNumericWrapperSize)
    override val BShiftNumeric: CostFunction2[Int, Numeric] =
      CostFunction2.Constant(NumericSize + SNumericWrapperSize)
    override val BAddInt64: CostFunction2[Int64, Int64] =
      CostFunction2.Constant(SInt64Size)
    override val BSubInt64: CostFunction2[Int64, Int64] =
      CostFunction2.Constant(SInt64Size)
    override val BMulInt64: CostFunction2[Int64, Int64] =
      CostFunction2.Constant(SInt64Size)
    override val BDivInt64: CostFunction2[Int64, Int64] =
      CostFunction2.Constant(SInt64Size)
    override val BModInt64: CostFunction2[Int64, Int64] =
      CostFunction2.Constant(SInt64Size)
    override val BExpInt64: CostFunction2[Int64, Int64] =
      CostFunction2.Constant(NumericSize + SNumericWrapperSize)
    override val BInt64ToNumeric: CostFunction2[Numeric, Int64] =
      CostFunction2.Constant(NumericSize + SNumericWrapperSize)
    override val BNumericToInt64: CostFunction1[Numeric] =
      CostFunction1.Constant(NumericSize + SNumericWrapperSize)
    override val BGenMapInsert: CostFunction3[Value, Value, GenMap] = CostFunction3.Null
    override val BGenMapLookup: CostFunction2[Value, GenMap] = CostFunction2.Null
    override val BGenMapDelete: CostFunction2[Value, GenMap] = CostFunction2.Null
    override val BGenMapToList: CostFunction1[GenMap] = CostFunction1.Null
    override val BGenMapKeys: CostFunction1[GenMap] = CostFunction1.Null
    override val BGenMapValues: CostFunction1[GenMap] = CostFunction1.Null
    override val BGenMapSize: CostFunction1[GenMap] = CostFunction1.Null
    override val BAppendText: CostFunction2[Text, Text] = {
      val poly = LinearPolynomial(STextWrapperSize, StringSize.b)
      (x: String, y: String) => poly.calculate(x.length + y.length)
    }
    override val BInt64ToText: CostFunction1[Int64] =
      // 20 = "-9223372036854775808".length
      CostFunction1.Constant(STextWrapperSize + AsciiStringSize.calculate(20))
    override val BNumericToText: CostFunction1[Numeric] = {
      // 40 = "-99999999999999999999999999999999999999.".length
      CostFunction1.Constant(STextWrapperSize + AsciiStringSize.calculate(40))
    }
    override val BTimestampToText: CostFunction1[Timestamp] =
      // 27 = "9999-12-31T23:59:59.999999Z".length
      CostFunction1.Constant(STextWrapperSize + AsciiStringSize.calculate(27))
    override val BPartyToText: CostFunction1[Party] =
      CostFunction1.Constant(STextWrapperSize) // We reuse the underlying string
    override val BContractIdToText: CostFunction1[ContractId] =
      // Only for V1 ContractId
      // Remy: We may want to shorted for canton
      CostFunction1.Constant(
        SOptionalWrapperSize + OptionSize + STextWrapperSize + AsciiStringSize.calculate(254)
      )
    override val BCodePointsToText: CostFunction1[FrontStack[Long]] = {
      // a code point is at most 2 chars in UTF-16
      val poly = LinearPolynomial(STextWrapperSize, StringSize.b * 2)
      (codePoints: FrontStack[Long]) => poly.calculate(codePoints.length)
    }
    override val BTextToParty: CostFunction1[Text] =
      CostFunction1.Constant(
        SOptionalWrapperSize + OptionSize + SPartyWrapperSize
      ) // We reuse the underlying string
    override val BTextToInt64: CostFunction1[Text] =
      CostFunction1.Constant(SOptionalWrapperSize + OptionSize + SInt64Size)
    override val BTextToNumeric: CostFunction2[Int, Text] =
      CostFunction2.Constant(SOptionalWrapperSize + OptionSize + SNumericWrapperSize + NumericSize)
    override val BTextToCodePoints: CostFunction1[Text] = {
      val poly = LinearPolynomial(SListWrapperSize + FrontStackSize.a, FrontStackSize.b)
      (text: String) => poly.calculate(text.length)
    }
    override val BSHA256Text: CostFunction1[Text] =
      CostFunction1.Constant(STextWrapperSize + AsciiStringSize.calculate(64))
    override val BSHA256Hex: CostFunction1[Text] =
      CostFunction1.Constant(STextWrapperSize + AsciiStringSize.calculate(64))
    override val BKECCAK256Text: CostFunction1[Text] =
      CostFunction1.Constant(STextWrapperSize + AsciiStringSize.calculate(64))
    override val BSECP256K1Bool: CostFunction3[Text, Text, Text] =
      CostFunction3.Constant(SBoolSize)
    override val BSECP256K1WithEcdsaBool: CostFunction3[Text, Text, Text] =
      CostFunction3.Constant(SBoolSize)
    override val BDecodeHex: CostFunction1[Text] = {
      val poly = LinearPolynomial(STextWrapperSize + StringSize.a, StringSize.b / 2)
      (t: String) => poly.calculate(t.length)
    }
    override val BEncodeHex: CostFunction1[Text] = { (t: String) =>
      STextWrapperSize + StringSize.a + StringSize.b * t.length / 2
    }
    override val BTextToContractId: CostFunction1[Text] =
      CostFunction1.Constant(SContractIdWrapperSize + ContractIdSize)
    override val BDateToUnixDays: CostFunction1[Date] =
      CostFunction1.Constant(SInt64Size)
    override val BExplodeText: CostFunction1[Text] = {
      val poly =
        LinearPolynomial(SListWrapperSize + FrontStackSize.a, FrontStackSize.b * SInt64Size)
      (text: String) => poly.calculate(text.length)
    }
    override val BImplodeText: CostFunction1[FrontStack[SValue]] = {
      val poly = LinearPolynomial(STextWrapperSize + StringSize.a, StringSize.b)
      (list: FrontStack[SValue]) =>
        // be carefull computation of n should use bounded memory
        val n = list.iterator.map {
          case SText(s) => s.length
          case _ => 0 // should not happen
        }.sum
        poly.calculate(n)
    }
    override val BTimestampToUnixMicroseconds: CostFunction1[Timestamp] =
      CostFunction1.Constant(SInt64Size)
    override val BDateToText: CostFunction1[Date] =
      //  = "9999-12-31".length
      CostFunction1.Constant(STextWrapperSize + StringSize.calculate(10))
    override val BUnixDaysToDate: CostFunction1[Int64] =
      CostFunction1.Constant(SDateWrapperSize + DateSize)
    override val BUnixMicrosecondsToTimestamp: CostFunction1[Int64] =
      CostFunction1.Constant(STimestampWrapperSize + TimestampSize)
    override val BEqual: CostFunction2[Value, Value] = CostFunction2.Constant(SBoolSize)
    override val BLess: CostFunction2[Value, Value] = CostFunction2.Constant(SBoolSize)
    override val BLessEq: CostFunction2[Value, Value] = CostFunction2.Constant(SBoolSize)
    override val BGreater: CostFunction2[Value, Value] = CostFunction2.Constant(SBoolSize)
    override val BGreaterEq: CostFunction2[Value, Value] = CostFunction2.Constant(SBoolSize)
    override val BCoerceContractId: CostFunction1[ContractId] = CostFunction1.Constant(0L)

    override val KontStackIncrease: CostFunction1[Int] = CostFunction1.Null
    override val EnvIncrease: CostFunction1[Int] = CostFunction1.Null
  }

}
