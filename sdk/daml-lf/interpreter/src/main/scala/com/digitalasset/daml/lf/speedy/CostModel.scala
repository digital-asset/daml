// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.speedy.Speedy.Env
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
  val BFoldl: CostConstant
  val BFoldr: CostConstant
  lazy val BTextMapEmpty = BGenMapEmpty
  lazy val BTextMapInsert: CostFunction3[SValue, SValue, STextMap] = BGenMapInsert
  lazy val BTextMapLookup: CostFunction2[SValue, STextMap] = BGenMapLookup
  lazy val BTextMapDelete: CostFunction2[SValue, STextMap] = BGenMapDelete
  val BTextMapToList: CostFunction1[STextMap] = BGenMapToList
  lazy val BTextMapSize = BGenMapSize
  val BGenMapEmpty: CostConstant = CostConstant.Null
  val BGenMapInsert: CostFunction3[SValue, SValue, SGenMap]
  val BGenMapLookup: CostFunction2[SValue, SGenMap]
  val BGenMapDelete: CostFunction2[SValue, SGenMap]
  val BGenMapToList: CostFunction1[SGenMap]
  val BGenMapKeys: CostFunction1[SGenMap]
  val BGenMapValues: CostFunction1[SGenMap]
  val BGenMapSize: CostFunction1[SGenMap]
  val BAppendText: CostFunction2[Text, Text]
  val BError: CostConstant
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
  val BSECP256K1ValidateKey: CostFunction1[Text]
  val BDecodeHex: CostFunction1[Text]
  val BEncodeHex: CostFunction1[Text]
  val BDateToUnixDays: CostFunction1[Date]
  val BExplodeText: CostFunction1[Text]
  val BImplodeText: CostFunction1[FrontStack[SValue]]
  val BTimestampToUnixMicroseconds: CostFunction1[Timestamp]
  val BDateToText: CostFunction1[Date]
  val BUnixDaysToDate: CostFunction1[Int64]
  val BUnixMicrosecondsToTimestamp: CostFunction1[Int64]
  val BEqual: CostFunction2[SValue, SValue]
  val BLess: CostFunction2[SValue, SValue]
  val BLessEq: CostFunction2[SValue, SValue]
  val BGreater: CostFunction2[SValue, SValue]
  val BGreaterEq: CostFunction2[SValue, SValue]
  val BEqualList: CostConstant
  val BTrace: CostConstant
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

  val EVar: CostConstant
  val EVal: CostConstant
  // no EBuiltinFun, we model model only the cost of the wrapped builtin
  // no EBuiltinLit, as we do not charge for literals
  val ERecCon: CostFunction1[Int]
  val ERecProj: CostFunction1[Int]
  val ERecUp: CostFunction1[Int]
  val EVariantCon: CostConstant
  val EEnumCon: CostConstant
  val EStructCon: CostFunction1[Int]
  val EStructProj: CostFunction1[Int]
  val EStructUp: CostFunction1[Int]
  val EApp: CostFunction2[Int, Int]
  val EAbs: CostConstant
  val ECase: CostConstant
  val ELet: CostConstant
  val ENil: CostConstant
  val ECons: CostFunction2[SValue, SValue]
  val ENone: CostConstant
  val ESome: CostConstant
  val EToAny: CostConstant
  val EFromAny: CostConstant
  val ETypeRep: CostConstant
  val EToInterface: CostConstant
  val EFromInterface: CostFunction1[SAnyContract]
  val EUnsafeFromInterface: NotDefined = NotDefined
  val EToRequiredInterface: CostConstant
  val EFromRequiredInterface: CostConstant
  val EUnsafeFromRequiredInterface: NotDefined = NotDefined
  val ECallInterface: CostConstant = EVal
  val EInterfaceTemplateTypeRep: CostConstant
  val ESignatoryInterface = EVal
  val EObserverInterface = EVal
  val EViewInterface = EVal

  val EMakeClosure: CostFunction1[Int]

  /* Kontinuation */

  val KPure: NotDefined = NotDefined
  val KOverApp: CostFunction2[Int, Int]
  val KPushTo: CostFunction2[Int, Speedy.Env]
  val KFoldl: CostFunction1[Int]
  val KFoldr: CostFunction1[Int]
  val KCacheVal: CostConstant
  val KCloseExercise: CostFunction1[SValue]
  val KCheckChoiceGuard: NotDefined = NotDefined // use in 2.dev
  val KLabelClosure: CostConstant = NotDefined // only use when profiling is on
  val KLeaveClosure: CostConstant = NotDefined // only use when profiling is on

  val KTryCatchHandler: CostConstant
  val KPreventException: CostConstant
  val KConvertingException: CostConstant

  // cost to add n cells to the continuation stack
  val KontStackIncrease: CostFunction1[Int]
  // cost to add n cells to the env
  val EnvIncrease: CostFunction1[Int]

}

object CostModel {

  type Cost = Long

  type Int64 = Long
  type Numeric = data.Numeric
  type Text = String
  type Date = data.Time.Date
  type Timestamp = data.Time.Timestamp
  type STextMap = SValue.SMap
  type SGenMap = SValue.SMap
  type SList = SValue.SList
  type SRecord = SValue.SRecord
  type SAny = SValue.SAny
  type SAnyContract = SValue.SAny
  type Contract = SValue.SRecord
  type ContractId = value.Value.ContractId
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

    def CostAware = Null
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
    override val BTextMapToList: CostFunction1[STextMap] = CostFunction1.Null
    override val BGenMapInsert: CostFunction3[SValue, SValue, SGenMap] = CostFunction3.Null
    override val BGenMapLookup: CostFunction2[SValue, SGenMap] = CostFunction2.Null
    override val BGenMapDelete: CostFunction2[SValue, SGenMap] = CostFunction2.Null
    override val BGenMapToList: CostFunction1[SGenMap] = CostFunction1.Null
    override val BGenMapKeys: CostFunction1[SGenMap] = CostFunction1.Null
    override val BGenMapValues: CostFunction1[SGenMap] = CostFunction1.Null
    override val BGenMapSize: CostFunction1[SGenMap] = CostFunction1.Null
    override val BAppendText: CostFunction2[Text, Text] = CostFunction2.Null
    override val BError: CostConstant = CostConstant.Null
    override val BInt64ToText: CostFunction1[Int64] = CostFunction1.Null
    override val BNumericToText: CostFunction1[Numeric] = CostFunction1.Null
    override val BFoldl: CostConstant = CostConstant.Null
    override val BFoldr: CostConstant = CostConstant.Null
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
    override val BSECP256K1ValidateKey: CostFunction1[Text] = CostFunction1.Null
    override val BDecodeHex: CostFunction1[Text] = CostFunction1.Null
    override val BEncodeHex: CostFunction1[Text] = CostFunction1.Null
    override val BDateToUnixDays: CostFunction1[Date] = CostFunction1.Null
    override val BExplodeText: CostFunction1[Text] = CostFunction1.Null
    override val BImplodeText: CostFunction1[FrontStack[SValue]] = CostFunction1.Null
    override val BTimestampToUnixMicroseconds: CostFunction1[Timestamp] = CostFunction1.Null
    override val BDateToText: CostFunction1[Date] = CostFunction1.Null
    override val BUnixDaysToDate: CostFunction1[Int64] = CostFunction1.Null
    override val BUnixMicrosecondsToTimestamp: CostFunction1[Int64] = CostFunction1.Null
    override val BEqual: CostFunction2[SValue, SValue] = CostFunction2.Null
    override val BLess: CostFunction2[SValue, SValue] = CostFunction2.Null
    override val BLessEq: CostFunction2[SValue, SValue] = CostFunction2.Null
    override val BGreater: CostFunction2[SValue, SValue] = CostFunction2.Null
    override val BGreaterEq: CostFunction2[SValue, SValue] = CostFunction2.Null
    override val BEqualList: CostConstant = CostConstant.Null
    override val BTrace: CostConstant = CostConstant.Null
    override val BCoerceContractId: CostFunction1[ContractId] = CostFunction1.Null

    override val EMakeClosure: CostFunction1[Int] = CostFunction1.Null

    override val KontStackIncrease: CostFunction1[Int] = CostFunction1.Null
    override val EnvIncrease: CostFunction1[Int] = CostFunction1.Null
    override val EVal: CostConstant = CostConstant.Null
    override val ERecCon: CostFunction1[Int] = CostFunction1.Null
    override val ERecProj: CostFunction1[Int] = CostFunction1.Null
    override val ERecUp: CostFunction1[Int] = CostFunction1.Null
    override val EVariantCon: CostConstant = CostConstant.Null
    override val EEnumCon: CostConstant = CostConstant.Null
    override val EStructCon: CostFunction1[Int] = CostFunction1.Null
    override val EStructProj: CostFunction1[Int] = CostFunction1.Null
    override val EStructUp: CostFunction1[Int] = CostFunction1.Null
    override val EApp: CostFunction2[Int, Int] = CostFunction2.Null
    override val EAbs: CostConstant = CostConstant.Null
    override val ECase: CostConstant = CostConstant.Null
    override val ELet: CostConstant = CostConstant.Null
    override val ENil: CostConstant = CostConstant.Null
    override val ECons: CostFunction2[SValue, SValue] = CostFunction2.Null
    override val ENone: CostConstant = CostConstant.Null
    override val ESome: CostConstant = CostConstant.Null
    override val EToAny: CostConstant = CostConstant.Null
    override val EFromAny: CostConstant = CostConstant.Null
    override val ETypeRep: CostConstant = CostConstant.Null
    override val EToInterface: CostConstant = CostConstant.Null
    override val EFromInterface: CostFunction1[SAny] = CostFunction1.Null
    override val EToRequiredInterface: CostConstant = CostConstant.Null
    override val EFromRequiredInterface: CostConstant = CostConstant.Null
    override val EInterfaceTemplateTypeRep: CostConstant = CostConstant.Null
    override val EVar: CostConstant = CostConstant.Null
    override val KOverApp: CostFunction2[Int, Int] = CostFunction2.Null
    override val KPushTo: CostFunction2[Int, Env] = CostFunction2.Null
    override val KFoldl: CostFunction1[Int] = CostFunction1.Null
    override val KFoldr: CostFunction1[Int] = CostFunction1.Null
    override val KCacheVal: CostConstant = CostConstant.Null
    override val KCloseExercise: CostFunction1[SValue] = CostFunction1.Null
    override val KTryCatchHandler: CostConstant = CostConstant.Null
    override val KPreventException: CostConstant = CostConstant.Null
    override val KConvertingException: CostConstant = CostConstant.Null
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
        BytesSize.calculate(value.Value.ContractId.V1.MaxSuffixLength)

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
    // Obtained empirically, so not a proper over approximation as it can contain ImmArray
    val FrontStackSize = LinearPolynomial(48, 120)

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

    val SOPTIONAL_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES // .value
    )

    val SOptionalSize = SOPTIONAL_SHELL_BYTES + OptionSize

    val SLIST_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES // .list
    )

    val SListSize = LinearPolynomial(
      SLIST_SHELL_BYTES + FrontStackSize.a,
      FrontStackSize.b,
    )

    val SCONS_BYTES = FrontStackSize.a

    val SRECORD_SHELL_SIZE = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES + // .id
        REFERENCE_BYTES + // .fields
        REFERENCE_BYTES // .value
    )
    val SRecordWrapperSize =
      // We do not charge for the id and the filed name themself
      LinearPolynomial(
        SRECORD_SHELL_SIZE + AnyRefArraySeqSize.a,
        AnyRefArraySeqSize.b,
      )
    val SVARIANT_SHELL_SIZE = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES + // .id
        REFERENCE_BYTES + // .constructor
        INT_BYTES + // .co

        REFERENCE_BYTES // .value
    )
    val SVariantWrapperSize =
      // We do not charge for the id and the constructor name themself
      SVARIANT_SHELL_SIZE
    val SENUM_SHELL_SIZE = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES + // .id
        REFERENCE_BYTES + // .constructor
        INT_BYTES // .constructorRank
    )
    val SEnumSize = SENUM_SHELL_SIZE

    val SSTRUCT_SHELL_SIZE = roundTo8(
      OBJECT_HEADER_BYTES +
        REFERENCE_BYTES + // .fieldName
        REFERENCE_BYTES // .values
    )

    val SStructWrapperSize =
      LinearPolynomial(
        SSTRUCT_SHELL_SIZE + AnyRefArraySeqSize.a,
        AnyRefArraySeqSize.b,
      )

    val SPAPWrapperSize = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES + // .fun
        REFERENCE_BYTES + // .args
        INT_BYTES // .arity
    )

    val PCLOSURE_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES + // .label
        REFERENCE_BYTES + // .expr
        REFERENCE_BYTES // from
    )

    val PClosureSize = LinearPolynomial(
      PCLOSURE_SHELL_BYTES + AnyRefArraySeqSize.a,
      AnyRefArraySeqSize.b,
    )

    val KPURE_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES // .f
    )

    val KOVERAPP_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        INT_BYTES + // .savedBase
        REFERENCE_BYTES + // .frame
        REFERENCE_BYTES + // .actuals
        REFERENCE_BYTES // .newArgs
    )

    val KPUSHTO_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES + // .frame
        REFERENCE_BYTES + // .actuals
        INT_BYTES + // .base
        REFERENCE_BYTES // .next
    )
    val KFOLDL_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES + // .frame
        REFERENCE_BYTES + // .actuals
        REFERENCE_BYTES + // .func
        REFERENCE_BYTES // .list
    )

    val KFOLDR_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES + // .frame
        REFERENCE_BYTES + // .actuals
        REFERENCE_BYTES + // .func
        REFERENCE_BYTES + // .list
        INT_BYTES // .lastIndex
    )

    val KCACHEVAL_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES + // .v
        REFERENCE_BYTES // .defn
    )

    val KCLOSEEXERCISE_SHELL_BYTES = 0L

    val KTRYCATCHHANDLER_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES + // .machine
        INT_BYTES + // .savedBase
        REFERENCE_BYTES + // .frame
        REFERENCE_BYTES + // .actuals
        REFERENCE_BYTES // .handler
    )

    val KLABELCLOSURE_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES // .label
    )

    val KLEAVECLOSURE_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES + // .machine
        REFERENCE_BYTES // .label
    )

    val KPREVENTEXCEPTION_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES // Header
    )

    val KCONVERTINGEXCEPTION_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES // .exceptionId
    )

    val KONT_SHELL_MAX_SIZE =
      KPURE_SHELL_BYTES max
        KOVERAPP_SHELL_BYTES max
        KPUSHTO_SHELL_BYTES max
        KFOLDL_SHELL_BYTES max
        KFOLDR_SHELL_BYTES max
        KCACHEVAL_SHELL_BYTES max
        KCLOSEEXERCISE_SHELL_BYTES max
        KTRYCATCHHANDLER_SHELL_BYTES max
        KLABELCLOSURE_SHELL_BYTES max
        KLEAVECLOSURE_SHELL_BYTES max
        KPREVENTEXCEPTION_SHELL_BYTES max
        KCONVERTINGEXCEPTION_SHELL_BYTES

    override val KontStackIncrease: CostFunction1[Int] =
      KONT_SHELL_MAX_SIZE * _
    override val EnvIncrease: CostFunction1[Int] =
      REFERENCE_BYTES * _

    val SANY_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES + // .value
        REFERENCE_BYTES // .type
    )

    val STYPEREP_SHELL_BYTES = roundTo8(
      OBJECT_HEADER_BYTES + // Header
        REFERENCE_BYTES // .type
    )

    // enterApplicationCost two ArraySeqs with a total size of newArgs.size + fun.actuals.size
    private val enterApplicationCost = LinearPolynomial(AnyRefArraySize.a * 2, AnyRefArraySize.b)

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
    override val BGenMapInsert: CostFunction3[SValue, SValue, SGenMap] = CostFunction3.Null
    override val BGenMapLookup: CostFunction2[SValue, SGenMap] = CostFunction2.Null
    override val BGenMapDelete: CostFunction2[SValue, SGenMap] = CostFunction2.Null
    override val BGenMapToList: CostFunction1[SGenMap] = CostFunction1.Null
    override val BGenMapKeys: CostFunction1[SGenMap] = CostFunction1.Null
    override val BGenMapValues: CostFunction1[SGenMap] = CostFunction1.Null
    override val BGenMapSize: CostFunction1[SGenMap] =
      CostFunction1.Constant(SInt64Size)
    override val BAppendText: CostFunction2[Text, Text] = {
      val poly = LinearPolynomial(STextWrapperSize, StringSize.b)
      (x: String, y: String) => poly.calculate(x.length + y.length)
    }
    override val BError: CostConstant = CostConstant(
      roundTo8(OBJECT_HEADER_BYTES + REFERENCE_BYTES)
    )
    override val BInt64ToText: CostFunction1[Int64] =
      // 20 = "-9223372036854775808".length
      CostFunction1.Constant(STextWrapperSize + AsciiStringSize.calculate(20))
    override val BNumericToText: CostFunction1[Numeric] = {
      // 40 = "-99999999999999999999999999999999999999.".length
      CostFunction1.Constant(STextWrapperSize + AsciiStringSize.calculate(40))
    }
    override val BFoldl: CostConstant = CostConstant(0)
    override val BFoldr: CostConstant = CostConstant(0)
    override val BTimestampToText: CostFunction1[Timestamp] =
      // 27 = "9999-12-31T23:59:59.999999Z".length
      CostFunction1.Constant(STextWrapperSize + AsciiStringSize.calculate(27))
    override val BPartyToText: CostFunction1[Party] =
      CostFunction1.Constant(STextWrapperSize) // We reuse the underlying string
    override val BContractIdToText: CostFunction1[ContractId] =
      // Only for V1 ContractId
      // Remy: We may want to shorted for canton
      CostFunction1.Constant(
        SOptionalSize + STextWrapperSize + AsciiStringSize.calculate(254)
      )
    override val BCodePointsToText: CostFunction1[FrontStack[Long]] = {
      // a code point is at most 2 chars in UTF-16
      val poly = LinearPolynomial(STextWrapperSize, StringSize.b * 2)
      (codePoints: FrontStack[Long]) => poly.calculate(codePoints.length)
    }
    override val BTextToParty: CostFunction1[Text] =
      CostFunction1.Constant(
        SOptionalSize + SPartyWrapperSize
      ) // We reuse the underlying string
    override val BTextToInt64: CostFunction1[Text] =
      CostFunction1.Constant(SOptionalSize + SInt64Size)
    override val BTextToNumeric: CostFunction2[Int, Text] =
      CostFunction2.Constant(SOptionalSize + SNumericWrapperSize + NumericSize)
    override val BTextToCodePoints: CostFunction1[Text] = {
      val poly = LinearPolynomial(SListSize.a, FrontStackSize.b + SInt64Size)
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
    override val BSECP256K1ValidateKey: CostFunction1[Text] =
      CostFunction1.Constant(SBoolSize)
    override val BDecodeHex: CostFunction1[Text] = {
      val poly = LinearPolynomial(STextWrapperSize + StringSize.a, StringSize.b / 2)
      (t: String) => poly.calculate(t.length)
    }
    override val BEncodeHex: CostFunction1[Text] = { (t: String) =>
      STextWrapperSize + StringSize.a + StringSize.b * t.length / 2
    }
    override val BDateToUnixDays: CostFunction1[Date] =
      CostFunction1.Constant(SInt64Size)
    override val BExplodeText: CostFunction1[Text] = {
      val poly =
        LinearPolynomial(SListSize.a, SListSize.b + STextWrapperSize + StringSize.calculate(1))
      (text: String) => poly.calculate(text.length)
    }
    override val BImplodeText: CostFunction1[FrontStack[SValue]] = {
      val poly = LinearPolynomial(STextWrapperSize + StringSize.a, StringSize.b)
      (list: FrontStack[SValue]) =>
        // take care as computation of n should use bounded memory
        val n = list.iterator.map {
          case SValue.SText(s) => s.length
          case _ => 0 // should not happen
        }.sum
        poly.calculate(n)
    }
    override val BTimestampToUnixMicroseconds: CostFunction1[Timestamp] =
      CostFunction1.Constant(SInt64Size)
    override val BDateToText: CostFunction1[Date] =
      //  10 = "9999-12-31".length
      CostFunction1.Constant(STextWrapperSize + StringSize.calculate(10))
    override val BUnixDaysToDate: CostFunction1[Int64] =
      CostFunction1.Constant(SDateWrapperSize + DateSize)
    override val BUnixMicrosecondsToTimestamp: CostFunction1[Int64] =
      CostFunction1.Constant(STimestampWrapperSize + TimestampSize)
    override val BEqual: CostFunction2[SValue, SValue] = CostFunction2.Constant(SBoolSize)
    override val BLess: CostFunction2[SValue, SValue] = CostFunction2.Constant(SBoolSize)
    override val BLessEq: CostFunction2[SValue, SValue] = CostFunction2.Constant(SBoolSize)
    override val BGreater: CostFunction2[SValue, SValue] = CostFunction2.Constant(SBoolSize)
    override val BGreaterEq: CostFunction2[SValue, SValue] = CostFunction2.Constant(SBoolSize)
    override val BEqualList: CostConstant = CostConstant(enterApplicationCost.calculate(3))
    override val BTrace: CostConstant = CostConstant(0)
    override val BCoerceContractId: CostFunction1[ContractId] = CostFunction1.Constant(0L)

    override val EVal: CostConstant = CostConstant(0L)
    override val ERecCon: CostFunction1[Int] = SRecordWrapperSize.calculate(_)
    override val ERecProj: CostFunction1[Int] = CostFunction1.Constant(0L)
    override val ERecUp: CostFunction1[Int] = SRecordWrapperSize.calculate(_)
    override val EVariantCon: CostConstant = CostConstant(SVariantWrapperSize)
    override val EEnumCon: CostConstant = CostConstant.Null // we do not charge constant
    override val EStructCon: CostFunction1[Int] = SStructWrapperSize.calculate(_)
    override val EStructProj: CostFunction1[Int] =
      CostFunction1.Null // no allocation needed for projection
    override val EStructUp: CostFunction1[Int] = SStructWrapperSize.calculate(_)
    override val EApp: CostFunction2[Int, Int] =
      (nActualArgs: Int, nNewArgs: Int) => enterApplicationCost.calculate(nActualArgs + nNewArgs)
    override val EAbs: CostConstant = CostConstant.Null
    override val ECase: CostConstant = CostConstant.Null
    override val ELet: CostConstant = CostConstant.Null
    override val ENil: CostConstant = CostConstant.Null // we do not charge for constant
    override val ECons: CostFunction2[SValue, SValue] =
      CostFunction2.Constant(SLIST_SHELL_BYTES + SCONS_BYTES)
    override val ENone: CostConstant = CostConstant.Null // we do not charge constants
    override val ESome: CostConstant = CostConstant(SOptionalSize)
    override val EToAny: CostConstant = CostConstant(SANY_SHELL_BYTES)
    override val EFromAny: CostConstant = CostConstant(SOptionalSize)
    override val ETypeRep: CostConstant = CostConstant(STYPEREP_SHELL_BYTES)
    override val EToInterface: CostConstant = EToAny
    override val EFromInterface: CostFunction1[SAnyContract] = CostFunction1.CostAware
    override val EToRequiredInterface: CostConstant = CostConstant.Null // identity at runtime
    override val EFromRequiredInterface: CostConstant = CostConstant(SOptionalSize)
    override val EInterfaceTemplateTypeRep: CostConstant = CostConstant(STYPEREP_SHELL_BYTES)
    override val EVar: CostConstant =
      CostConstant.Null // we do not memory for looking up a variable

    override val EMakeClosure: CostFunction1[Int] =
      (nFVs: Int) => SPAPWrapperSize + PClosureSize.calculate(nFVs)

    override val KOverApp: CostFunction2[Int, Int] = { (nActualArgs: Int, nNewArgs: Int) =>
      enterApplicationCost.calculate(nActualArgs + nNewArgs)
    }

    override val KPushTo: CostFunction2[Int, Env] = CostFunction2.Null // no memory allocation
    override val KFoldl: CostFunction1[Int] =
      (actualArgs: Int) => enterApplicationCost.calculate(actualArgs + 2)
    override val KFoldr: CostFunction1[Int] = { (actualArgs: Int) =>
      enterApplicationCost.calculate(actualArgs + 2)
    }
    override val KCacheVal: CostConstant = CostConstant(
      0L
    ) // We do not charge for caching the result
    override val KCloseExercise: CostFunction1[SValue] = CostFunction1.Null // no memory allocation
    override val KTryCatchHandler: CostConstant = CostConstant.Null // no memory allocation
    override val KPreventException: CostConstant = CostConstant.Null // no memory allocation
    override val KConvertingException: CostConstant = CostConstant.Null // no memory allocation
  }
}
