// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding

import com.digitalasset.ledger.api.refinements.ApiTypes.{ContractId, Party}
import com.digitalasset.ledger.api.v1.value.Value.{Sum => VSum}
import com.digitalasset.ledger.api.v1.{value => rpcvalue}
import com.digitalasset.ledger.client.binding.{Primitive => P}

import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom
import scala.{specialized => sp}
import scalaz.std.option._
import scalaz.syntax.traverse._
import com.github.ghik.silencer.silent

sealed trait DamlCodecs // always include `object DamlCodecs` in implicit search

// Should be covariant, but that prevents P.List resolution from happening (see
// test "be found for lists" in ValueSpec)
sealed trait ValueDecoder[@sp(Long) A] extends DamlCodecs {
  def read(argumentValue: VSum): Option[A]
}

sealed trait ValueEncoder[@sp(Long) -A] extends DamlCodecs {
  def write(obj: A): VSum
}

/** Typeclass of "serializable" types as defined by the LF specification.
  *
  * @tparam A Scala representation for some DAML ''serializable''
  *         type. Specialized to match [[Primitive]].
  */
sealed trait Value[@sp(Long) A] extends ValueDecoder[A] with ValueEncoder[A]

object Value {
  // Replace all three with imp if available.
  @inline def apply[@sp(Long) A](implicit A: Value[A]): Value[A] = A

  @inline def Decoder[@sp(Long) A](implicit A: ValueDecoder[A]): ValueDecoder[A] = A

  @inline def Encoder[@sp(Long) A](implicit A: ValueEncoder[A]): ValueEncoder[A] = A

  @inline def decode[@sp(Long) A](a: rpcvalue.Value)(implicit A: ValueDecoder[A]): Option[A] =
    A read a.sum

  @inline def encode[@sp(Long) A](a: A)(implicit A: ValueEncoder[A]): rpcvalue.Value =
    rpcvalue.Value(A write a)

  // TODO remove this suppression, sadly now we do inherit from `Value ValueRef`
  @SuppressWarnings(Array("org.wartremover.warts.LeakingSealed"))
  private[binding] trait InternalImpl[A] extends Value[A]

  // TODO remove dependency on deprecated .name and drop this @silent
  @silent
  private[binding] def splattedVariantId(
      baseVariantId: rpcvalue.Identifier,
      caseName: String
  ): rpcvalue.Identifier =
    baseVariantId copy (entityName = s"${baseVariantId.entityName}.$caseName")
}

object DamlCodecs extends encoding.ValuePrimitiveEncoding[Value] {
  @inline private[this] def fromArgumentValueFuns[@sp(Long) A](
      _read: VSum => Option[A],
      _write: A => VSum): Value[A] =
    new Value[A] {
      override def read(argumentValue: VSum): Option[A] = _read(argumentValue)
      override def write(obj: A): VSum = _write(obj)
    }

  implicit override val valueInt64: Value[P.Int64] = new Value[P.Int64] {
    override def read(argumentValue: VSum): Option[P.Int64] =
      argumentValue.int64

    override def write(obj: P.Int64): VSum = VSum.Int64(obj)
  }

  implicit override val valueNumeric: Value[P.Numeric] =
    fromArgumentValueFuns(_.numeric map BigDecimal.exact, bd => VSum.Numeric(bd.toString))

  implicit override val valueParty: Value[P.Party] =
    Party.subst(fromArgumentValueFuns(_.party, VSum.Party))

  implicit override val valueText: Value[P.Text] =
    fromArgumentValueFuns(_.text, VSum.Text)

  implicit val valueDate: Value[P.Date] = P.Date.subst {
    import java.time.LocalDate
    fromArgumentValueFuns(
      _.date flatMap (i => P.Date.fromLocalDate(LocalDate ofEpochDay i.toLong)),
      ld => VSum.Date(ld.toEpochDay.toInt))
  }

  implicit override val valueTimestamp: Value[P.Timestamp] = P.Timestamp.subst {
    import com.digitalasset.api.util.TimestampConversion.{microsToInstant, instantToMicros}
    fromArgumentValueFuns({
      case ts @ VSum.Timestamp(_) => Some(microsToInstant(ts))
      case _ => None
    }, instantToMicros)
  }

  implicit override val valueUnit: Value[P.Unit] = {
    val decoded = Some(())
    val encoded = VSum.Unit(com.google.protobuf.empty.Empty())
    fromArgumentValueFuns(_ => decoded, _ => encoded)
  }

  implicit override val valueBool: Value[P.Bool] =
    fromArgumentValueFuns(_.bool, VSum.Bool)

  private[this] def seqAlterTraverse[A, B, That](xs: Iterable[A])(f: A => Option[B])(
      implicit cbf: CanBuildFrom[Nothing, B, That]): Option[That] = {
    val bs = cbf()
    val i = xs.iterator
    @tailrec def go(): Option[That] =
      if (i.hasNext) f(i.next) match {
        case Some(b) =>
          bs += b
          go()
        case None => None
      } else Some(bs.result)
    go()
  }

  implicit override def valueList[A](implicit A: Value[A]): Value[P.List[A]] =
    fromArgumentValueFuns(
      _.list flatMap (gl => seqAlterTraverse(gl.elements)(Value.decode[A](_))),
      as => VSum.List(rpcvalue.List(as map (Value.encode(_)))))

  implicit override def valueContractId[A]: Value[P.ContractId[A]] =
    Primitive.substContractId(
      ContractId.subst(fromArgumentValueFuns(_.contractId, VSum.ContractId)))

  implicit override def valueOptional[A](implicit A: Value[A]): Value[P.Optional[A]] =
    fromArgumentValueFuns(
      _.optional flatMap (_.value traverse (Value.decode[A](_))),
      oa => VSum.Optional(rpcvalue.Optional(oa map (Value.encode(_)))))

  implicit override def valueMap[A](implicit A: Value[A]): Value[P.Map[A]] =
    fromArgumentValueFuns(
      _.map.flatMap(gm =>
        seqAlterTraverse(gm.entries)(e => e.value.flatMap(Value.decode[A](_)).map(e.key -> _))),
      oa =>
        VSum.Map(rpcvalue.Map(oa.map {
          case (key, value) =>
            rpcvalue.Map.Entry(
              key = key,
              value = Some(Value.encode(value))
            )
        }.toSeq))
    )

  implicit override def valueGenMap[K, V](
      implicit K: Value[K],
      V: Value[V]
  ): Value[P.GenMap[K, V]] =
    fromArgumentValueFuns(
      _.genMap.flatMap(gm =>
        seqAlterTraverse(gm.entries)(e =>
          for {
            k <- e.key.flatMap(Value.decode[K](_))
            v <- e.value.flatMap(Value.decode[V](_))
          } yield k -> v)),
      oa =>
        VSum.GenMap(rpcvalue.GenMap(oa.map {
          case (key, value) =>
            rpcvalue.GenMap.Entry(
              key = Some(Value.encode(key)),
              value = Some(Value.encode(value))
            )
        }.toSeq))
    )

}

/** Common superclass of record and variant classes' companions. */
abstract class ValueRefCompanion {
  // This class is not generally safe for generic records/variants,
  // so is hidden. It's the only externally visible opening in the
  // Value typeclass.
  //
  // TODO remove this suppression, sadly now we do inherit from `Value ValueRef`
  @SuppressWarnings(Array("org.wartremover.warts.LeakingSealed"))
  protected abstract class `Value ValueRef`[A] extends Value[A]

  protected val ` dataTypeId`: rpcvalue.Identifier
  // recordId and variantId are optional when submitting commands, according to
  // value.proto

  protected final def ` mkDataTypeId`(
      packageId: String,
      moduleName: String,
      entityName: String): rpcvalue.Identifier =
    rpcvalue.Identifier(packageId = packageId, moduleName = moduleName, entityName = entityName)

  protected final def ` record`(elements: (String, rpcvalue.Value)*): VSum.Record =
    VSum.Record(Primitive.arguments(` dataTypeId`, elements))

  protected final def ` variant`(constructor: String, value: rpcvalue.Value): VSum.Variant =
    VSum.Variant(
      rpcvalue
        .Variant(variantId = Some(` dataTypeId`), constructor = constructor, Some(value)))

  protected final def ` enum`(constructor: String): VSum.Enum =
    VSum.Enum(rpcvalue.Enum(enumId = Some(` dataTypeId`), constructor))

  protected final def ` createVariantOfSynthRecord`(
      k: String,
      o: (String, rpcvalue.Value)*): VSum.Variant =
    ` variant`(
      k,
      rpcvalue.Value(
        VSum.Record(Primitive.arguments(Value.splattedVariantId(` dataTypeId`, k), o))))
}
