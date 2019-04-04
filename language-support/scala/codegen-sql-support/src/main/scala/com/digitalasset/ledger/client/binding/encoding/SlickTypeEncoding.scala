// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.encoding

import scala.language.higherKinds

import scala.reflect.ClassTag
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.ledger.client.binding.{Primitive => P}
import JdbcJsonTypeCodecs.Codec
import com.digitalasset.ledger.client.binding.encoding.SlickTypeEncoding.SupportedProfile

import scalaz.{-\/, Plus, \/, \/-, ~>}
import slick.lifted._
import spray.{json => sj}
import sj.JsValue
import com.github.ghik.silencer.silent

final class SlickTypeEncoding[+Profile <: SupportedProfile] private (
    val profile: Profile,
    jsonColumnType: Profile#BaseColumnType[JsValue],
    val jsonEncoding: LfTypeEncoding {
      type Out[A] = JsonLfTypeEncoding.Out[A]
    })
    extends LfTypeEncoding {
  import SlickTypeEncoding._
  import profile.{BaseColumnType, ColumnType}

  case class Out[A](jdbc: OwnOut[A], json: jsonEncoding.Out[A])

  type OwnOut[A] = RecordOut[A] \/ BaseColumnType[A]

  case class Field[A](name: String, value: OwnOut[A], jsonField: jsonEncoding.Field[A])

  case class RecordOut[A](recordId: Identifier, fields: OwnRecordFields[A])

  case class RecordFields[A](repShape: OwnRecordFields[A], json: jsonEncoding.RecordFields[A])

  type OwnRecordFields[A] = ColumnContext[RepAndShape[A]] \/ A

  // since we know that the outermost variant will "reset" the JDBC column
  // representation, we can discard all JDBC information here
  type VariantCases[A] = jsonEncoding.VariantCases[A]

  override val VariantCases: Plus[VariantCases] = jsonEncoding.VariantCases

  type ColumnNamePrefix = Option[String]
  type ColumnSource = profile.Table[_]
  type ColumnContext[A] = ColumnNamePrefix => ColumnSource => A

  private[this] def mapColumnContext[A, B](cc: ColumnContext[A])(f: A => B): ColumnContext[B] =
    cc andThen (_ andThen f)

  override def record[A](recordId: Identifier, fi: RecordFields[A]): Out[A] =
    Out(\/.left(RecordOut(recordId, fi.repShape)), jsonEncoding.record(recordId, fi.json))

  override def emptyRecord[A](recordId: Identifier, element: () => A): Out[A] = {
    val vacuousFields: OwnRecordFields[A] = \/.right(element())
    Out(\/.left(RecordOut(recordId, vacuousFields)), jsonEncoding.emptyRecord(recordId, element))
  }

  def table[A](o: Out[A]): (String, Tag => profile.Table[A]) =
    tableComponents(o) match {
      case NonEmptyRecord(tableName, columns) =>
        (
          tableName,
          tag =>
            new profile.Table[A](tag, None, tableName) {
              override val * = columns(this).*
          })
      case _ => sys.error(s"$o is not a record")
    }

  def tableWithColumns[A, View[C[_]] <: RecordView[C, View]](
      o: Out[A],
      fields: View[Field]): (String, Tag => profile.Table[A] with ViewColumns[View]) =
    tableComponents(o) match {
      case NonEmptyRecord(tableName, columns) =>
        val fieldsPrep = fields.hoist[EdslColumn](edslColumn)
        (
          tableName,
          tag =>
            new profile.Table[A](tag, None, tableName) with ViewColumns[View] {
              @SuppressWarnings(Array("org.wartremover.warts.Any"))
              override val c: View[Rep] = fieldsPrep.hoist(funApp(this))
              override val * = columns(this).*
          })
      case _ => sys.error(s"$o is not a record")
    }

  def tableWithId[R, A, B, Z](id: profile.Table[Z] => R, o: Out[B])(to: (A, B) => Z)(
      from: Z => (A, B))(
      implicit ct: ClassTag[Z],
      rshape: Shape[_ <: FlatShapeLevel, R, A, _]): Tag => profile.Table[Z] = {
    val (tableName, star): (String, profile.Table[Z] => ProvenShape[Z]) =
      tableComponents(o) match {
        case nr: NominalRecord[B] =>
          (nr.tableName, nr.shapeWithId(id, to)(from))
        case VariantOrPrimitive(column) =>
          sys.error(s"$column is not a record")
      }
    tag =>
      new profile.Table[Z](tag, None, tableName) {
        override val * = star(this)
      }
  }

  sealed abstract class TableComponents[A] extends Product with Serializable
  sealed abstract class NominalRecord[A] extends TableComponents[A] {
    val tableName: String

    /** Create a contract table by making a product with an "ID" column[s].
      * @param id Function to produce the id descriptor, e.g. `_.column[String]("ID")`.
      * @tparam Id The unpacked type of the id column or columns.
      * @tparam R The "mixed" type of Id, usually a subtype of `Rep[Id]`.
      * @tparam Z The table's projection type.
      * @tparam Y The subtype of `Table` to be constructed. Columns like `id` typically
      *           come from the table (e.g. [[profile.Table#column]], but at the same time
      *           we're trying to make an "input" to the table, requiring literal knot-tying.
      */
    def shapeWithId[Id, R, Z, Y <: ColumnSource](id: Y => R, to: (Id, A) => Z)(from: Z => (Id, A))(
        implicit ct: ClassTag[Z],
        rshape: Shape[_ <: FlatShapeLevel, R, Id, _]): Y => ProvenShape[Z] = {
      import profile.api.anyToShapedValue
      this match {
        case EmptyRecord(_, element) =>
          colSrc =>
            id(colSrc) <> (to(_, element), (z: Z) => Some(from(z)._1))
        case NonEmptyRecord(_, star) =>
          colSrc =>
            {
              val rs = star(colSrc)
              anyToShapedValue((id(colSrc), rs.value))(Shape.tuple2Shape(rshape, rs.shape)) <> (to.tupled, (z: Z) =>
                Some(from(z)))
            }
      }
    }
  }

  // the following silent are due to
  // <https://github.com/scala/bug/issues/4440>
  @silent
  private final case class EmptyRecord[A](tableName: String, constant: A) extends NominalRecord[A]
  @silent
  private final case class NonEmptyRecord[A](
      tableName: String,
      columns: ColumnSource => RepAndShape[A])
      extends NominalRecord[A]
  @silent
  final case class VariantOrPrimitive[A](column: BaseColumnType[A]) extends TableComponents[A]

  def tableComponents[A](o: Out[A]): TableComponents[A] =
    o.jdbc fold ({ recOut =>
      val P.LegacyIdentifier(_, tableName) = recOut.recordId
      recOut.fields fold (ccras => NonEmptyRecord(tableName, ccras(None)),
      EmptyRecord(tableName, _))
    },
    VariantOrPrimitive(_))

  override def field[A](fieldName: String, o: Out[A]): Field[A] =
    Field(fieldName, o.jdbc, jsonEncoding.field(fieldName, o.json))

  private[this] def qualifyColumnName(prefix: ColumnNamePrefix, name: String): String =
    prefix.fold(name)(s => s"$s.$name")

  private[this] def mkEdslColumn[A](fi: Field[A]): EdslColumn[A] =
    jdbcField(fi) fold (ccrsa => ccrsa(None) andThen (_.value), { a =>
      import profile.api.valueToConstColumn
      implicit val att: ColumnType[A] =
        profile.MappedColumnType
          .base[A, Unit](_ => (), _ => a)(fakeClassTag[A], underlyingPrimitive.valueUnit._1)
      val ca: Rep[A] = a
      _ =>
        ca
    })

  type EdslColumn[A] = ColumnSource => Rep[A]

  /** A natural transformation for `view`s to take you 99% of the way
    * to Slick's EDSL support.
    */
  def edslColumn: Field ~> EdslColumn =
    Lambda[Field ~> EdslColumn](mkEdslColumn(_))

  private[this] def jdbcField[A](fi: Field[A]): OwnRecordFields[A] =
    fi.value fold (_.fields.leftMap(_ compose (prefix => Some(qualifyColumnName(prefix, fi.name)))), {
      // tbl.column needs the implicit BaseColumnType[A], which we get for each
      // backend from the profile.
      implicit single: BaseColumnType[A] =>
        \/.left(prefix =>
          (tbl: ColumnSource) => RepAndShape(tbl.column(qualifyColumnName(prefix, fi.name))))
    })

  override def fields[A](fi: Field[A]): RecordFields[A] =
    RecordFields(
      repShape = jdbcField(fi),
      json = jsonEncoding.fields(fi.jsonField)
    )

  override def variant[A](variantId: Identifier, cases: VariantCases[A]): Out[A] = {
    val jsonFormat = jsonEncoding.variant(variantId, cases)

    def decode(jsValue: JsValue): A =
      jsValue.convertTo[A](jsonFormat)

    val columnType: BaseColumnType[A] =
      profile.MappedColumnType
        .base[A, JsValue](jsonFormat.write, decode)(fakeClassTag[A], jsonColumnType)
    Out(\/.right(columnType), jsonFormat)
  }

  override def variantCase[B, A](caseName: String, o: Out[B])(inject: B => A)(
      select: A PartialFunction B): VariantCases[A] =
    jsonEncoding.variantCase(caseName, o.json)(inject)(select)

  object RecordFields extends InvariantApply[RecordFields] {
    import slick.lifted.Shape._
    import profile.api.{FlatShapeLevel => _, Rep => _, Shape => _, _}

    /*
    val shape: Shape[FlatShapeLevel, Rep[Z], Z, Rep[Z]] = provenShapeShape(
      anyToShapedValue(tuple2Shape(fa.shape, fb.shape)).<>(f.tupled, g))
     */

    private def xmapRepAndShape[A, B](fa: RepAndShape[A])(f: A => B)(g: B => A): RepAndShape[B] = {
      implicit val ctb: ClassTag[B] = fakeClassTag[B]
      RepAndShape(anyToShapedValue(fa.value)(fa.shape) <> (f, g andThen (Some(_))))
    }

    private def tuple2RepAndShape[A, B](
        fa: RepAndShape[A],
        fb: RepAndShape[B]): RepAndShape[(A, B)] =
      RepAndShape(anyToShapedValue(fa.value)(fa.shape) zip anyToShapedValue(fb.value)(fb.shape))

    override def xmap[A, B](fa: RecordFields[A], f: A => B, g: B => A): RecordFields[B] =
      RecordFields(
        repShape = fa.repShape bimap (mapColumnContext(_)(xmapRepAndShape(_)(f)(g)), f),
        json = jsonEncoding.RecordFields.xmap(fa.json, f, g))

    override def xmapN[A, B, Z](fa: RecordFields[A], fb: RecordFields[B])(f: (A, B) => Z)(
        g: Z => (A, B)): RecordFields[Z] =
      RecordFields(
        repShape = (fa.repShape, fb.repShape) match {
          case (-\/(faf), -\/(fbf)) =>
            -\/(prefix =>
              tbl =>
                xmapRepAndShape(tuple2RepAndShape(faf(prefix)(tbl), fbf(prefix)(tbl)))(f.tupled)(g))
          case (\/-(a), -\/(fbf)) =>
            -\/(mapColumnContext(fbf)(xmapRepAndShape(_)(f(a, _))(g(_)._2)))
          case (-\/(faf), \/-(b)) =>
            -\/(mapColumnContext(faf)(xmapRepAndShape(_)(f(_, b))(g(_)._1)))
          case (\/-(a), \/-(b)) => \/-(f(a, b))
        },
        json = jsonEncoding.RecordFields.xmapN(fa.json, fb.json)(f)(g)
      )
  }

  private[this] val underlyingPrimitive =
    JdbcJsonTypeCodecs(profile, jsonColumnType, jsonEncoding.primitive)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  override val primitive: ValuePrimitiveEncoding[Out] = {
    val liftCodecs = new (Codec ~> Out) {
      override def apply[A](fa: Codec[A]): Out[A] = {
        val (jdbc, json) = fa
        Out(\/.right(jdbc), json)
      }
    }
    object primitiveList extends (Out ~> Lambda[a => Out[P.List[a]]]) {
      override def apply[A](fa: Out[A]): Out[P.List[A]] =
        liftCodecs(underlyingPrimitive.jsonValueList(fa.json))
    }
    val primitiveOption = Lambda[Out ~> Lambda[a => Out[P.Optional[a]]]] { fa =>
      liftCodecs(underlyingPrimitive.jsonValueOptional(fa.json))
    }
    val primitiveMap = Lambda[Out ~> Lambda[a => Out[P.Map[a]]]] { fa =>
      liftCodecs(underlyingPrimitive.jsonValueMap(fa.json))
    }
    ValuePrimitiveEncoding.mapped(underlyingPrimitive)(liftCodecs)(primitiveList)(primitiveOption)(
      primitiveMap)
  }
}

object SlickTypeEncoding {
  type SupportedProfile = slick.jdbc.JdbcProfile

  // Here we cheat by pulling getting an AnyRef classtag to give to slick.
  // This relies on the fact that slick wants a classtag but doesn't really
  // do anything useful with it. If it ever starts doing something useful
  // with it, we're in trouble.
  private def fakeClassTag[A]: ClassTag[A] =
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[A]]

  // (implicit jcm: JsonColumnMapping[profile.type] = JsonColumnMapping.runtimeMapping(profile))
  def apply(profile: SupportedProfile): SlickTypeEncoding[profile.type] =
    new SlickTypeEncoding(
      profile,
      JsonColumnMapping.runtimeMapping.column(profile),
      JsonLfTypeEncoding)

  sealed abstract class RepAndShape[U] {
    type T <: Rep[U]
    val value: T
    type Level <: FlatShapeLevel
    type P
    implicit val shape: Shape[Level, T, U, P]

    final def * : ProvenShape[U] = ProvenShape.proveShapeOf(value)
  }

  /** `t => f => f(t)`, but we don't have first-class forall in Scala */
  def funApp[T, R[_]](t: T): Lambda[a => T => R[a]] ~> R =
    Lambda[Lambda[a => T => R[a]] ~> R](_(t))

  def RepAndShape[U, T0 <: Rep[U]](value0: T0 with Rep[U])(
      implicit shape0: Shape[_ <: FlatShapeLevel, T0, U, _]): RepAndShape[U] = {
    final case class Instance[Level0 <: FlatShapeLevel, P0](value: T0)(
        implicit val shape: Shape[Level0, T0, U, P0])
        extends RepAndShape[U] {
      type T = T0
      type Level = Level0
      type P = P0
      override def productPrefix = "RepAndShape"
    }
    Instance(value0)
  }

  trait ViewColumns[View[_[_]]] {
    val c: View[Rep]
  }
}
