// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.typesig

import java.{util => j}

import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.data.{Numeric, Ref}

import scala.jdk.CollectionConverters._

/** [[Type]] is an intermediate form from
  * which record/variant objects or type aliases are generated from.
  *
  * The [[Type]] structure defined in this module defines all types that
  * can validly have a Scala data structure generated from them.
  * It contains type variables which, when present, are used to generate
  * polymorphic Scala data structures.
  */
sealed abstract class Type extends Product with Serializable {

  /** Handle the possible Types without missing one or casting. Meant to be used
    * from Java.
    *
    * @note Normally [[Type]]'s recursive occurrences would be replaced with
    *       `Z`, but we have not done that for closer analogy to pattern
    *       matching.
    */
  def fold[Z](
      typeCon: TypeCon => Z,
      typePrim: TypePrim => Z,
      typeVar: TypeVar => Z,
      typeNum: TypeNumeric => Z,
  ): Z =
    this match {
      case t @ TypeCon(_, _) => typeCon(t)
      case t @ TypePrim(_, _) => typePrim(t)
      case t @ TypeVar(_) => typeVar(t)
      case t @ TypeNumeric(_) => typeNum(t)
    }

  /** Map all type variables that occur anywhere within this type */
  def mapTypeVars(f: TypeVar => Type): Type =
    this match {
      case t @ TypeVar(_) => f(t)
      case t @ TypeCon(_, _) => TypeCon(t.tycon, t.typArgs.map(_.mapTypeVars(f)))
      case t @ TypePrim(_, _) => TypePrim(t.typ, t.typArgs.map(_.mapTypeVars(f)))
      case t @ TypeNumeric(_) => t
    }
}

final case class TypeCon(tycon: TypeConId, typArgs: ImmArraySeq[Type])
    extends Type
    with Type.HasTypArgs {

  /** crashes if the tyvars and type args lengths do not match */
  def instantiate(defn: DefDataType.FWT): DataType.FWT =
    if (defn.typeVars.length != typArgs.length) {
      throw new RuntimeException(
        s"Mismatching type vars and applied types, expected ${defn.typeVars} but got ${typArgs.length} types"
      )
    } else {
      if (defn.typeVars.isEmpty) { // optimization
        defn.dataType
      } else {
        val paramsMap = Map(defn.typeVars.zip(typArgs): _*)
        val instantiateFWT: Type => Type = _.mapTypeVars(v => paramsMap.getOrElse(v.name, v))
        defn.dataType.bimap(instantiateFWT, instantiateFWT)
      }
    }
}

final case class TypeNumeric(scale: Numeric.Scale) extends Type

final case class TypePrim(typ: PrimType, typArgs: ImmArraySeq[Type])
    extends Type
    with Type.HasTypArgs

final case class TypeVar(name: Ref.Name) extends Type

object Type {

  /** Java-friendly typArgs getter. */
  sealed trait HasTypArgs {
    def typArgs: ImmArraySeq[Type]
    def getTypArgs: j.List[_ <: Type] = typArgs.asJava
  }
}

sealed abstract class TypeConIdOrPrimType extends Product with Serializable {
  def fold[Z](TypeConId: TypeConId => Z, primType: PrimType => Z): Z = this match {
    case tc: TypeConId => TypeConId(tc)
    case pt: PrimType => primType(pt)
  }
}

final case class TypeConId(identifier: Ref.TypeConId) extends TypeConIdOrPrimType
sealed abstract class PrimType extends TypeConIdOrPrimType {

  /** Named pattern match for Java. */
  def fold[Z](v: PrimTypeVisitor[Z]): Z = {
    import v._, PrimType._
    this match {
      case Bool => bool
      case Int64 => int64
      case Text => text
      case Date => date
      case Timestamp => timestamp
      case Party => party
      case ContractId => contractId
      case List => list
      case Unit => unit
      case Optional => optional
      case TextMap => map
      case GenMap => genMap
    }
  }
}

object PrimType {
  final val Bool = PrimTypeBool
  final val Int64 = PrimTypeInt64
  final val Text = PrimTypeText
  final val Date = PrimTypeDate
  final val Timestamp = PrimTypeTimestamp
  final val Party = PrimTypeParty
  final val ContractId = PrimTypeContractId
  final val List = PrimTypeList
  final val Unit = PrimTypeUnit
  final val Optional = PrimTypeOptional
  final val TextMap = PrimTypeTextMap
  final val GenMap = PrimTypeGenMap
}

case object PrimTypeBool extends PrimType
case object PrimTypeInt64 extends PrimType
case object PrimTypeText extends PrimType
case object PrimTypeDate extends PrimType
case object PrimTypeTimestamp extends PrimType
case object PrimTypeParty extends PrimType
case object PrimTypeContractId extends PrimType
case object PrimTypeList extends PrimType
case object PrimTypeUnit extends PrimType
case object PrimTypeOptional extends PrimType
case object PrimTypeTextMap extends PrimType
case object PrimTypeGenMap extends PrimType

trait PrimTypeVisitor[+Z] {
  def bool: Z
  def int64: Z
  def text: Z
  def date: Z
  def timestamp: Z
  def party: Z
  def contractId: Z
  def list: Z
  def unit: Z
  def optional: Z
  def map: Z
  def genMap: Z
}
