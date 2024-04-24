// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package language

import com.digitalasset.daml.lf.data.{Numeric, Ref}

import scala.collection.immutable.ArraySeq

sealed abstract class TypeF[+Type] extends Product with Serializable

object TypeF {

  case object UnitF extends TypeF[Nothing]

  case object BoolF extends TypeF[Nothing]

  case object Int64F extends TypeF[Nothing]

  case object DateF extends TypeF[Nothing]

  case object TimestampF extends TypeF[Nothing]

  final case class NumericF(scale: Numeric.Scale) extends TypeF[Nothing]

  case object PartyF extends TypeF[Nothing]

  case object TextF extends TypeF[Nothing]

  final case class ContractIdF[Type](a: Type) extends TypeF[Type]

  final case class OptionalF[Type](a: Type) extends TypeF[Type]

  final case class ListF[Type](a: Type) extends TypeF[Type]

  final case class MapF[Type](a: Type, b: Type) extends TypeF[Type]

  final case class TextMapF[Type](a: Type) extends TypeF[Type]

  final case class RecordF[Type](
      tyCon: Ref.TypeConName,
      pkgName: Ref.PackageName,
      fieldName: ArraySeq[Ref.Name],
      fieldTypes: ArraySeq[Type],
  ) extends TypeF[Type]

  final case class VariantF[Type](
      tyCon: Ref.TypeConName,
      pkgName: Ref.PackageName,
      cons: ArraySeq[Ref.Name],
      consTypes: ArraySeq[Type],
  ) extends TypeF[Type] {
    private[this] lazy val consRankMap = cons.view.zipWithIndex.toMap
    def consRank(cons: Ref.Name): Either[LookupError, Int] = {
      def ref = Reference.DataEnumConstructor(tyCon, cons)
      consRankMap.get(cons).toRight(LookupError.NotFound(ref, ref))
    }
  }

  final case class EnumF(tyCon: Ref.TypeConName, pkgName: Ref.PackageName, cons: ArraySeq[Ref.Name])
      extends TypeF[Nothing] {
    private[this] lazy val consRankMap = cons.view.zipWithIndex.toMap
    def consRank(cons: Ref.Name): Either[LookupError, Int] = {
      def ref = Reference.DataEnumConstructor(tyCon, cons)
      consRankMap.get(cons).toRight(LookupError.NotFound(ref, ref))
    }
  }
}
