// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package language

import com.digitalasset.daml.lf.data.{FrontStack, Numeric, Ref}

import scala.collection.immutable.ArraySeq

object StructuralType {
  sealed abstract class StructuralTypeF extends Product with Serializable

  object StructuralTypeF {

    case object UnitF extends StructuralTypeF

    case object BoolF extends StructuralTypeF

    case object Int64F extends StructuralTypeF

    case object DateF extends StructuralTypeF

    case object TimestampF extends StructuralTypeF

    final case class NumericF(scale: Numeric.Scale) extends StructuralTypeF

    case object PartyF extends StructuralTypeF

    case object TextF extends StructuralTypeF

    final case class ContractIdF(arg: StructuralTypeF) extends StructuralTypeF

    final case class OptionalF(arg: StructuralTypeF) extends StructuralTypeF

    final case class ListF(arg: StructuralTypeF) extends StructuralTypeF

    final case class StructuralListF(arg: FrontStack[StructuralTypeF]) extends StructuralTypeF

    final case class MapF(keyT: StructuralTypeF, valueT: StructuralTypeF) extends StructuralTypeF

    final case class StructuralMapF(arg: ArraySeq[(StructuralTypeF, StructuralTypeF)])
        extends StructuralTypeF

    final case class TextMapF(arg: StructuralTypeF) extends StructuralTypeF

    final case class StructuralTextMapF(arg: ArraySeq[StructuralTypeF]) extends StructuralTypeF

    final case class RecordF(
        tyCon: Ref.TypeConName,
        pkgName: Ref.PackageName,
        fieldNames: ArraySeq[Ref.Name],
        fieldTypes: ArraySeq[StructuralTypeF],
    ) extends StructuralTypeF

    final case class VariantF(
        tyCon: Ref.TypeConName,
        pkgName: Ref.PackageName,
        cons: ArraySeq[Ref.Name],
        consTypes: ArraySeq[StructuralTypeF],
    ) extends StructuralTypeF

    final case class StructuralVariantF(
        tyCon: Ref.TypeConName,
        pkgName: Ref.PackageName,
        cons: ArraySeq[Ref.Name],
        consTypes: ArraySeq[StructuralTypeF],
        variant: Ref.Name,
    ) extends StructuralTypeF

    final case class EnumF(
        tyCon: Ref.TypeConName,
        pkgName: Ref.PackageName,
        cons: ArraySeq[Ref.Name],
    ) extends StructuralTypeF
  }

  sealed abstract class Error extends Throwable

  object Error {
    final case class StructuralTypeError(msg: String) extends Error
  }
}
