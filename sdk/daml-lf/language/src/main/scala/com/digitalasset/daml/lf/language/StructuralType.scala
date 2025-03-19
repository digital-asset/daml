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

    final case class ContractIdF(templateId: Ref.Identifier) extends StructuralTypeF

    final case class OptionalF(arg: Option[StructuralTypeF]) extends StructuralTypeF

    final case class ListF(arg: FrontStack[StructuralTypeF]) extends StructuralTypeF

    final case class MapF(arg: ArraySeq[(StructuralTypeF, StructuralTypeF)]) extends StructuralTypeF

    final case class TextMapF(arg: ArraySeq[StructuralTypeF]) extends StructuralTypeF

    final case class RecordF(
        tyCon: Ref.QualifiedName,
        pkgName: Ref.PackageName,
        fieldInfo: ArraySeq[(Ref.Name, Int, StructuralTypeF)],
    ) extends StructuralTypeF

    final case class VariantF(
        tyCon: Ref.QualifiedName,
        pkgName: Ref.PackageName,
        variant: Ref.Name,
        variantIndex: Int,
        variantType: StructuralTypeF,
    ) extends StructuralTypeF

    final case class EnumF(
        tyCon: Ref.QualifiedName,
        pkgName: Ref.PackageName,
        cons: Ref.Name,
        consIndex: Int,
    ) extends StructuralTypeF
  }

  sealed abstract class Error extends Throwable

  object Error {
    final case class StructuralTypeError(msg: String) extends Error
  }
}
