// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package language

import com.digitalasset.daml.lf.data.{FrontStack, Numeric, Ref}

import scala.collection.immutable.ArraySeq

object StructuralType {
  sealed abstract class StructuralType extends Product with Serializable

  object StructuralType {

    case object UnitT extends StructuralType

    case object BoolT extends StructuralType

    case object Int64T extends StructuralType

    case object DateT extends StructuralType

    case object TimestampT extends StructuralType

    final case class NumericT(scale: Numeric.Scale) extends StructuralType

    case object PartyT extends StructuralType

    case object TextT extends StructuralType

    final case class ContractIdT(templateId: Ref.Identifier) extends StructuralType

    final case class OptionalT(arg: Option[StructuralType]) extends StructuralType

    final case class ListT(arg: FrontStack[StructuralType]) extends StructuralType

    final case class MapT(arg: ArraySeq[(StructuralType, StructuralType)]) extends StructuralType

    final case class TextMapT(arg: ArraySeq[StructuralType]) extends StructuralType

    final case class RecordT(
        tyCon: Ref.QualifiedName,
        pkgName: Ref.PackageName,
        fieldInfo: ArraySeq[(Ref.Name, Int, StructuralType)],
    ) extends StructuralType

    final case class VariantT(
        tyCon: Ref.QualifiedName,
        pkgName: Ref.PackageName,
        variant: Ref.Name,
        variantIndex: Int,
        variantType: StructuralType,
    ) extends StructuralType

    final case class EnumT(
        tyCon: Ref.QualifiedName,
        pkgName: Ref.PackageName,
        cons: Ref.Name,
        consIndex: Int,
    ) extends StructuralType
  }

  sealed abstract class Error extends Throwable

  object Error {
    final case class StructuralTypeError(msg: String) extends Error
  }
}
