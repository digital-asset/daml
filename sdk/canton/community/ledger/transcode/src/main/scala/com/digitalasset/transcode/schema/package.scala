// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode

/** Note: code in this package is a translation to scala2 of code from
  * https://github.com/DACH-NY/transcode
  */
package schema {
  final case class Identifier(packageId: String, moduleName: String, entityName: String) {
    lazy val qualifiedName = s"$moduleName:$entityName"
  }

  object Identifier {
    private[transcode] def fromString(str: String): Identifier = str.split(':') match {
      case Array(pkgId, module, name) => Identifier(pkgId, module, name)
      case other => throw new Exception(s"Unsupported identifier format: $str")
    }
  }
}
package object schema {

  type TypeVarName = String

  def TypeVarName(value: String): TypeVarName = value

  implicit class TypeVarNameExtension(value: TypeVarName) {
    def typeVarName: String = value
  }

  type FieldName = String

  def FieldName(value: String): FieldName = value

  implicit class FieldNameExtensiond(value: FieldName) {
    def fieldName: String = value
  }
  type EnumConName = String

  def EnumConName(value: String): EnumConName = value

  implicit class EnumConNameExtension(value: EnumConName) {
    def enumConName: String = value
  }

  type ChoiceName = String

  def ChoiceName(value: String): ChoiceName = value

  implicit class ChoiceNameExtension(value: ChoiceName) {
    def choiceName: String = value
  }

  type VariantConName = String

  def VariantConName(value: String): VariantConName = value

  implicit class VariantConNameExtension(value: VariantConName) {
    def variantConName: String = value
  }
}
