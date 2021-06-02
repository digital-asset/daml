// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

import com.daml.lf.data.Ref._

sealed abstract class LookupError extends Product with Serializable {
  def pretty: String
}

object LookupError {

  final case class Package(packageId: PackageId) extends LookupError {
    def pretty: String = s"unknown package: $packageId"
  }

  final case class Module(packageId: PackageId, moduleRef: ModuleName) extends LookupError {
    def pretty: String = s"unknown module: $moduleRef"
  }

  final case class Definition(conName: TypeConName) extends LookupError {
    def pretty: String = s"unknown definition: ${conName.qualifiedName}"
  }

  final case class TypeSyn(syn: TypeSynName) extends LookupError {
    def pretty: String = s"unknown type synonym: ${syn.qualifiedName}"
  }

  final case class DataType(conName: TypeConName) extends LookupError {
    def pretty: String = s"unknown data type: ${conName.qualifiedName}"
  }

  final case class DataRecord(tyCon: TypeConName) extends LookupError {
    def pretty: String = s"unknown record: ${tyCon.qualifiedName}"
  }

  final case class DataRecordField(tyCon: TypeConName, conName: Ast.VariantConName)
      extends LookupError {
    def pretty: String = s"unknown record field: ${tyCon.qualifiedName} {$conName}"
  }

  final case class DataVariant(tyCon: TypeConName) extends LookupError {
    def pretty: String = s"unknown variant: ${tyCon.qualifiedName}"
  }

  final case class DataVariantConstructor(tyCon: TypeConName, conName: Ast.VariantConName)
      extends LookupError {
    def pretty: String = s"unknown variant constructor: ${tyCon.qualifiedName}:$conName"
  }

  final case class DataEnum(tyCon: TypeConName) extends LookupError {
    def pretty: String = s"unknown enumeration: ${tyCon.qualifiedName}"
  }

  final case class DataEnumConstructor(tyCon: TypeConName, conName: Ast.EnumConName)
      extends LookupError {
    def pretty: String = s"unknown enumeration constructor: ${tyCon.qualifiedName}:$conName"
  }

  final case class Value(valName: ValueRef) extends LookupError {
    def pretty: String = s"unknown value: ${valName.qualifiedName}"
  }

  final case class Template(conName: TypeConName) extends LookupError {
    def pretty: String = s"unknown template: ${conName.qualifiedName}"
  }

  final case class TemplateKey(conName: TypeConName) extends LookupError {
    def pretty: String = s"template without contract key: ${conName.qualifiedName}"
  }

  final case class Choice(conName: TypeConName, choiceName: ChoiceName) extends LookupError {
    def pretty: String = s"unknown choice: ${conName.qualifiedName}:$choiceName"
  }

  final case class Exception(conName: TypeConName) extends LookupError {
    def pretty: String = s"unknown exception: ${conName.qualifiedName}"
  }

}
