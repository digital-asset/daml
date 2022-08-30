// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

package object iface {
  @deprecated("renamed to typesig.EnvironmentSignature", since = "2.4.0")
  type EnvironmentInterface = typesig.EnvironmentSignature
  @deprecated("renamed to typesig.EnvironmentSignature", since = "2.4.0")
  final val EnvironmentInterface = typesig.EnvironmentSignature

  @deprecated("renamed to typesig.FieldWithType", since = "2.4.0")
  type FieldWithType = typesig.FieldWithType

  @deprecated("moved to typesig.DefDataType", since = "2.4.0")
  type DefDataType[+RF, +VF] = typesig.DefDataType[RF, VF]
  @deprecated("moved to typesig.DefDataType", since = "2.4.0")
  final val DefDataType = typesig.DefDataType

  @deprecated("moved to typesig.DataType", since = "2.4.0")
  type DataType[+RT, +VT] = typesig.DataType[RT, VT]
  @deprecated("moved to typesig.DataType", since = "2.4.0")
  final val DataType = typesig.DataType

  @deprecated("moved to typesig.Record", since = "2.4.0")
  type Record[+RT] = typesig.Record[RT]
  @deprecated("moved to typesig.Record", since = "2.4.0")
  final val Record = typesig.Record

  @deprecated("moved to typesig.Variant", since = "2.4.0")
  type Variant[+VT] = typesig.Variant[VT]
  @deprecated("moved to typesig.Variant", since = "2.4.0")
  final val Variant = typesig.Variant

  @deprecated("moved to typesig.Enum", since = "2.4.0")
  type Enum = typesig.Enum
  @deprecated("moved to typesig.Enum", since = "2.4.0")
  final val Enum = typesig.Enum

  @deprecated("moved to typesig.DefTemplate", since = "2.4.0")
  type DefTemplate[+Ty] = typesig.DefTemplate[Ty]
  @deprecated("moved to typesig.DefTemplate", since = "2.4.0")
  final val DefTemplate = typesig.DefTemplate

  @deprecated("moved to typesig.TemplateChoices", since = "2.4.0")
  type TemplateChoices[+Ty] = typesig.TemplateChoices[Ty]
  @deprecated("moved to typesig.TemplateChoices", since = "2.4.0")
  final val TemplateChoices = typesig.TemplateChoices

  @deprecated("moved to typesig.TemplateChoice", since = "2.4.0")
  type TemplateChoice[+Ty] = typesig.TemplateChoice[Ty]
  @deprecated("moved to typesig.TemplateChoice", since = "2.4.0")
  final val TemplateChoice = typesig.TemplateChoice

  @deprecated("moved to typesig.DefInterface", since = "2.4.0")
  type DefInterface[+Ty] = typesig.DefInterface[Ty]
  @deprecated("moved to typesig.DefInterface", since = "2.4.0")
  final val DefInterface = typesig.DefInterface

  @deprecated("renamed to typesig.PackageSignature.TypeDecl", since = "2.4.0")
  type InterfaceType = typesig.PackageSignature.TypeDecl
  @deprecated("renamed to typesig.PackageSignature.TypeDecl", since = "2.4.0")
  final val InterfaceType = typesig.PackageSignature.TypeDecl

  @deprecated("moved to typesig.PackageMetadata", since = "2.4.0")
  type PackageMetadata = typesig.PackageMetadata
  @deprecated("moved to typesig.PackageMetadata", since = "2.4.0")
  final val PackageMetadata = typesig.PackageMetadata

  @deprecated("renamed to typesig.PackageSignature", since = "2.4.0")
  type Interface = typesig.PackageSignature
  @deprecated("renamed to typesig.PackageSignature", since = "2.4.0")
  final val Interface = typesig.PackageSignature

  @deprecated("moved to typesig.Type", since = "2.4.0")
  type Type = typesig.Type
  @deprecated("moved to typesig.Type", since = "2.4.0")
  final val Type = typesig.Type
  @deprecated("moved to typesig.TypeCon", since = "2.4.0")
  type TypeCon = typesig.TypeCon
  @deprecated("moved to typesig.TypeCon", since = "2.4.0")
  final val TypeCon = typesig.TypeCon
  @deprecated("moved to typesig.TypeNumeric", since = "2.4.0")
  type TypeNumeric = typesig.TypeNumeric
  @deprecated("moved to typesig.TypeNumeric", since = "2.4.0")
  final val TypeNumeric = typesig.TypeNumeric
  @deprecated("moved to typesig.TypePrim", since = "2.4.0")
  type TypePrim = typesig.TypePrim
  @deprecated("moved to typesig.TypePrim", since = "2.4.0")
  final val TypePrim = typesig.TypePrim
  @deprecated("moved to typesig.TypeVar", since = "2.4.0")
  type TypeVar = typesig.TypeVar
  @deprecated("moved to typesig.TypeVar", since = "2.4.0")
  final val TypeVar = typesig.TypeVar

  @deprecated("moved to typesig.TypeConNameOrPrimType", since = "2.4.0")
  type TypeConNameOrPrimType = typesig.TypeConNameOrPrimType

  @deprecated("moved to typesig.TypeConName", since = "2.4.0")
  type TypeConName = typesig.TypeConName
  @deprecated("moved to typesig.TypeConName", since = "2.4.0")
  final val TypeConName = typesig.TypeConName
  @deprecated("moved to typesig.PrimType", since = "2.4.0")
  type PrimType = typesig.PrimType
  @deprecated("moved to typesig.PrimType", since = "2.4.0")
  final val PrimType = typesig.PrimType

  @deprecated("moved to typesig.PrimTypeBool", since = "2.4.0")
  final val PrimTypeBool = typesig.PrimTypeBool
  @deprecated("moved to typesig.PrimTypeInt", since = "2.4.0")
  final val PrimTypeInt64 = typesig.PrimTypeInt64
  @deprecated("moved to typesig.PrimTypeText", since = "2.4.0")
  final val PrimTypeText = typesig.PrimTypeText
  @deprecated("moved to typesig.PrimTypeDate", since = "2.4.0")
  final val PrimTypeDate = typesig.PrimTypeDate
  @deprecated("moved to typesig.PrimTypeTimestamp", since = "2.4.0")
  final val PrimTypeTimestamp = typesig.PrimTypeTimestamp
  @deprecated("moved to typesig.PrimTypeParty", since = "2.4.0")
  final val PrimTypeParty = typesig.PrimTypeParty
  @deprecated("moved to typesig.PrimTypeContractId", since = "2.4.0")
  final val PrimTypeContractId = typesig.PrimTypeContractId
  @deprecated("moved to typesig.PrimTypeList", since = "2.4.0")
  final val PrimTypeList = typesig.PrimTypeList
  @deprecated("moved to typesig.PrimTypeUnit", since = "2.4.0")
  final val PrimTypeUnit = typesig.PrimTypeUnit
  @deprecated("moved to typesig.PrimTypeOptional", since = "2.4.0")
  final val PrimTypeOptional = typesig.PrimTypeOptional
  @deprecated("moved to typesig.PrimTypeTextMap", since = "2.4.0")
  final val PrimTypeTextMap = typesig.PrimTypeTextMap
  @deprecated("moved to typesig.PrimTypeGenMap", since = "2.4.0")
  final val PrimTypeGenMap = typesig.PrimTypeGenMap

  @deprecated("moved to typesig.PrimTypeVisitor", since = "2.4.0")
  type PrimTypeVisitor[+Z] = typesig.PrimTypeVisitor[Z]
}
