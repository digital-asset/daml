// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode

import java.nio.ByteBuffer

package object schema:
  type Schema = Dictionary[Descriptor]
  object Schema:
    import boopickle.Default.*
    import com.digitalasset.transcode.schema.Pickler.given

    def serialize(d: Schema): Array[Byte] = Pickle.intoBytes(d).array()
    def deserialize(d: Array[Byte]): Schema = Unpickle[Schema].fromBytes(ByteBuffer.wrap(d))

  opaque type PackageId <: String = String
  def PackageId(value: String): PackageId = value.intern()
  extension (value: PackageId) def packageId: String = value

  opaque type PackageName <: String = String
  def PackageName(value: String): PackageName = value.intern()
  extension (value: PackageName) def packageName: String = value

  opaque type PackageVersion <: String = String
  def PackageVersion(value: String): PackageVersion = value.intern()
  extension (value: PackageVersion) def packageVersion: String = value
  object PackageVersion:
    val Unknown: PackageVersion = PackageVersion("")

  opaque type ModuleName <: String = String
  def ModuleName(value: String): ModuleName = value.intern()
  extension (value: ModuleName) def moduleName: String = value

  opaque type EntityName <: String = String
  def EntityName(value: String): EntityName = value
  extension (value: EntityName) def entityName: String = value

  opaque type ChoiceName <: String = String
  def ChoiceName(value: String): ChoiceName = value
  extension (value: ChoiceName) def choiceName: String = value
  object ChoiceName:
    val Archive: ChoiceName = ChoiceName("Archive")

  opaque type TypeVarName <: String = String
  def TypeVarName(value: String): TypeVarName = value
  extension (value: TypeVarName) def typeVarName: String = value

  opaque type FieldName <: String = String
  def FieldName(value: String): FieldName = value
  extension (value: FieldName) def fieldName: String = value

  opaque type EnumConName <: String = String
  def EnumConName(value: String): EnumConName = value
  extension (value: EnumConName) def enumConName: String = value

  opaque type VariantConName <: String = String
  def VariantConName(value: String): VariantConName = value
  extension (value: VariantConName) def variantConName: String = value

  type DynamicValue = DynamicValue.Type
