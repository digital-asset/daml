// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package value.json

import data.{Ref => DamlLfRef}
import value.{Value => V}

/** Aliases used by navigator's Scala backend, from which this package
  * was derived.
  */
trait NavigatorModelAliases[Cid] {

  /** An absolute reference of a Daml-LF entity.
    * Contains a Daml-LF package ID and a qualified name.
    * Currently, such identifiers can point to:
    * - Templates
    * - User-defined records
    * - User-defined variants
    */
  type DamlLfIdentifier = DamlLfRef.Identifier
  val DamlLfIdentifier = DamlLfRef.Identifier

  /** A simple Daml-LF type
    * Currently, these can be:
    * - Primitive types
    * - Type constructor applications (i.e., dereferencing a DamlLfIdentifier)
    * - Type variables
    */
  type DamlLfType = iface.Type
  type DamlLfTypeCon = iface.TypeCon
  val DamlLfTypeCon = iface.TypeCon
  type DamlLfTypePrim = iface.TypePrim
  val DamlLfTypePrim = iface.TypePrim
  type DamlLfTypeVar = iface.TypeVar
  val DamlLfTypeVar = iface.TypeVar
  type DamlLfTypeConName = iface.TypeConName
  val DamlLfTypeConName = iface.TypeConName
  type DamlLfTypeNumeric = iface.TypeNumeric
  val DamlLfTypeNumeric = iface.TypeNumeric

  type DamlLfPrimType = iface.PrimType
  val DamlLfPrimType = iface.PrimType

  /** A user-defined Daml-LF type (closed form). Can be a record or variant. */
  type DamlLfDataType = iface.DataType.FWT
  val DamlLfDataType = iface.DataType

  /** A user-defined Daml-LF type (generic form). Can be a record or variant. */
  type DamlLfDefDataType = iface.DefDataType.FWT
  val DamlLfDefDataType = iface.DefDataType

  type DamlLfTypeLookup = DamlLfIdentifier => Option[DamlLfDefDataType]

  /** A user-defined Daml-LF record */
  type DamlLfRecord = iface.Record.FWT
  val DamlLfRecord = iface.Record

  /** A user-defined Daml-LF variant */
  type DamlLfVariant = iface.Variant.FWT
  val DamlLfVariant = iface.Variant

  /** A user-defined Daml-LF enum */
  type DamlLfEnum = iface.Enum
  val DamlLfEnum = iface.Enum

  type ApiValue = V
  type ApiRecordField = (Option[DamlLfRef.Name], ApiValue)
  type ApiRecord = V.ValueRecord
  type ApiVariant = V.ValueVariant
  type ApiList = V.ValueList
  type ApiOptional = V.ValueOptional
  type ApiMap = V.ValueTextMap
  type ApiGenMap = V.ValueGenMap
}

object NavigatorModelAliases extends NavigatorModelAliases[String]
