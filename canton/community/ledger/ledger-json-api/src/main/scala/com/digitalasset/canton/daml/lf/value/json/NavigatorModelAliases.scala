// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.daml.lf.value.json

import com.daml.lf.data.Ref as DamlLfRef
import com.daml.lf.typesig
import com.daml.lf.value.Value as V

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
  type DamlLfType = typesig.Type
  type DamlLfTypeCon = typesig.TypeCon
  val DamlLfTypeCon = typesig.TypeCon
  type DamlLfTypePrim = typesig.TypePrim
  val DamlLfTypePrim = typesig.TypePrim
  type DamlLfTypeVar = typesig.TypeVar
  val DamlLfTypeVar = typesig.TypeVar
  type DamlLfTypeConName = typesig.TypeConName
  val DamlLfTypeConName = typesig.TypeConName
  type DamlLfTypeNumeric = typesig.TypeNumeric
  val DamlLfTypeNumeric = typesig.TypeNumeric

  type DamlLfPrimType = typesig.PrimType
  val DamlLfPrimType = typesig.PrimType

  /** A user-defined Daml-LF type (closed form). Can be a record or variant. */
  type DamlLfDataType = typesig.DataType.FWT
  val DamlLfDataType = typesig.DataType

  /** A user-defined Daml-LF type (generic form). Can be a record or variant. */
  type DamlLfDefDataType = typesig.DefDataType.FWT
  val DamlLfDefDataType = typesig.DefDataType

  type DamlLfTypeLookup = DamlLfIdentifier => Option[DamlLfDefDataType]

  /** A user-defined Daml-LF record */
  type DamlLfRecord = typesig.Record.FWT
  val DamlLfRecord = typesig.Record

  /** A user-defined Daml-LF variant */
  type DamlLfVariant = typesig.Variant.FWT
  val DamlLfVariant = typesig.Variant

  /** A user-defined Daml-LF enum */
  type DamlLfEnum = typesig.Enum
  val DamlLfEnum = typesig.Enum

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
