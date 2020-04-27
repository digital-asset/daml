// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package value.json

import data.{Ref => DamlLfRef}
import value.{Value => V}

/** Aliases used by navigator's Scala backend, from which this package
  * was derived.
  */
trait NavigatorModelAliases[Cid] {

  /**
    * An absolute reference of a DAML-LF entity.
    * Contains a DAML-LF package ID and a qualified name.
    * Currently, such identifiers can point to:
    * - Templates
    * - User-defined records
    * - User-defined variants
    */
  type DamlLfIdentifier = DamlLfRef.Identifier
  val DamlLfIdentifier = DamlLfRef.Identifier

  /**
    * A simple DAML-LF type
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

  /** A user-defined DAML-LF type (closed form). Can be a record or variant. */
  type DamlLfDataType = iface.DataType.FWT
  val DamlLfDataType = iface.DataType

  /** A user-defined DAML-LF type (generic form). Can be a record or variant. */
  type DamlLfDefDataType = iface.DefDataType.FWT
  val DamlLfDefDataType = iface.DefDataType

  type DamlLfTypeLookup = DamlLfIdentifier => Option[DamlLfDefDataType]

  /** A user-defined DAML-LF record */
  type DamlLfRecord = iface.Record.FWT
  val DamlLfRecord = iface.Record

  /** A user-defined DAML-LF variant */
  type DamlLfVariant = iface.Variant.FWT
  val DamlLfVariant = iface.Variant

  /** A user-defined DAML-LF enum */
  type DamlLfEnum = iface.Enum
  val DamlLfEnum = iface.Enum

  def damlLfInstantiate(typeCon: DamlLfTypeCon, defn: DamlLfDefDataType): DamlLfDataType =
    if (defn.typeVars.length != typeCon.typArgs.length) {
      throw new RuntimeException(
        s"Mismatching type vars and applied types, expected ${defn.typeVars} but got ${typeCon.typArgs} types")
    } else {
      if (defn.typeVars.isEmpty) { // optimization
        defn.dataType
      } else {
        val paramsMap = Map(defn.typeVars.zip(typeCon.typArgs): _*)
        def mapTypeVars(typ: DamlLfType, f: DamlLfTypeVar => DamlLfType): DamlLfType = typ match {
          case t @ DamlLfTypeVar(_) => f(t)
          case t @ DamlLfTypeCon(_, _) => DamlLfTypeCon(t.name, t.typArgs.map(mapTypeVars(_, f)))
          case t @ DamlLfTypePrim(_, _) => DamlLfTypePrim(t.typ, t.typArgs.map(mapTypeVars(_, f)))
          case t @ DamlLfTypeNumeric(_) => t
        }
        val withTyp: iface.Type => iface.Type = { typ =>
          mapTypeVars(typ, v => paramsMap.getOrElse(v.name, v))
        }
        defn.dataType.bimap(withTyp, withTyp)
      }
    }

  import scala.language.higherKinds
  type OfCid[V[_]] = V[Cid]
  type ApiValue = OfCid[V]
  type ApiRecordField = (Option[DamlLfRef.Name], ApiValue)
  type ApiRecord = OfCid[V.ValueRecord]
  type ApiVariant = OfCid[V.ValueVariant]
  type ApiList = OfCid[V.ValueList]
  type ApiOptional = OfCid[V.ValueOptional]
  type ApiMap = OfCid[V.ValueTextMap]
  type ApiGenMap = OfCid[V.ValueGenMap]
  type ApiImpossible = OfCid[V.ValueStruct]
}

object NavigatorModelAliases extends NavigatorModelAliases[String]
