// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.schema

import boopickle.Default.*
import boopickle.{CompositePickler, PicklerHelper}
import com.digitalasset.transcode.schema.Descriptor.*

import scala.collection.immutable.List as SList

object Pickler extends PicklerHelper:
  given descriptor: CompositePickler[Descriptor] = compositePickler[Descriptor]
  private val _ = descriptor
    .addConcreteType[Record]
    .addConcreteType[Variant]
    .addConcreteType[Enumeration]
    .addConcreteType[List]
    .addConcreteType[Optional]
    .addConcreteType[TextMap]
    .addConcreteType[GenMap]
    .addConcreteType[Unit.type]
    .addConcreteType[Bool.type]
    .addConcreteType[Text.type]
    .addConcreteType[Int64.type]
    .addConcreteType[Numeric]
    .addConcreteType[Timestamp.type]
    .addConcreteType[Date.type]
    .addConcreteType[Party.type]
    .addConcreteType[ContractId]
    .addConcreteType[Constructor]
    .addConcreteType[Application]
    .addConcreteType[Variable]

  // Schema's specialized types
  given Pickler[Choice[Descriptor]] = generatePickler[Choice[Descriptor]]
  given Pickler[Template[Descriptor]] = generatePickler[Template[Descriptor]]

  // Opaque types
  given Pickler[ChoiceName] = transformPickler[ChoiceName, String](ChoiceName)(choiceName)
  given Pickler[PackageId] = transformPickler[PackageId, String](PackageId)(packageId)
  given Pickler[PackageName] = transformPickler[PackageName, String](PackageName)(packageName)
  given Pickler[ModuleName] = transformPickler[ModuleName, String](ModuleName)(moduleName)
  given Pickler[PackageVersion] =
    transformPickler[PackageVersion, String](PackageVersion)(packageVersion)
  given Pickler[EntityName] = transformPickler[EntityName, String](EntityName)(entityName)
  given Pickler[TypeVarName] = transformPickler[TypeVarName, String](TypeVarName)(typeVarName)
  given Pickler[FieldName] = transformPickler[FieldName, String](FieldName)(fieldName)
  given Pickler[VariantConName] =
    transformPickler[VariantConName, String](VariantConName)(variantConName)
  given Pickler[EnumConName] = transformPickler[EnumConName, String](EnumConName)(EnumConName)

  // Deal with Cycles in graph
  implicit object ConstructorPickler extends P[Constructor] {
    def pickle(obj: Constructor)(implicit state: PickleState): Unit =
      val Constructor(id, params, body) = obj
      state.immutableRefFor(id) match
        case Some(idx) =>
          // do nothing
          state.enc.writeInt(idx): Unit
        case None =>
          state.enc.writeInt(0): Unit
          summon[Pickler[Identifier]].pickle(id)
          summon[Pickler[SList[TypeVarName]]].pickle(params)
          state.addImmutableRef(id)
          summon[Pickler[Adt]].pickle(body)

    def unpickle(implicit state: UnpickleState): Constructor =
      state.dec.readInt match
        case 0 =>
          val id = summon[Pickler[Identifier]].unpickle
          val params = summon[Pickler[SList[TypeVarName]]].unpickle
          @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
          var body: Adt = null
          val ctor = constructor(id, params, body)
          state.addImmutableRef(ctor)
          body = summon[Pickler[Adt]].unpickle
          ctor
        case idx =>
          state.immutableFor[Constructor](idx)
  }
