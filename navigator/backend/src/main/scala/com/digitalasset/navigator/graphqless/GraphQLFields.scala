// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.graphqless

import sangria.schema.{Context, Field}
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}
import shapeless.labelled.FieldType
import shapeless.ops.record.Selector

trait GraphQLFields[T, Repr] {
  def fields[Ctx]: List[Field[Ctx, T]]
}

object GraphQLFields {
  def apply[T, Repr](implicit graphQLFields: GraphQLFields[T, Repr]): GraphQLFields[T, Repr] =
    graphQLFields
}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
trait DerivedGraphQLFields {

  def mkField[Ctx, C, Repr <: HList, K <: Symbol, V](implicit
      kw: Witness.Aux[K],
      vField: Lazy[GraphQLOutputType[V]],
      gen: LabelledGeneric.Aux[C, Repr],
      selector: Selector.Aux[Repr, K, V],
  ): Field[Ctx, C] =
    Field(
      name = kw.value.name,
      fieldType = vField.value.outputType[Ctx],
      resolve = (context: Context[Ctx, C]) => selector(gen.to(context.value)),
    )

  implicit def singleFieldGraphQLField[C, Repr <: HList, K <: Symbol, V](implicit
      kw: Witness.Aux[K],
      vField: Lazy[GraphQLOutputType[V]],
      gen: LabelledGeneric.Aux[C, Repr],
      selector: Selector.Aux[Repr, K, V],
  ): GraphQLFields[C, FieldType[K, V] :: HNil] = new GraphQLFields[C, FieldType[K, V] :: HNil] {

    override def fields[Ctx]: List[Field[Ctx, C]] =
      List(mkField[Ctx, C, Repr, K, V])
  }

  implicit def productGraphQLField[C, Repr <: HList, K <: Symbol, V, T <: HList](implicit
      kw: Witness.Aux[K],
      vField: Lazy[GraphQLOutputType[V]],
      tGraphQLField: Lazy[GraphQLFields[C, T]],
      gen: LabelledGeneric.Aux[C, Repr],
      selector: Selector.Aux[Repr, K, V],
  ): GraphQLFields[C, FieldType[K, V] :: T] = new GraphQLFields[C, FieldType[K, V] :: T] {
    override def fields[Ctx]: List[Field[Ctx, C]] = {
      val head = mkField[Ctx, C, Repr, K, V]
      val tail = tGraphQLField.value.fields[Ctx]
      head +: tail
    }
  }
}

object DerivedGraphQLFields extends DerivedGraphQLFields
