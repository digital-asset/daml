// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.graphqless

import sangria.schema._
import shapeless._

import scala.annotation.nowarn
import scala.reflect.ClassTag

trait GraphQLObject[T] {
  def to[Ctx]: ObjectType[Ctx, T]
}

object GraphQLObject {
  def apply[T](implicit graphQLObject: GraphQLObject[T]): GraphQLObject[T] = graphQLObject
}

trait DerivedGraphQLObject {

  @nowarn("msg=parameter generic .*is never used") // used to calculate Repr0 tparam only
  implicit def caseClassGraphQLObject[C, Repr0 <: HList](implicit
      classTag: ClassTag[C],
      generic: LabelledGeneric.Aux[C, Repr0],
      graphQLFields: GraphQLFields[C, Repr0],
  ): GraphQLObject[C] = new GraphQLObject[C] {
    override def to[Ctx]: ObjectType[Ctx, C] = {
      val objectName = classTag.runtimeClass.getSimpleName
      val reprFields = graphQLFields.fields[Ctx]
      ObjectType(
        name = objectName,
        fields = reprFields,
      )
    }
  }
}

object DerivedGraphQLObject extends DerivedGraphQLObject
