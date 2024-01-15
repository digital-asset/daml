// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.graphqless

import sangria.schema.OutputType

trait GraphQLOutputType[T] {
  def outputType[Ctx]: OutputType[T]
}

object GraphQLOutputType {
  def apply[T](implicit graphQLOutputType: GraphQLOutputType[T]): GraphQLOutputType[T] =
    graphQLOutputType
}

trait DerivedGraphQLOutputType {

  implicit def graphQLLeafToGraphQLOutputType[T](implicit
      graphQLLeaf: GraphQLLeaf[T]
  ): GraphQLOutputType[T] = new GraphQLOutputType[T] {
    override def outputType[Ctx]: OutputType[T] =
      graphQLLeaf.to
  }

  implicit def graphQLObjectToGraphQLOutputType[T](implicit
      graphQLObject: GraphQLObject[T]
  ): GraphQLOutputType[T] = new GraphQLOutputType[T] {
    override def outputType[Ctx]: OutputType[T] =
      graphQLObject.to[Ctx]
  }
}

object DerivedGraphQLOutputType extends DerivedGraphQLOutputType
