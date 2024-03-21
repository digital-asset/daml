// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.graphqless

import sangria.schema._

trait GraphQLLeaf[T] {
  def to: OutputType[T] with LeafType with Named
}

object GraphQLLeaf {
  def apply[T](implicit toGraphQLLeaf: GraphQLLeaf[T]): GraphQLLeaf[T] = toGraphQLLeaf
  def instance[T](toImpl: OutputType[T] with LeafType with Named): GraphQLLeaf[T] =
    new GraphQLLeaf[T] {
      override def to: OutputType[T] with LeafType with Named = toImpl
    }
}

trait PrimitiveGraphQLLeaf {
  implicit val bigDecimalGraphQLLeaf: GraphQLLeaf[BigDecimal] =
    GraphQLLeaf.instance[BigDecimal](BigDecimalType)
  implicit val bigIntTypeGraphQLLeaf: GraphQLLeaf[BigInt] = GraphQLLeaf.instance[BigInt](BigIntType)
  implicit val booleanToGraphQLLeaf: GraphQLLeaf[Boolean] =
    GraphQLLeaf.instance[Boolean](BooleanType)
  implicit val doubleToGraphQLLeaf: GraphQLLeaf[Double] = GraphQLLeaf.instance[Double](FloatType)
  implicit val intToGraphQLLeaf: GraphQLLeaf[Int] = GraphQLLeaf.instance[Int](IntType)
  implicit val longToGraphQLLeaf: GraphQLLeaf[Long] = GraphQLLeaf.instance[Long](LongType)
  implicit val stringToGraphQLLeaf: GraphQLLeaf[String] = GraphQLLeaf.instance[String](StringType)
}

object PrimitiveGraphQLLeaf extends PrimitiveGraphQLLeaf
