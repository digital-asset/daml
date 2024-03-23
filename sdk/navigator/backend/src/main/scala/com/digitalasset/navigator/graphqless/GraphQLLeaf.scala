// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  implicit val bigDecimalGraphQLLeaf = GraphQLLeaf.instance[BigDecimal](BigDecimalType)
  implicit val bigIntTypeGraphQLLeaf = GraphQLLeaf.instance[BigInt](BigIntType)
  implicit val booleanToGraphQLLeaf = GraphQLLeaf.instance[Boolean](BooleanType)
  implicit val doubleToGraphQLLeaf = GraphQLLeaf.instance[Double](FloatType)
  implicit val intToGraphQLLeaf = GraphQLLeaf.instance[Int](IntType)
  implicit val longToGraphQLLeaf = GraphQLLeaf.instance[Long](LongType)
  implicit val stringToGraphQLLeaf = GraphQLLeaf.instance[String](StringType)
}

object PrimitiveGraphQLLeaf extends PrimitiveGraphQLLeaf
