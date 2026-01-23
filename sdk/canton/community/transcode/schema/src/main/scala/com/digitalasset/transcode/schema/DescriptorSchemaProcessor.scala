// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.schema

import scala.annotation.static
import scala.collection.mutable
import scala.util.Try

object DescriptorSchemaProcessor {
  @static def assertProcess[R](
      dictionary: Dictionary[? <: HasDescriptor],
      visitor: SchemaVisitor.WithResult[R],
      filter: IdentifierFilter = IdentifierFilter.AcceptAll,
  ): R =
    new DescriptorSchemaProcessor(
      dictionary.map(_.getDescriptor).filterIdentifiers(filter),
      visitor,
    ).getResult

  @static def process(
      dictionary: Dictionary[? <: HasDescriptor],
      visitor: SchemaVisitor,
      filter: IdentifierFilter = IdentifierFilter.AcceptAll,
  ): Either[String, visitor.Result] = Try {
    new DescriptorSchemaProcessor(
      dictionary.map(_.getDescriptor).filterIdentifiers(filter),
      visitor,
    ).getResult
  }.toEither.left.map(_.getMessage)
}

private class DescriptorSchemaProcessor[R](
    dictionary: Dictionary[Descriptor],
    visitor: SchemaVisitor { type Result = R },
) {
  def getResult: R = visitor.collect(Lazy.withForcedValues(dictionary.map(handle).entities))

  private val Lazy = LazinessManager()
  private val Cache = mutable.Map.empty[Identifier, visitor.Type]
  private def handle(descriptor: Descriptor): visitor.Type =
    descriptor match
      case Descriptor.Record(fields) => visitor.record(fields.map((n, v) => n -> handle(v)))
      case Descriptor.Variant(cases) => visitor.variant(cases.map((n, v) => n -> handle(v)))
      case Descriptor.Enumeration(cases) => visitor.enumeration(cases)
      case Descriptor.List(value) => visitor.list(handle(value))
      case Descriptor.Optional(value) => visitor.optional(handle(value))
      case Descriptor.TextMap(value) => visitor.textMap(handle(value))
      case Descriptor.GenMap(key, value) => visitor.genMap(handle(key), handle(value))
      case Descriptor.Unit => visitor.unit
      case Descriptor.Bool => visitor.bool
      case Descriptor.Text => visitor.text
      case Descriptor.Int64 => visitor.int64
      case Descriptor.Numeric(scale) => visitor.numeric(scale)
      case Descriptor.Timestamp => visitor.timestamp
      case Descriptor.Date => visitor.date
      case Descriptor.Party => visitor.party
      case Descriptor.ContractId(value) => visitor.contractId(handle(value))
      case Descriptor.Variable(name) => visitor.variable(name)
      case Descriptor.Constructor(id, typeParams, body) =>
        Cache.getOrElseUpdate(
          id, {
            val lazyBody = Lazy(handle(body))
            visitor.constructor(id, typeParams, lazyBody())
          },
        )
      case Descriptor.Application(ctor @ Descriptor.Constructor(id, typeParams, body), args) =>
        visitor.application(handle(ctor), typeParams, args.map(handle))

}
