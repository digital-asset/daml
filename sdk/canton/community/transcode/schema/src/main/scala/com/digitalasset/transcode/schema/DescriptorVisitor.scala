// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.schema

object DescriptorVisitor extends SchemaVisitor {
  type Type = Descriptor
  type Result = Dictionary[Descriptor]

  def collect(entities: Seq[Template[Descriptor]]): Dictionary[Descriptor] = Dictionary(entities)

  def record(fields: Seq[(FieldName, Descriptor)]): Descriptor = Descriptor.record(fields)
  def variant(cases: Seq[(VariantConName, Descriptor)]): Descriptor = Descriptor.variant(cases)
  def enumeration(cases: Seq[EnumConName]): Descriptor = Descriptor.enumeration(cases)

  def list(elem: Descriptor): Descriptor = Descriptor.list(elem)
  def optional(elem: Descriptor): Descriptor = Descriptor.optional(elem)
  def textMap(value: Descriptor): Descriptor = Descriptor.textMap(value)
  def genMap(key: Descriptor, value: Descriptor): Descriptor = Descriptor.genMap(key, value)

  val unit: Descriptor = Descriptor.unit
  val bool: Descriptor = Descriptor.bool
  val text: Descriptor = Descriptor.text
  val int64: Descriptor = Descriptor.int64
  def numeric(scale: Int): Descriptor = Descriptor.numeric(scale)
  val timestamp: Descriptor = Descriptor.timestamp
  val date: Descriptor = Descriptor.date
  val party: Descriptor = Descriptor.party
  def contractId(template: Descriptor): Descriptor = Descriptor.contractId(template)

  def variable(name: TypeVarName): Descriptor =
    Descriptor.variable(name)
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def constructor(id: Identifier, typeParams: Seq[TypeVarName], body: => Descriptor): Descriptor =
    Descriptor.constructor(id, typeParams, body.asInstanceOf[Descriptor.Adt])
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def application(
      ctor: Descriptor,
      typeParams: Seq[TypeVarName],
      args: Seq[Descriptor],
  ): Descriptor =
    Descriptor.application(ctor.asInstanceOf[Descriptor.Constructor], args)
}
