// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode

import com.digitalasset.transcode.schema.*

package object conformance:
  val RoundtripId: Identifier = Identifier.fromString("examples:Conformance:Roundtrip")

  trait TestCase:
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private var _cases: Vector[(String, Descriptor, DynamicValue)] = Vector.empty
    final def addCase(descriptor: Descriptor, values: DynamicValue*)(using sourcecode.Name): Unit =
      _cases = _cases ++ values.map(v => (genTestName(descriptor), descriptor, v))
    final lazy val cases: Seq[(String, Descriptor, DynamicValue)] = _cases.toList

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private var _failing: Vector[(String, String, Descriptor, DynamicValue)] = Vector.empty
    final def addFailingCase(descriptor: Descriptor, cases: (String, DynamicValue)*)(using
        sourcecode.Name
    ): Unit =
      _failing = _failing ++ cases.map((msg, v) => (genTestName(descriptor), msg, descriptor, v))
    final lazy val failing: Seq[(String, String, Descriptor, DynamicValue)] = _failing.toList
  end TestCase

  inline def ctor(descriptor: => Descriptor.Adt): Descriptor.Constructor =
    mkCtor(Seq.empty, descriptor)

  inline def ctor(inline descriptor: Descriptor => Descriptor.Adt): Descriptor.Constructor =
    val typeParams = lambdaArgNames(descriptor).map(TypeVarName)
    val Seq(a) = typeParams.map(Descriptor.variable)
    mkCtor(typeParams, descriptor(a))

  inline def ctor(
      inline descriptor: (Descriptor, Descriptor) => Descriptor.Adt
  ): Descriptor.Constructor =
    val typeParams = lambdaArgNames(descriptor).map(TypeVarName)
    val Seq(a, b) = typeParams.map(Descriptor.variable)
    mkCtor(typeParams, descriptor(a, b))

  inline def ctor(
      inline descriptor: (Descriptor, Descriptor, Descriptor) => Descriptor.Adt
  ): Descriptor.Constructor =
    val typeParams = lambdaArgNames(descriptor).map(TypeVarName)
    val Seq(a, b, c) = typeParams.map(Descriptor.variable)
    mkCtor(typeParams, descriptor(a, b, c))

  private inline def mkCtor(typeParams: Seq[String], descriptor: => Descriptor.Adt) =
    Descriptor.constructor(identifier, typeParams, descriptor)

  inline def identifier: Identifier =
    Identifier(
      PackageId("0"),
      PackageName("examples"),
      PackageVersion.Unknown,
      ModuleName(sourcecode.Enclosing().split('#').head.split('.').map(_.capitalize).mkString(".")),
      EntityName(sourcecode.Enclosing().split('#').last.replace(' ', '.')),
    )

  private inline def lambdaArgNames(inline x: Any): Seq[String] = ${ Macros.lambdaArgNames('x) }

  private val nameCache = scala.collection.mutable.Map.empty[String, Int]
  private def genTestName(descriptor: Descriptor)(using name: sourcecode.Name): String = {
    val nameBase = s"${name.value}_${descriptorPart(descriptor)}"
    val ix = nameCache.updateWith(nameBase)(x => Some(x.getOrElse(0) + 1)).getOrElse(1)
    s"${nameBase}_$ix"
  }

  private def descriptorPart(d: Descriptor): String = d match {
    case Descriptor.Constructor(id, _, _) => id.entityName
    case Descriptor.Application(Descriptor.Constructor(id, _, _), args) =>
      s"${id.entityName}_${args.map(descriptorPart).mkString("_")}"

    case Descriptor.Unit => "Unit"
    case Descriptor.Bool => "Bool"
    case Descriptor.Text => "Text"
    case Descriptor.Int64 => "Int64"
    case Descriptor.Numeric(scale) => s"Numeric$scale"
    case Descriptor.Timestamp => "Timestamp"
    case Descriptor.Date => "Date"
    case Descriptor.Party => "Party"
    case Descriptor.ContractId(value) => s"ContractId_${descriptorPart(value)}"
    case Descriptor.Variable(name) => name

    case Descriptor.List(value) => s"List_${descriptorPart(value)}"
    case Descriptor.Optional(value) => s"Optional_${descriptorPart(value)}"
    case Descriptor.TextMap(value) => s"TextMap_${descriptorPart(value)}"
    case Descriptor.GenMap(key, value) => s"GenMap_${descriptorPart(key)}_${descriptorPart(value)}"

    case _ => "__"
  }
