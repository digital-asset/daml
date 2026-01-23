// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.utils.propertygenerators

import com.digitalasset.transcode.schema
import com.digitalasset.transcode.schema.*
import zio.test.Gen

import java.time.Instant
import scala.collection.mutable

object SchemaGenerator:
  type Schema = Dictionary[Descriptor]

  def generateSchema: Gen[Any, Schema] = Gen.listOfBounded(1, 5)(entityG).map(Dictionary(_))

  def dynamicEntityGen(schema: Schema): Gen[Any, Dictionary[DynamicValue]] =
    def go(tpe: Descriptor)(using varMap: Map[TypeVarName, DynamicValue]): Gen[Any, DynamicValue] =
      Gen.suspend(tpe match
        case Descriptor.List(elem) =>
          Gen.listOfBounded(0, 10)(go(elem)).map(DynamicValue.List)
        case Descriptor.Optional(elem) =>
          Gen.option(go(elem)).map(DynamicValue.Optional)
        case Descriptor.TextMap(value) =>
          Gen
            .listOfBounded(0, 10)(Gen.string zip go(value))
            .map(ll => DynamicValue.TextMap(ll.toMap.toSeq))
        case Descriptor.GenMap(key, value) =>
          Gen
            .listOfBounded(0, 10)(go(key) zip go(value))
            .map(ll => DynamicValue.GenMap(ll.toMap.toSeq))
        case Descriptor.Unit =>
          Gen.const(DynamicValue.Unit)
        case Descriptor.Bool =>
          Gen.boolean.map(DynamicValue.Bool)
        case Descriptor.Text =>
          Gen.string.map(DynamicValue.Text)
        case Descriptor.Int64 =>
          Gen.long.map(DynamicValue.Int64)
        case Descriptor.Numeric(scale) => // todo scale
          Gen.listOfBounded(1, 9)(Gen.numericChar).map(_.mkString).map(DynamicValue.Numeric)
        case Descriptor.Timestamp =>
          Gen
            .instant(Instant.EPOCH, Instant.parse("9999-12-31T23:59:59Z"))
            .map(v => DynamicValue.Timestamp(v.getEpochSecond * 1000000 + v.getNano / 1000))
        case Descriptor.Date =>
          Gen.localDate.map(d => DynamicValue.Date(d.toEpochDay.toInt))
        case Descriptor.Party =>
          Gen.alphaNumericString.map(DynamicValue.Party)
        case Descriptor.ContractId(template) =>
          Gen.alphaNumericString.map(DynamicValue.ContractId)
        case Descriptor.Variable(name) =>
          Gen.const(varMap(name))
        case Descriptor.Constructor.Applied(id, vm, body) =>
          for
            argValues <- Gen.collectAll(vm.map(_._2).map(go))
            newVarMap = (vm.map(_._1) zip argValues).toMap
            result <- body match
              case Descriptor.Record(fields) =>
                Gen
                  .collectAll(fields.map((_, v) => go(v)(using newVarMap)))
                  .map(values => DynamicValue.Record(values))
              case Descriptor.Variant(cases) =>
                Gen
                  .oneOf(cases.zipWithIndex.map { case ((k, v), ix) =>
                    Gen.const(ix) zip go(v)(using newVarMap)
                  }*)
                  .map((ix, v) => DynamicValue.Variant(ix, v))
              case Descriptor.Enumeration(cases) =>
                Gen.int(0, cases.length - 1).map(DynamicValue.Enumeration)
          yield result
      )

    // mapping inside Dictionary happens deterministically, so we can traverse over schema by extracting generators,
    // sequencing the results and then traversing dictionary again with values in the same order
    val generators = mutable.Buffer.empty[Gen[Any, DynamicValue]]
    val _ = schema.map(descriptor => go(descriptor)(using Map.empty)).map(f => generators.addOne(f))
    Gen.collectAll(generators).map(_.iterator).map(iterator => schema.map(_ => iterator.next()))
  end dynamicEntityGen

  private def entityG: Gen[Any, Template[Descriptor]] =
    (identifierG zip templatePayloadG zip templateKeyG zip isInterfaceG zip templateImplementsG zip templateChoicesG)
      .map(Template.apply)
  private def choiceG: Gen[Any, Choice[Descriptor]] =
    (choiceNameG zip consumingG zip choiceArgG zip choiceResultG).map(Choice.apply)

  private def templatePayloadG = conWithoutAppG(recordG(using 5))
  private def templateKeyG = Gen.option(typeG(using 2)(using Seq.empty))
  private def templateImplementsG = Gen.listOfBounded(0, 3)(identifierG)
  private def templateChoicesG = Gen.listOfBounded(0, 3)(choiceG)
  private def isInterfaceG = Gen.boolean
  private def choiceArgG = typeG(using 2)(using Seq.empty)
  private def choiceResultG = typeG(using 2)(using Seq.empty)
  private def consumingG = Gen.boolean

  private type MaxDepth = Int
  private type Vars = Seq[TypeVarName]
  private def typeG(using maxDepth: MaxDepth)(using vars: Vars): Gen[Any, Descriptor] = Gen.suspend(
    Gen.oneOf(
      Seq(
        Seq(
          conWithoutAppG(enumG),
          unitG,
          boolG,
          textG,
          int64G,
          numericG,
          timestampG,
          dateG,
          partyG,
          contractIdG,
        ),
        Seq(
          variableG
        ).filter(_ => vars.nonEmpty),
        Seq(
          conAppG(recordG)(using maxDepth - 1),
          conAppG(variantG)(using maxDepth - 1),
          listG(using maxDepth - 1),
          optionalG(using maxDepth - 1),
          textMapG(using maxDepth - 1),
          genMapG(using maxDepth - 1),
          recursiveRecordG(using maxDepth - 1),
          recursiveVariantG(using maxDepth - 1),
        ).filter(_ => maxDepth > 0),
      ).flatten*
    )
  )

  private def conAppG(genBody: Vars ?=> Gen[Any, Descriptor.Adt])(using MaxDepth)(using Vars) =
    Gen.weighted(
      conWithoutAppG(genBody) -> 3,
      conWithAppG(genBody) -> 1,
    )
  private def conWithAppG(genBody: Vars ?=> Gen[Any, Descriptor.Adt])(using MaxDepth)(using Vars) =
    for
      id <- identifierG
      vars <- Gen.listOfBounded(1, 5)(variableNameG)
      values <- Gen.collectAll(vars.map(_ => typeG))
      body <- genBody(using vars)
    yield Descriptor.application(Descriptor.constructor(id, vars, body), values)
  private def conWithoutAppG(genBody: Vars ?=> Gen[Any, Descriptor.Adt]) = for
    id <- identifierG
    body <- genBody(using Seq.empty)
  yield Descriptor.constructor(id, body)

  private def recordG(using MaxDepth)(using Vars) =
    for fields <- Gen.listOfBounded(1, 3)(lowerCaseName zip typeG)
    yield Descriptor.record(fields.toMap.toSeq)

  private def recursiveRecordG(using MaxDepth)(using Vars) =
    for
      id <- identifierG
      vars <- Gen.listOfBounded(0, 2)(variableNameG)
      values <- Gen.listOfN(vars.size)(typeG)
      conApp = {
        lazy val record: Descriptor.Record =
          if vars.nonEmpty
          then
            Descriptor.record(
              "self" -> Descriptor.optional(
                Descriptor.application(
                  Descriptor.constructor(id, vars, record),
                  vars.map(Descriptor.variable),
                )
              ),
              vars.map(name => s"var$name" -> Descriptor.variable(name))*
            )
          else Descriptor.record("self" -> Descriptor.optional(descriptor))
        lazy val descriptor: Descriptor =
          if vars.nonEmpty
          then Descriptor.application(Descriptor.constructor(id, vars, record), values)
          else Descriptor.constructor(id, record)
        descriptor
      }
    yield conApp

  private def recursiveVariantG(using MaxDepth)(using Vars) =
    for
      id <- identifierG
      vars <- Gen.listOfBounded(0, 2)(variableNameG)
      values <- Gen.listOfN(vars.size)(typeG)
      conApp = {
        lazy val variant: Descriptor.Variant =
          if vars.nonEmpty
          then
            Descriptor.variant(
              Seq(
                "Stop" ->
                  Descriptor.unit,
                "Self" ->
                  Descriptor.application(
                    Descriptor.constructor(id, vars, variant),
                    vars.map(Descriptor.variable),
                  ),
              ) ++ vars.map(name =>
                s"SelfWith$name" -> Descriptor.application(
                  Descriptor.constructor(
                    id.withSuffixEntityName(s"SelfWith$name"),
                    vars,
                    Descriptor.record(
                      s"var$name" ->
                        Descriptor.variable(name),
                      "self" ->
                        Descriptor.application(
                          Descriptor.constructor(id, vars, variant),
                          vars.map(Descriptor.variable),
                        ),
                    ),
                  ),
                  vars.map(Descriptor.variable),
                )
              )
            )
          else
            Descriptor.variant(
              "Stop" -> Descriptor.unit,
              "Self" -> Descriptor.constructor(id, variant),
            )
        lazy val descriptor: Descriptor =
          if vars.nonEmpty
          then Descriptor.application(Descriptor.constructor(id, vars, variant), values)
          else Descriptor.constructor(id, variant)
        descriptor
      }
    yield conApp

  private def variantG(using MaxDepth)(using Vars) =
    for cases <- Gen.listOfBounded(1, 3)(upperCaseName zip typeG)
    yield Descriptor.variant(cases.toMap.toSeq)

  private def enumG =
    for cases <- Gen.listOfBounded(1, 3)(upperCaseName)
    yield Descriptor.enumeration(cases.toSet.toSeq)

  private def optionalG(using MaxDepth)(using Vars) = typeG.map(Descriptor.optional)
  private def listG(using MaxDepth)(using Vars) = typeG.map(Descriptor.list)
  private def textMapG(using MaxDepth)(using Vars) = typeG.map(Descriptor.textMap)
  private def genMapG(using MaxDepth)(using Vars) = (typeG zip typeG).map(Descriptor.genMap)

  private def unitG = Gen.const(Descriptor.unit)
  private def boolG = Gen.const(Descriptor.bool)
  private def textG = Gen.const(Descriptor.text)
  private def int64G = Gen.const(Descriptor.int64)
  private def numericG = Gen.int(1, 10).map(Descriptor.numeric)
  private def timestampG = Gen.const(Descriptor.timestamp)
  private def dateG = Gen.const(Descriptor.date)
  private def partyG = Gen.const(Descriptor.party)
  private def contractIdG = unitG.map(Descriptor.contractId)
  private def variableG(using vars: Seq[TypeVarName]) =
    Gen.oneOf(vars.map(Gen.const)*).map(Descriptor.variable)

  private def packageIdG =
    Gen.long.map(_.toHexString).map(PackageId)
  private def packageNameG =
    Gen.stringBounded(2, 10)(Gen.alphaChar).map(PackageName)
  private def packageVersionG =
    (Gen.int(0, 100) zip Gen.int(0, 100) zip Gen.int(0, 100)).map((a, b, c) =>
      PackageVersion(s"$a.$b.$c")
    )
  private def moduleNameG =
    dottedNameG.map(ModuleName)
  private def entityNameG =
    upperCaseName.map(EntityName)
  private def lowerCaseName =
    (Gen.char('a', 'z') zip Gen.alphaNumericStringBounded(0, 9)).map((l, s) => s"$l$s")
  private def upperCaseName =
    (Gen.char('A', 'Z') zip Gen.alphaNumericStringBounded(0, 9)).map((l, s) => s"$l$s")
  private def dottedNameG =
    Gen.listOfBounded(1, 5)(upperCaseName).map(_.mkString("."))
  private def identifierG: Gen[Any, Identifier] =
    (packageIdG zip packageNameG zip packageVersionG zip moduleNameG zip entityNameG)
      .map(Identifier.apply)
  private def choiceNameG =
    upperCaseName.map(ChoiceName)
  private def variableNameG =
    lowerCaseName.map(TypeVarName)

end SchemaGenerator
