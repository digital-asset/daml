// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.daml_lf

import com.digitalasset.transcode.DamlExamples
import com.digitalasset.transcode.daml_lf.Util
import com.digitalasset.transcode.daml_lf.synonyms.DarDecoder
import com.digitalasset.transcode.schema.*
import zio.test.*
import zio.test.diff.Diff.DiffOps
import zio.test.diff.{Diff, DiffResult}

import scala.collection.mutable
import scala.language.implicitConversions

trait SchemaProcessorSpecDefault extends ZIOSpecDefault:
  private val dar = DarDecoder.assertReadArchiveFromFile(DamlExamples.darPath.toFile)
  private val packages =
    dar.all.map((pkgId, pkg) => pkgId -> Util.toSignature(pkg)).toMap
  private val dictionary =
    LfSchemaProcessor
      .process(packages, IdentifierFilter.AcceptAll)(DescriptorVisitor)
      .fold(m => throw Exception(m), identity)
      .matchByPackageName
  DescriptorSchemaProcessor // make sure it finishes
    .process(
      dictionary,
      new SchemaVisitor.Unit {
        type Result = Seq[Template[Unit]]
        def collect(entities: Seq[Template[Unit]]) = entities
      },
    )
    .fold(m => throw Exception(m), identity)
  private val dictionaryDerivative =
    DescriptorSchemaProcessor
      .process(dictionary, DescriptorVisitor)
      .fold(m => throw Exception(m), identity)
  private val picklerRoundtrip =
    Schema.deserialize(Schema.serialize(dictionary))

  def getSchema = dictionary

  def testSchema(
      moduleName: String,
      expectedTemplateDescriptors: Descriptor*
  ) =
    test(moduleName)(
      TestResult.allSuccesses(expectedTemplateDescriptors.map { expected =>
        val Descriptor.Constructor(id, typeParams, body) = expected: @unchecked
        val actual1 = dictionary.template(id)
        val actual2 = dictionaryDerivative.template(id)
        val actual3 = picklerRoundtrip.template(id)
        assert(actual1)(equalTo(expected)) && assert(actual2)(equalTo(expected)) && assert(actual3)(
          equalTo(expected)
        )
      })
    )

  protected implicit def stringDiff[T <: String]: Diff[T] = (x, y) => Diff.stringDiff.diff(x, y)

  protected implicit def idDiff: Diff[Identifier] = (left, right) =>
    DiffResult.Nested(
      "Identifier",
      List(
        Some("moduleName") -> (left.moduleName.moduleName diffed right.moduleName.moduleName),
        Some("entityName") -> (left.entityName.entityName diffed right.entityName.entityName),
      ),
    )

  protected implicit val descriptorDiff: Diff[Descriptor] = (xx, yy) =>
    def go(x: Descriptor, y: Descriptor)(using seen: mutable.Set[Descriptor]): DiffResult =
      (x, y) match
        case (left, right) if left == right =>
          DiffResult.Identical(left)
        case (left: Descriptor.Constructor, right: Descriptor.Constructor)
            if seen.contains(left) || seen.contains(right) =>
          DiffResult.Identical(left)
        case (left: Descriptor.Constructor, right: Descriptor.Constructor) =>
          seen.add(left)
          seen.add(right)
          val Descriptor.Constructor(leftId, leftTypeParams, leftBody) = left
          val Descriptor.Constructor(rightId, rightTypeParams, rightBody) = right
          DiffResult.Nested(
            s"Constructor(${leftId.moduleName}:${leftId.entityName})",
            List(
              Some("id") -> (leftId diffed rightId),
              Some("typeParams") -> (leftTypeParams diffed rightTypeParams),
              Some("body") -> go(leftBody, rightBody),
            ),
          )
        case (left: Descriptor.Application, right: Descriptor.Application) =>
          DiffResult.Nested(
            "Application",
            List(
              Some("ctor") -> go(left.ctor, right.ctor),
              Some("args") -> DiffResult.Nested(
                "$",
                left.args.toList
                  .map(Option.apply)
                  .zipAll(right.args.toList.map(Option.apply), None, None)
                  .collect {
                    case (Some(l), Some(r)) => go(l, r)
                    case (None, Some(r)) => DiffResult.Added(r)
                    case (Some(l), None) => DiffResult.Removed(l)
                  }
                  .map(None -> _),
              ),
            ),
          )
        case (left: Descriptor.Record, right: Descriptor.Record) =>
          val leftMap = left.fields.toMap
          val rightMap = right.fields.toMap
          DiffResult.Nested(
            "Record",
            (leftMap.keySet ++ rightMap.keySet).toList
              .map(k => (Some(k), leftMap.get(k), rightMap.get(k)))
              .collect {
                case (k, Some(l), Some(r)) => k -> go(l, r)
                case (k, None, Some(r)) => k -> DiffResult.Added(r)
                case (k, Some(l), None) => k -> DiffResult.Removed(l)
              },
          )
        case (left: Descriptor.Variant, right: Descriptor.Variant) =>
          val leftMap = left.cases.toMap
          val rightMap = right.cases.toMap
          DiffResult.Nested(
            "Variant",
            (leftMap.keySet ++ rightMap.keySet).toList
              .map(k => (Some(k), leftMap.get(k), rightMap.get(k)))
              .collect {
                case (k, Some(l), Some(r)) => k -> go(l, r)
                case (k, None, Some(r)) => k -> DiffResult.Added(r)
                case (k, Some(l), None) => k -> DiffResult.Removed(l)
              },
          )
        case (left: Descriptor.Enumeration, right: Descriptor.Enumeration) =>
          DiffResult.Nested("Enumeration", List(None -> (left.cases diffed right.cases)))
        case (left: Descriptor.List, right: Descriptor.List) =>
          DiffResult.Nested("List", List(None -> go(left.value, right.value)))
        case (left: Descriptor.Optional, right: Descriptor.Optional) =>
          DiffResult.Nested("Optional", List(None -> go(left.value, right.value)))
        case (left: Descriptor.TextMap, right: Descriptor.TextMap) =>
          DiffResult.Nested("TextMap", List(None -> go(left.value, right.value)))
        case (left: Descriptor.GenMap, right: Descriptor.GenMap) =>
          DiffResult.Nested(
            "GenMap",
            List(
              Some("key") -> go(left.key, right.key),
              Some("value") -> go(left.value, right.value),
            ),
          )
        case (left: Descriptor.ContractId, right: Descriptor.ContractId) =>
          go(left.value, right.value)
        case (left, right) =>
          DiffResult.Different(left, right)

    go(xx, yy)(using mutable.Set.empty)

  protected def equalTo[A](that: A)(implicit diff: Diff[A]) = Assertion(
    TestArrow.make[A, Boolean] { a =>
      import zio.test.ErrorMessage as M
      val diffRes = diff.diff(that, a)
      TestTrace.boolean(diffRes.noDiff) {
        M.choice("There was no difference", "There was a difference") ++
          M.custom("Diff" + s" ${scala.Console.RED}-expected ${scala.Console.GREEN}+obtained") ++
          M.custom(scala.Console.RESET + diffRes.render)
      }
    }
  )

  given Conversion[String, Identifier] = name =>
    Identifier(
      PackageId(""),
      PackageName("examples"),
      PackageVersion("1.0.0"),
      ModuleName(name.split(":").head),
      EntityName(name.split(":").last),
    )

  protected def template(id: String, fields: (String, Descriptor)*) =
    Descriptor.constructor(
      id,
      Descriptor.record(fields :+ ("party" -> Descriptor.party)),
    )
