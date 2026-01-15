// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.schema

import com.digitalasset.transcode.schema.Descriptor.*

import scala.collection.mutable

object Debug {
  def showDescriptorsFlat(dictionary: Dictionary[Descriptor]): String =
    val buf = StringBuilder()
    def go(descriptor: Descriptor): Unit = descriptor match
      case Constructor(id, typeParams, body) => buf.append(id.show)
      case Application(ctor, args) =>
        go(ctor)
        buf.append("(")
        args.zipWithIndex.foreach { (arg, ix) =>
          go(arg)
          if ix < args.length - 1 then buf.append(", ")
        }
        buf.append(")")
      case Variable(name) => buf.append("$").append(name): Unit
      case Record(fields) =>
        buf.append("Record(").append(System.lineSeparator()): Unit
        fields.foreach { (n, t) =>
          buf.append("  ").append(n).append(": "): Unit
          go(t)
          buf.append(System.lineSeparator())
        }
        buf.append(")")
      case Variant(cases) =>
        buf.append("Variant(").append(System.lineSeparator()): Unit
        cases.foreach { (n, t) =>
          buf.append("  ").append(n).append(": "): Unit
          go(t)
          buf.append(System.lineSeparator())
        }
        buf.append(")")
      case Enumeration(cases) =>
        buf.append("Enumeration(").append(System.lineSeparator()): Unit
        cases.foreach(x => buf.append("  ").append(x).append(System.lineSeparator()))
        buf.append(")")
      case List(value) => buf.append("List("); go(value); buf.append(")")
      case Optional(value) => buf.append("Optional("); go(value); buf.append(")")
      case TextMap(value) => buf.append("TextMap("); go(value); buf.append(")")
      case GenMap(key, value) =>
        buf.append("GenMap("); go(key); buf.append(", "); go(value); buf.append(")")
      case Descriptor.Unit => buf.append("unit")
      case Descriptor.Bool => buf.append("bool")
      case Descriptor.Text => buf.append("text")
      case Descriptor.Int64 => buf.append("int64")
      case Numeric(scale) => buf.append(s"numeric($scale)")
      case Descriptor.Timestamp => buf.append("timestamp")
      case Descriptor.Date => buf.append("date")
      case Descriptor.Party => buf.append("party")
      case ContractId(value) => buf.append("contractId("); go(value); buf.append(")")

    buf.append("--- Dictionary ---").append(System.lineSeparator()): Unit
    buf
      .append("Strict: ")
      .append(dictionary.strictPackageMatching)
      .append(System.lineSeparator()): Unit

    buf.append("--- Entities ---").append(System.lineSeparator()): Unit
    dictionary.entities.foreach { entity =>
      buf.append(if entity.isInterface then "Interface" else "Template").append(" "): Unit
      buf.append(entity.templateId.show).append(System.lineSeparator()): Unit
      buf.append("  Payload: "); go(entity.payload); buf.append(System.lineSeparator())
      entity.key.foreach { k =>
        buf.append("  Key: "); go(k); buf.append(System.lineSeparator())
      }
      if entity.implements.nonEmpty then
        buf
          .append(s"  Implements: ${entity.implements.map(_.show).mkString(", ")}")
          .append(System.lineSeparator()): Unit
      buf.append("  Choices:").append(System.lineSeparator()): Unit
      entity.choices.foreach { choice =>
        buf.append("    ").append(choice.name).append(System.lineSeparator()): Unit
        buf
          .append("      Consuming: ")
          .append(choice.consuming)
          .append(System.lineSeparator()): Unit
        buf.append("      Argument: "); go(choice.argument); buf.append(System.lineSeparator())
        buf.append("      Result: "); go(choice.result); buf.append(System.lineSeparator())
      }
      buf.append(System.lineSeparator())
    }

    buf.append("--- Descriptors ---").append(System.lineSeparator()): Unit
    val allDescriptors =
      dictionary.entities.flatMap(e =>
        e.payload +: (e.key.toList ++ e.choices.flatMap(c => Seq(c.argument, c.result)))
      )
    flatten(allDescriptors).foreach { root =>
      buf.append(root.id.show)
      if root.typeParams.nonEmpty then buf.append(root.typeParams.mkString("[", ", ", "]"))
      buf.append(System.lineSeparator())
      val Constructor(_, _, body) = root
      go(body)
      buf.append(System.lineSeparator()).append("---"): Unit
      buf.append(System.lineSeparator())
    }

    buf.toString()

  private def flatten(descriptors: Seq[Descriptor]): Seq[Constructor] =
    val buf = mutable.Map.empty[Identifier, Constructor]
    def go(descriptor: Descriptor): Unit = descriptor match
      case c @ Constructor(id, _, body) if !buf.contains(id) =>
        buf.put(id, c): Unit
        go(body)
      case Record(fields) => fields.foreach((_, t) => go(t))
      case Variant(cases) => cases.foreach((_, t) => go(t))
      case List(value) => go(value)
      case Optional(value) => go(value)
      case TextMap(value) => go(value)
      case GenMap(key, value) => go(key); go(value)
      case ContractId(value) => go(value)
      case Application(ctor, args) => go(ctor); args.foreach(go)
      case other => // ignore
    descriptors.foreach(go)
    buf.toSeq.sortBy(x => (x._1.packageName, x._1.moduleName, x._1.entityName)).map(_._2)

  extension (id: Identifier)
    private def show: String =
      s"${id.packageName}:${id.moduleName}:${id.entityName}#${id.packageVersion}/${id.packageId}"
}
