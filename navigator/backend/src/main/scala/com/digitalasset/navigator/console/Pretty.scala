// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console

import java.time.Instant
import java.time.format.DateTimeFormatter

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.model
import com.daml.lf.value.{Value => V}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
  * Pretty printing ledger objects.
  * - If the output is a list of objects, it is printed using a ASCII table
  * - If the output is a single object, a JSON-like object is generated and then printed in YAML format
  */
case class PrettyField(name: String, value: PrettyNode)

/** A JSON-like object */
sealed trait PrettyNode
final case class PrettyObject(fields: List[PrettyField]) extends PrettyNode
final case class PrettyPrimitive(value: String) extends PrettyNode
final case class PrettyArray(values: List[PrettyNode]) extends PrettyNode

object PrettyField {
  def apply(name: String, value: String): PrettyField = PrettyField(name, PrettyPrimitive(value))
}
object PrettyObject {
  def apply(xs: PrettyField*): PrettyObject = PrettyObject(xs.toList)
}
object PrettyArray {
  def apply(xs: PrettyNode*): PrettyArray = PrettyArray(xs.toList)
}

object Pretty {

  /** Date and time in ISO format */
  def prettyInstant(t: Instant): String =
    DateTimeFormatter.ISO_INSTANT.format(t)

  /** Shorten a string to the desired length */
  def abbreviate(input: String, maxLength: Int): String = {
    if (input.length <= maxLength)
      input
    else
      input.substring(0, maxLength - 3) + "..."
  }

  /** A short template name (i.e., without the module or package name) */
  def shortTemplateName(name: String): String =
    name
      .replaceAll(".*[.]", "")
      .replaceAll("@.*", "")

  /** A short template name (i.e., without the module or package name) */
  def shortTemplateId(name: ApiTypes.TemplateId): String = {
    val ident = ApiTypes.TemplateId.unwrap(name)
    shortTemplateName(ident.moduleName + ":" + ident.entityName)
  }

  def shortTemplateId(name: model.DamlLfIdentifier): String =
    shortTemplateName(name.qualifiedName.name.toString())

  /** A short template name (i.e., without the module or package name) */
  def shortTemplateStringId(name: model.TemplateStringId): String =
    shortTemplateName(model.TemplateStringId.unwrap(name))

  /** A description of a command result */
  def commandResult(ps: model.PartyState, commandId: ApiTypes.CommandId): PrettyNode = {
    val delayMilis = 200L
    val attempts = 10

    @tailrec
    def go(attemptsRemaining: Int): List[PrettyField] = {
      val result = Try(ps.ledger.statusOf(commandId, ps.packageRegistry))
      result match {
        case Success(Some(status)) =>
          status match {
            case model.CommandStatusWaiting() if attemptsRemaining > 0 =>
              Thread.sleep(delayMilis)
              go(attemptsRemaining - 1)
            case model.CommandStatusWaiting() =>
              List(
                PrettyField("Status", "Pending"),
                PrettyField(
                  "Note",
                  s"Use 'command ${ApiTypes.CommandId.unwrap(commandId)}' to query the status.")
              )
            case model.CommandStatusError(code, details) =>
              List(
                PrettyField("Status", "Failed"),
                PrettyField("Error", s"$code: $details")
              )
            case model.CommandStatusSuccess(tx) =>
              List(
                PrettyField("Status", "Success"),
                PrettyField("TransactionId", ApiTypes.TransactionId.unwrap(tx.id))
              )
            case model.CommandStatusUnknown() =>
              List(
                PrettyField("Status", "Unknown"),
                PrettyField("Error", "Failed to track command.")
              )
          }
        case Success(None) =>
          List(
            PrettyField("Status", "Unknown"),
            PrettyField("Error", "Command ID not found.")
          )
        case Failure(e) =>
          List(
            PrettyField("Status", "Unknown"),
            PrettyField("Error", e.getMessage)
          )
      }
    }

    PrettyObject(PrettyField("CommandId", ApiTypes.CommandId.unwrap(commandId)) :: go(attempts))
  }

  /** Creates a JSON-like object that describes a DAML-LF type */
  def damlLfType(
      param: model.DamlLfType,
      typeDefs: model.DamlLfTypeLookup,
      doNotExpand: Set[model.DamlLfIdentifier] = Set.empty
  ): (Option[String], PrettyNode) = param match {
    case typeCon: model.DamlLfTypeCon =>
      val id = model
        .DamlLfIdentifier(typeCon.name.identifier.packageId, typeCon.name.identifier.qualifiedName)
      if (doNotExpand.contains(id)) {
        // val dt = typeCon.instantiate(typeDefs(id).get)
        val dt = model.damlLfInstantiate(typeCon, typeDefs(id).get)
        (Some(typeCon.name.identifier.qualifiedName.name.toString), PrettyPrimitive("..."))
      } else {
        // Once a type is instantiated, do not instantiate it in any child node.
        // Required to prevent infinite expansion of recursive types.
        // val dt = typeCon.instantiate(typeDefs(id).get)
        val dt = model.damlLfInstantiate(typeCon, typeDefs(id).get)
        (
          Some(typeCon.name.identifier.qualifiedName.name.toString),
          damlLfDataType(dt, typeDefs, doNotExpand + id))
      }
    case model.DamlLfTypeNumeric(_) =>
      (None, PrettyPrimitive("Decimal"))
    case typePrim: model.DamlLfTypePrim =>
      (None, damlLfPrimitive(typePrim.typ, typePrim.typArgs, typeDefs, doNotExpand))
    case typeVar: model.DamlLfTypeVar =>
      (None, PrettyPrimitive(s"<$typeVar>"))
  }

  /** Creates a JSON-like object that describes a DAML-LF primitive */
  def damlLfPrimitive(
      param: model.DamlLfPrimType,
      typArgs: model.DamlLfImmArraySeq[model.DamlLfType],
      typeDefs: model.DamlLfTypeLookup,
      doNotExpand: Set[model.DamlLfIdentifier]
  ): PrettyNode = param match {
    case model.DamlLfPrimType.List =>
      val listType = typArgs.headOption
        .map(t => damlLfType(t, typeDefs, doNotExpand))
        .getOrElse((None, PrettyPrimitive("???")))
      PrettyObject(
        PrettyField(listType._1.fold("List")(n => s"List [$n]"), listType._2)
      )
    case model.DamlLfPrimType.Bool => PrettyPrimitive("Bool")
    case model.DamlLfPrimType.Int64 => PrettyPrimitive("Int64")
    case model.DamlLfPrimType.ContractId => PrettyPrimitive("ContractId")
    case model.DamlLfPrimType.Date => PrettyPrimitive("Date")
    case model.DamlLfPrimType.Party => PrettyPrimitive("Party")
    case model.DamlLfPrimType.Text => PrettyPrimitive("Text")
    case model.DamlLfPrimType.Timestamp => PrettyPrimitive("Timestamp")
    case model.DamlLfPrimType.Unit => PrettyPrimitive("Unit")
    case _ => PrettyPrimitive(s"???($param)")
  }

  private def damlLfDataType(
      dt: model.DamlLfDataType,
      typeDefs: model.DamlLfTypeLookup,
      doNotExpand: Set[model.DamlLfIdentifier]
  ): PrettyNode = {
    dt match {
      case r: model.DamlLfRecord =>
        PrettyObject(r.fields.toList.map(f => {
          val fieldType = damlLfType(f._2, typeDefs, doNotExpand)
          val label = fieldType._1.fold[String](f._1)(n => s"${f._1} [$n]")
          PrettyField(label, fieldType._2)
        }))
      case v: model.DamlLfVariant =>
        PrettyObject(v.fields.toList.map(f => {
          val fieldType = damlLfType(f._2, typeDefs, doNotExpand)
          val label = fieldType._1.fold[String](f._1)(n => s"${f._1} [$n]")
          PrettyField(label, fieldType._2)
        }))
      case e: model.DamlLfEnum =>
        PrettyArray(e.constructors.map(PrettyPrimitive).toList)
    }
  }

  def damlLfDefDataType(
      id: model.DamlLfIdentifier,
      typeDefs: model.DamlLfIdentifier => Option[model.DamlLfDefDataType],
      doNotExpand: Set[model.DamlLfIdentifier] = Set.empty
  ): PrettyNode = {
    val ddt = typeDefs(id)
    ddt match {
      case Some(model.DamlLfDefDataType(tv, dt)) =>
        damlLfDataType(dt, typeDefs, Set.empty)
      case None =>
        PrettyPrimitive(s"??? [${id.qualifiedName.name}]")
    }
  }

  /** Creates a JSON-like object that describes an argument value */
  def argument(arg: model.ApiValue): PrettyNode = arg match {
    case V.ValueRecord(id, fields) =>
      PrettyObject(
        fields.iterator.zipWithIndex.map {
          case ((flabel, fvalue), ix) =>
            PrettyField(flabel getOrElse (ix: Int).toString, argument(fvalue))
        }.toSeq: _*
      )
    case V.ValueVariant(id, constructor, value) =>
      PrettyObject(
        PrettyField(constructor, argument(value))
      )
    case V.ValueEnum(id, constructor) =>
      PrettyPrimitive(constructor)
    case V.ValueList(elements) =>
      PrettyArray(
        elements.toImmArray.map(e => argument(e)).toSeq: _*
      )
    case V.ValueText(value) => PrettyPrimitive(value)
    case V.ValueInt64(value) => PrettyPrimitive(value.toString)
    case V.ValueNumeric(value) => PrettyPrimitive(value.toUnscaledString)
    case V.ValueBool(value) => PrettyPrimitive(value.toString)
    case V.ValueContractId(value) => PrettyPrimitive(value.toString)
    case V.ValueTimestamp(value) => PrettyPrimitive(value.toString)
    case V.ValueDate(value) => PrettyPrimitive(value.toString)
    case V.ValueParty(value) => PrettyPrimitive(value.toString)
    case V.ValueUnit => PrettyPrimitive("<unit>")
    case V.ValueOptional(None) => PrettyPrimitive("<none>")
    case V.ValueOptional(Some(v)) => PrettyObject(PrettyField("value", argument(v)))
    case V.ValueTextMap(map) =>
      PrettyObject(map.toImmArray.toList.map {
        case (key, value) => PrettyField(key, argument(value))
      })
    case V.ValueGenMap(genMap) =>
      PrettyArray(genMap.toSeq.map {
        case (key, value) =>
          PrettyObject(
            PrettyField("key", argument(key)),
            PrettyField("value", argument(value))
          )
      }: _*)
    case _: model.ApiImpossible => sys.error("impossible! structs are not serializable")
  }

  /** Outputs an object in YAML format */
  def yaml(node: PrettyNode): String = {
    val indent = "  "
    val arrayIndent = "- "
    val alignFields = false

    def go(node: PrettyNode, i: Int): String = node match {
      case PrettyPrimitive(value) =>
        value
      case PrettyArray(values) =>
        values
          .map(v => arrayIndent + go(v, i + 1))
          .mkString("\n" + indent * i)
      case PrettyObject(fields) if fields.isEmpty =>
        "{}"
      case PrettyObject(fields) =>
        val maxFieldLength = fields.map(_.name.length).max + 1
        fields
          .map(f => {
            val left =
              if (alignFields)
                (f.name + ":").formatted(s"%-${maxFieldLength}s")
              else
                f.name + ":"
            val right = f.value match {
              case n: PrettyPrimitive => s" ${go(f.value, i + 1)}"
              case n: PrettyArray => "\n" + indent * i + go(f.value, i)
              case n: PrettyObject => "\n" + indent * (i + 1) + go(f.value, i + 1)
            }
            left + right
          })
          .mkString("\n" + indent * i)
    }
    go(node, 0) + "\n"
  }

  /** Outputs a pretty ASCII table with the given data */
  def asciiTable(
      state: State,
      header: List[String],
      data: TraversableOnce[TraversableOnce[String]]): String = {
    val width = state.reader.getTerminal.getWidth
    AsciiTable()
      .width(if (width > 4) width else 80)
      .multiline(false)
      .columnMinWidth(4)
      .sampleAtMostRows(200)
      .header(header)
      .rows(data)
      .toString
  }
}
