// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.digitalasset.canton.http.json.v2.ProtoInfo.{camelToSnake, fixedName, normalizeName}
import io.circe.yaml.Printer

import scala.collection.immutable.SortedMap
import scala.io.Source
import scala.util.Using

/** Reads stored proto data in order to extract comments (and put into openapi).
  *
  * The algorithms used are inefficient, but since the code is used only to generate documentation
  * it should not cause any problems.
  */

final case class ProtoInfo(
    protoComments: ExtractedProtoComments,
    overrides: ExtractedProtoFileComments = ExtractedProtoFileComments(),
) {

  def findServiceDescription(
      file: String,
      serviceName: String,
      methodName: String,
  ): String = protoComments.fileComments.get(file) match {
    case Some(f) =>
      f.findServiceDescription(serviceName, methodName) match {
        case Some(desc) => desc
        case None =>
          throw new IllegalArgumentException(
            s"Cannot find method description in file: $file, method: $serviceName/$methodName"
          )
      }
    case None =>
      throw new IllegalArgumentException(
        s"Cannot find proto file: $file for method: $serviceName/$methodName"
      )
  }

  def findMessageInfo(
      componentName: String,
      parentComponentName: Option[String] = None,
  ): Option[MessageInfo] = findMessageWithOverrides(componentName)
    .orElse(findMessageWithOverrides(normalizeName(componentName)))
    .orElse(
      protoComments.oneOfs
        .get(normalizeName(parentComponentName.getOrElse("")))
        .flatMap(_.get(camelToSnake(normalizeName(componentName))))
    )
    .orElse(
      // if there is no message in proto comments, maybe it was defined only in json
      overrides.messages.get(componentName)
    )

  private def findMessageWithOverrides(name: String): Option[MessageInfo] = {
    // Convenience methods to maintain backward compatibility
    val original = protoComments.messages.get(name)
    val overridingMessage = overrides.messages.get(name)
    val commentOverride = overridingMessage.flatMap(m => m.message.comments)
    val fieldsOverride =
      overridingMessage.map(_.message.fieldComments).getOrElse(Map.empty)
    original.map(info =>
      info.copy(message =
        info.message.copy(
          fieldComments = info.message.fieldComments ++ fieldsOverride,
          comments = commentOverride.orElse(info.message.comments),
        )
      )
    )
  }
}

final case class MessageInfo(message: MessageData) {
  def getComments(): Option[String] = message.comments

  def getFieldComment(name: String): Option[String] =
    message.fieldComments
      .get(name)
      .orElse(message.fieldComments.get(camelToSnake(name)))
      .orElse(message.fieldComments.get(fixedName(name)))

  def isFieldRequired(fieldName: String): Boolean =
    getFieldComment(fieldName) match {
      case Some(comment) =>
        val lines = comment.split("\n")
        lines.exists(line => ProtoInfo.requiredPattern.findFirstIn(line).isDefined)
      case None => false
    }

  def isFieldOptional(fieldName: String): Boolean =
    getFieldComment(fieldName) match {
      case Some(comment) =>
        val lines = comment.split("\n")
        lines.exists(line =>
          ProtoInfo.optionalPattern
            .findFirstIn(line)
            .isDefined || ProtoInfo.optionalWhenRequiredUnlessPattern.findFirstIn(line).isDefined
        )
      case None => false
    }

  override def toString: String = getComments().getOrElse("")
}

final case class MessageData(comments: Option[String], fieldComments: Map[String, String])

final case class ExtractedProtoComments(
    fileComments: SortedMap[String, ExtractedProtoFileComments] = SortedMap.empty
) {
  def toYaml(): String = {
    import io.circe.syntax.*
    import io.circe.generic.auto.*
    val yamlPrinter = Printer(preserveOrder = true)
    yamlPrinter.pretty(this.asJson)
  }

  lazy val messages = fileComments.values.flatMap(_.messages).to(SortedMap)

  def oneOfs: SortedMap[String, SortedMap[String, MessageInfo]] =
    fileComments.values.flatMap(_.oneOfs).to(SortedMap)
}

final case class ServiceMethod(
    name: String,
    comments: Option[String],
)

final case class ExtractedProtoFileComments(
    messages: SortedMap[String, MessageInfo] = SortedMap.empty,
    oneOfs: SortedMap[String, SortedMap[String, MessageInfo]] = SortedMap.empty,
    services: Map[String, Seq[ServiceMethod]] = Map.empty,
) {
  def isEmpty: Boolean = messages.isEmpty && oneOfs.isEmpty && services.isEmpty

  def findServiceDescription(serviceName: String, methodName: String): Option[String] =
    services.get(serviceName).flatMap { methods =>
      methods.find(_.name == methodName).flatMap(_.comments)
    }
}

object ProtoInfo {
  val optionalPattern = raw"^\s*\bOptional\b".r
  val requiredPattern = raw"^\s*\bRequired\b(?!\s+unless\b)".r
  val optionalWhenRequiredUnlessPattern = raw"^\s*\bRequired\b(?:\s+unless\b)".r
  val LedgerApiDescriptionResourceLocation = "ledger-api/proto-data.yml"
  val CommentsOverridesApiDescriptionResourceLocation = "ledger-api/json-comments-overrides.yml"
  def camelToSnake(name: String): String =
    name
      .replaceAll("([a-z0-9])([A-Z])", "$1_$2")
      .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
      .toLowerCase

  // Special cases for names that (due to mistake) do not follow the usual convention
  def fixedName(name: String): String = name match {
    case "createArgument" => "create_arguments"
    case other => other
  }

  /** We drop initial `Js` prefix and single digits suffixes.
    */
  def normalizeName(s: String): String =
    if (s.nonEmpty && s.last.isDigit) s.dropRight(1) else if (s.startsWith("Js")) s.drop(2) else s

  def loadOverrides(): ExtractedProtoFileComments =
    Using
      .resource(
        Source.fromResource(
          CommentsOverridesApiDescriptionResourceLocation,
          classOf[com.digitalasset.canton.http.json.v2.ProtoInfo.type].getClassLoader,
        )
      ) { jsonCommentsOverridersResource =>
        import io.circe.generic.auto.*
        import io.circe.yaml.Parser as YamlParser
        Using.resource(jsonCommentsOverridersResource.bufferedReader()) { overridesReader =>
          YamlParser.default
            .parse(overridesReader)
            .flatMap(_.as[ExtractedProtoFileComments])
        }
      }
      .getOrElse(
        throw new IllegalStateException(
          s"Cannot load proto comments overrides from $CommentsOverridesApiDescriptionResourceLocation"
        )
      )

  def loadData(): ProtoInfo = {
    import io.circe.generic.auto.*
    import io.circe.yaml.Parser as YamlParser
    val protoDocumentation = Using.resource(
      Source.fromResource(
        LedgerApiDescriptionResourceLocation,
        classOf[com.digitalasset.canton.http.json.v2.ProtoInfo.type].getClassLoader,
      )
    ) { protoCommentsResource =>
      Using.resource(protoCommentsResource.bufferedReader()) { protoCommentsReader =>
        YamlParser.default
          .parse(protoCommentsReader)
          .flatMap(_.as[ExtractedProtoComments])
          .getOrElse(
            throw new IllegalStateException(
              s"Cannot load scanned proto comments from $LedgerApiDescriptionResourceLocation"
            )
          )
      }
    }
    val commentOverrides = loadOverrides()
    ProtoInfo(protoDocumentation, commentOverrides)
  }
}
