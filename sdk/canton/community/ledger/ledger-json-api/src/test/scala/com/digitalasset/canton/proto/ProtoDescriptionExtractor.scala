// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.proto

import com.digitalasset.canton.http.json.v2.ProtoInfo.{camelToSnake, normalizeName}
import com.digitalasset.canton.http.json.v2.{
  ExtractedProtoComments,
  ExtractedProtoFileComments,
  MessageData,
  MessageInfo,
  ServiceMethod,
}
import io.protostuff.compiler.model.{FieldContainer, Proto}

import scala.collection.immutable.SortedMap
import scala.jdk.CollectionConverters.CollectionHasAsScala

/** Extract comments from parsed proto files
  */
object ProtoDescriptionExtractor {

  def extract(protos: Seq[Proto]): ExtractedProtoComments = {
    // Create a map of file names to ExtractedProtoFileComments
    val fileCommentsMap = protos
      .map { proto =>
        val fileName = proto.getFilename
        val normalizedFileName =
          if (fileName.startsWith(s"${ProtoParser.ledgerApiProtoLocation}/")) {
            fileName.stripPrefix(s"${ProtoParser.ledgerApiProtoLocation}/")
          } else fileName

        val messages = proto.getMessages.asScala.map { msg =>
          msg.getName -> MessageInfo(toFieldData(msg))
        }.toMap

        val oneOfs = proto.getMessages.asScala.map { msg =>
          msg.getName -> SortedMap.from {
            msg.getOneofs.asScala.map { oneOf =>
              camelToSnake(normalizeName(oneOf.getName)) -> MessageInfo(toFieldData(oneOf))
            }
          }
        }.toMap

        val services: Map[String, Seq[ServiceMethod]] = proto.getServices.asScala.map { service =>
          service.getName -> service.getMethods.asScala.map { method =>
            ServiceMethod(method.getName, Option(method.getComments))
          }.toSeq
        }.toMap

        normalizedFileName -> ExtractedProtoFileComments(
          SortedMap.from(messages),
          SortedMap.from(oneOfs),
          services,
        )
      }
      .filter { case (_, fileComments) => !fileComments.isEmpty }
      .toMap

    ExtractedProtoComments(
      SortedMap.from(fileCommentsMap)
    )
  }

  private def toFieldData(message: FieldContainer) =
    MessageData(
      Option(message.getComments).filter(!_.isEmpty),
      message.getFields.asScala.map { field =>
        (field.getName, field.getComments)
      }.toMap,
    )

}
