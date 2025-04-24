// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.proto

import com.digitalasset.canton.http.json.v2.ProtoInfo.{camelToSnake, normalizeName}
import com.digitalasset.canton.http.json.v2.{ExtractedProtoComments, FieldData, MessageInfo}
import io.protostuff.compiler.model.{FieldContainer, Proto}

import scala.jdk.CollectionConverters.CollectionHasAsScala

/** Extract comments from parsed proto files
  */
object ProtoDescriptionExtractor {

  def extract(protos: Seq[Proto]): ExtractedProtoComments = {
    val messages = protos.flatMap(_.getMessages().asScala)
    val componentsMessages =
      messages.map(msg => (msg.getName(), MessageInfo(toFieldData(msg)))).toMap
    val oneOfMessages = messages
      .flatMap(_.getOneofs().asScala)
      .map(msg => (msg.getName(), MessageInfo(toFieldData(msg))))
      .map { case (name, msgInfo) =>
        (camelToSnake(normalizeName(name)), msgInfo)
      }
      .toMap
    ExtractedProtoComments(componentsMessages, oneOfMessages)
  }

  private def toFieldData(message: FieldContainer) =
    FieldData(
      Option(message.getComments).filter(!_.isEmpty),
      message.getFields.asScala.map { field =>
        (field.getName, field.getComments)
      }.toMap,
    )

}
