// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import org.scalatest.AppendedClues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

/** This test ensures that all new proto comments contain clear Required or Optional markers. It
  * accepts a certain number of already existing fields without clear markers to avoid failing the
  * build.
  */
class ProtoDocumentationConformanceTest extends AnyWordSpecLike with Matchers with AppendedClues {

  // This is number of known fields in proto files without clear Required or Optional markers
  // TODO(i30114) Update this if/when some comments are fixed
  val numberOfKnownUnspecifiedFields = 216

  "new proto comments" should {
    "contain clear Required or Optional marks" in {
      val allFields = ProtoCommentsChecker.reportProto()
      val conflictingFields = allFields.filter(f => f.isOptional && f.isRequired)
      val unspecifiedFields = allFields.filter(f => !f.isOptional && !f.isRequired)
      conflictingFields shouldBe empty withClue
        s"The following fields have conflicting Required and Optional markers:\n" +
        conflictingFields
          .map(f => s"- ${f.fileName} / ${f.messageName} / ${f.fieldName}: ${f.comment.trim}")
          .mkString("\n")
      unspecifiedFields.length should be <= numberOfKnownUnspecifiedFields withClue
        s"""The are ${unspecifiedFields.length} fields in proto files without Required or Optional markers, max accepted number is $numberOfKnownUnspecifiedFields
           |Check if any recently added fields in proto files contain clear Required or Optional comment""".stripMargin
    }
  }
}

final case class ReportedField(
    fileName: String,
    messageName: String,
    fieldName: String,
    isOptional: Boolean,
    isRequired: Boolean,
    comment: String,
)

object ProtoCommentsChecker {

  lazy val protoInfo = GenerateJSONApiDocs.regenerateProtoData()

  def reportProto(): Seq[ReportedField] =
    protoInfo.protoComments.fileComments.flatMap { case (fileName, fileComments) =>
      fileComments.messages.flatMap { case (messageName, messageInfo) =>
        messageInfo.message.fieldComments.map { case (fieldName, comment) =>
          ReportedField(
            fileName,
            messageName,
            fieldName,
            messageInfo.isFieldOptional(fieldName),
            messageInfo.isFieldRequired(fieldName),
            comment,
          )
        }
      }
    }.toSeq
}
