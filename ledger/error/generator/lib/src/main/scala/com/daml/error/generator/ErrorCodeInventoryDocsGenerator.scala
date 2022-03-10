// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.generator

import com.daml.error.{ErrorClass, Grouping}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ErrorCodeInventoryDocsGenerator {

  def genText(): String = {
    val errorDocItems = ErrorCodeDocumentationGenerator.getErrorCodeItems()
    val groupDocItems = ErrorCodeDocumentationGenerator.getErrorGroupItems()

    val groupSegmentsToExplanationMap: Map[List[Grouping], Option[String]] =
      groupDocItems.map { groupDocItem: ErrorGroupDocItem =>
        groupDocItem.errorClass.groupings -> groupDocItem.explanation.map(_.explanation)
      }.toMap

    val errorCodes: Seq[ErrorCodeValue] = errorDocItems.map { errorCodeDocItem: ErrorCodeDocItem =>
      ErrorCodeValue(
        category = errorCodeDocItem.category,
        errorGroupPath = errorCodeDocItem.hierarchicalGrouping,
        conveyance = newlineIntoSpace(errorCodeDocItem.conveyance.getOrElse("")),
        code = errorCodeDocItem.code,
        deprecationO = errorCodeDocItem.deprecation.map(v =>
          newlineIntoSpace(v.message) + v.since.fold("")(s => s" Since: ${newlineIntoSpace(s)}")
        ),
        explanation = newlineIntoSpace(errorCodeDocItem.explanation.fold("")(_.explanation)),
        resolution = newlineIntoSpace(errorCodeDocItem.resolution.fold("")(_.resolution)),
      )
    }

    val root = ErrorGroupTree.empty()

    // Build trie like structure of error groups and error codes.
    errorCodes.foreach(errorCode => root.insertErrorCode(errorCode, groupSegmentsToExplanationMap))
    // Traverse the trie to emit error code text.
    ErrorGroupTree
      .collectErrorCodesAsReStructuredTextSubsections(root)
      .mkString("\n\n")

  }

  private def newlineIntoSpace(s: String): String = {
    s.replace('\n', ' ')
  }

}

case class ErrorCodeValue(
    code: String,
    errorGroupPath: ErrorClass,
    category: String,
    explanation: String,
    resolution: String,
    conveyance: String,
    deprecationO: Option[String],
)

class ErrorGroupTree(
    val name: String,
    val explanation: Option[String] = None,
    children: mutable.Map[Grouping, ErrorGroupTree] =
      new mutable.HashMap[Grouping, ErrorGroupTree](),
    errorCodes: mutable.Map[String, ErrorCodeValue] = new mutable.HashMap[String, ErrorCodeValue](),
) {

  def sortedSubGroups(): List[ErrorGroupTree] = {
    children.values.toList.sortBy(_.name)
  }

  def sortedErrorCodes(): List[ErrorCodeValue] = {
    errorCodes.values.toList.sortBy(_.code)
  }

  def insertErrorCode(
      errorCode: ErrorCodeValue,
      getExplanation: (List[Grouping]) => Option[String],
  ): Unit = {
    insert(
      remaining = errorCode.errorGroupPath.groupings,
      path = Nil,
      errorCode = errorCode,
      getExplanation = getExplanation,
    )
  }

  private def insert(
      remaining: List[Grouping],
      errorCode: ErrorCodeValue,
      path: List[Grouping],
      getExplanation: (List[Grouping]) => Option[String],
  ): Unit = {

    remaining match {
      case Nil =>
        assert(!errorCodes.contains(errorCode.code), s"Code: ${errorCode.code} is already present!")
        errorCodes.put(errorCode.code, errorCode): Unit
      case headGroup :: tail =>
        val newPath = path :+ headGroup
        if (!children.contains(headGroup)) {
          children.put(
            headGroup,
            new ErrorGroupTree(
              name = headGroup.docName,
              explanation = getExplanation(newPath),
            ),
          )
        }
        children(headGroup).insert(
          remaining = tail,
          errorCode = errorCode,
          path = newPath,
          getExplanation,
        )
    }
  }

}

object ErrorGroupTree {
  def empty(): ErrorGroupTree = new ErrorGroupTree(
    name = "",
    explanation = None,
  )

  def collectErrorCodesAsReStructuredTextSubsections(root: ErrorGroupTree): List[String] = {

    // in-order tree traversal
    def iter(
        tree: ErrorGroupTree,
        path: List[String],
        groupHierarchicalIndex: List[Int],
    ): List[String] = {
      val newPath = path :+ tree.name
      val textBuffer: mutable.ArrayBuffer[String] = new ArrayBuffer[String]()

      // Add group text
      textBuffer.addOne(s"""${groupHierarchicalIndex.mkString(".")}. ${newPath.mkString(" / ")}
           |===================================================================================================================
           |
           |${tree.explanation.getOrElse("")}
           |""".stripMargin)
      // Add error codes in this group
      textBuffer.addAll(
        tree
          .sortedErrorCodes()
          .map(handleErrorCode)
      )
      // Recurse to sub-groups
      textBuffer.addAll(
        tree
          .sortedSubGroups()
          .zipWithIndex
          .flatMap { case (subGroup: ErrorGroupTree, index: Int) =>
            iter(
              subGroup,
              newPath,
              groupHierarchicalIndex = groupHierarchicalIndex :+ (index + 1),
            )
          }
      )
      textBuffer.toList
    }

    root
      .sortedSubGroups()
      .zipWithIndex
      .flatMap { case (subGroup, i) =>
        iter(subGroup, path = List(), groupHierarchicalIndex = List(i + 1))
      }
  }

  private def handleErrorCode(e: ErrorCodeValue): String = {
    val deprecationText = e.deprecationO.fold("")(d => s"""
         |    **Deprecation**: ${d}
         |    """.stripMargin)
    val errorCodeReferenceName = s"error_code_${e.code}"
    s"""
       |.. _$errorCodeReferenceName:
       |
       |${e.code}
       |---------------------------------------------------------------------------------------------------------------------------------------
       |    $deprecationText
       |    **Explanation**: ${e.explanation}
       |
       |    **Category**: ${e.category}
       |
       |    **Conveyance**: ${e.conveyance}
       |
       |    **Resolution**: ${e.resolution}
       |
       |""".stripMargin
  }
}
