// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.generator.app

import java.nio.file.{Files, Paths, StandardOpenOption}

import com.daml.error.{ErrorGroupPath, ErrorGroupSegment}
import com.daml.error.generator.{ErrorCodeDocumentationGenerator, ErrorDocItem, GroupDocItem}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** Generates error codes inventory as a reStructuredText
  */
object ErrorCodeInventoryDocsGen_App {

  def main(args: Array[String]): Unit = {
    val text = {
      val (errorDocItems, groupDocItems): (Seq[ErrorDocItem], Seq[GroupDocItem]) =
        new ErrorCodeDocumentationGenerator().getDocItems

      val groupSegmentsToExplanationMap: Map[List[ErrorGroupSegment], Option[String]] =
        groupDocItems.map { groupDocItem: GroupDocItem =>
          groupDocItem.errorGroupPath.segments -> groupDocItem.explanation.map(_.explanation)
        }.toMap

      val errorCodes: Seq[ErrorCodeValue] = errorDocItems.map { (errorDocItem: ErrorDocItem) =>
        ErrorCodeValue(
          category = errorDocItem.category,
          errorGroupPath = errorDocItem.errorGroupPath,
          conveyance = errorDocItem.conveyance.getOrElse("").replace('\n', ' '),
          code = errorDocItem.code,
          deprecationO = errorDocItem.deprecation.map(_.deprecation.replace('\n', ' ')),
          explanation = errorDocItem.explanation.fold("")(_.explanation).replace('\n', ' '),
          resolution = errorDocItem.resolution.fold("")(_.resolution).replace('\n', ' '),
        )
      }

      val root = ErrorGroupTree.empty()

      // Build trie like structure of error groups and error codes.
      errorCodes.foreach(errorCode =>
        root.insertErrorCode(errorCode, groupSegmentsToExplanationMap)
      )
      // Traverse the trie to emit error code text.
      ErrorGroupTree
        .collectErrorCodesAsReStructuredTextSubsections(root)
        .mkString("\n\n")
    }

    if (args.length >= 1) {
      val outputFile = Paths.get(args(0))
      val _ = Files.write(outputFile, text.getBytes, StandardOpenOption.CREATE_NEW)
    } else {
      println(text)
    }

  }

}

case class ErrorCodeValue(
    code: String,
    errorGroupPath: ErrorGroupPath,
    category: String,
    explanation: String,
    resolution: String,
    conveyance: String,
    deprecationO: Option[String],
)

class ErrorGroupTree(
    val name: String,
    val explanation: Option[String] = None,
    children: mutable.Map[ErrorGroupSegment, ErrorGroupTree] =
      new mutable.HashMap[ErrorGroupSegment, ErrorGroupTree](),
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
      getExplanation: (List[ErrorGroupSegment]) => Option[String],
  ): Unit = {
    insert(
      remaining = errorCode.errorGroupPath.segments,
      path = Nil,
      errorCode = errorCode,
      getExplanation = getExplanation,
    )
  }

  private def insert(
      remaining: List[ErrorGroupSegment],
      errorCode: ErrorCodeValue,
      path: List[ErrorGroupSegment],
      getExplanation: (List[ErrorGroupSegment]) => Option[String],
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
           |-------------------------------------------------------------------------------------------------------------------
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
         |    **Depreciation**: ${d}
         |    """.stripMargin)
    s"""Error code: ${e.code}
       |^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
