// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.generator

import java.lang.reflect.Modifier
import com.daml.error.ErrorCode
import com.daml.error.generator.ErrorCodeDocumentationGenerator.{
  explanationTypeName,
  resolutionTypeName,
}
import com.daml.error.{Explanation, Resolution}
import org.reflections.Reflections

import scala.reflect.runtime.{universe => ru}
import scala.jdk.CollectionConverters._

/** Utility that indexes all error code implementations.
  *
  * @param prefix The classpath prefix that should be scanned for finding subtypes of [[ErrorCode]].
  */
case class ErrorCodeDocumentationGenerator(prefix: String = "com.daml") {

  def getDocItems: Seq[DocItem] = {
    val errorCodes = getErrorCodeInstances

    errorCodes.view.map(_.id).groupBy(identity).collect {
      case (code, occurrences) if occurrences.size > 1 =>
        sys.error(
          s"Error code $code is used ${occurrences.size} times but we require each error code to be unique! " +
            s"Make these error code unique to make this assertion run through"
        )
    }

    errorCodes.map(convertToDocItem).sortBy(_.code)
  }

  private def getErrorCodeInstances =
    new Reflections(prefix)
      .getSubTypesOf(classOf[ErrorCode])
      .asScala
      .view
      .collect {
        case clazz if !Modifier.isAbstract(clazz.getModifiers) =>
          clazz.getDeclaredField("MODULE$").get(clazz).asInstanceOf[ErrorCode]
      }
      .toSeq

  private def convertToDocItem(error: ErrorCode): DocItem = {
    val (expl, res) = getErrorNameAndAnnotations(error)
    DocItem(
      className = error.getClass.getName,
      category = error.category.getClass.getSimpleName.replace("$", ""),
      hierarchicalGrouping = error.parent.docNames.filter(_ != ""),
      conveyance = error.errorConveyanceDocString.getOrElse(""),
      code = error.id,
      explanation = expl,
      resolution = res,
    )
  }

  private def getErrorNameAndAnnotations(error: ErrorCode): (Explanation, Resolution) = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val mirroredType = mirror.reflect(error)
    val annotations: Seq[ru.Annotation] = mirroredType.symbol.annotations
    getAnnotations(annotations)
  }

  private def getAnnotations(annotations: Seq[ru.Annotation]): (Explanation, Resolution) = {
    annotations match {
      case Nil => (Explanation(""), Resolution(""))

      case v :: Nil if isExplanationAnnotation(v) =>
        (Explanation(parseValueOfAnnotation(v.tree)), Resolution(""))

      case v :: Nil if isResolutionAnnotation(v) =>
        (Explanation(""), Resolution(parseValueOfAnnotation(v.tree)))

      case v :: v2 :: Nil if isExplanationAnnotation(v) && isResolutionAnnotation(v2) =>
        (Explanation(parseValueOfAnnotation(v.tree)), Resolution(parseValueOfAnnotation(v2.tree)))

      case v :: v2 :: Nil if isResolutionAnnotation(v) && isExplanationAnnotation(v2) =>
        (Explanation(parseValueOfAnnotation(v2.tree)), Resolution(parseValueOfAnnotation(v.tree)))

      case _ =>
        sys.error(
          s"Error code has an unexpected amount (${annotations.size}) or type of annotations. " +
            s"Did you rename the error code annotations `${classOf[Explanation].getTypeName}` or `${classOf[Resolution].getTypeName}`?"
        )
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def parseValueOfAnnotation(tree: ru.Tree): String = {
    try {
      // get second (index starts at 0) child of tree as it contains the first value of the annotation
      Seq(1).map(tree.children(_).asInstanceOf[ru.Literal].value.value.asInstanceOf[String]) match {
        case s :: Nil => s.stripMargin
        case _ => sys.exit(1)
      }
    } catch {
      case x: RuntimeException =>
        println(
          "Failed to process description (description needs to be a constant-string. i.e. don't apply stripmargin here ...): " + tree.toString
        )
        throw x
    }
  }

  private def isResolutionAnnotation(annotation: ru.Annotation): Boolean =
    annotation.tree.tpe.toString == resolutionTypeName

  private def isExplanationAnnotation(annotation: ru.Annotation): Boolean =
    annotation.tree.tpe.toString == explanationTypeName
}

private object ErrorCodeDocumentationGenerator {
  private val explanationTypeName = classOf[Explanation].getTypeName.replace("$", ".")
  private val resolutionTypeName = classOf[Resolution].getTypeName.replace("$", ".")
}
