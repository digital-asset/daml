// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.generator

import java.lang.reflect.Modifier

import com.daml.error._
import org.reflections.Reflections

import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe.typeOf
import scala.reflect.runtime.{universe => ru}

/** Utility that indexes all error code implementations.
  *
  * @param prefixes The classpath prefixes that should be scanned for finding subtypes of [[ErrorCode]].
  */
class ErrorCodeDocumentationGenerator(prefixes: Array[String] = Array("com.daml")) {

  import ErrorCodeDocumentationGenerator._

  def getDocItems: (Seq[ErrorDocItem], Seq[GroupDocItem]) = {
    val errorCodes = getInstances[ErrorCode]
    errorCodes.view.map(_.id).groupBy(identity).collect {
      case (code, occurrences) if occurrences.size > 1 =>
        sys.error(
          s"Error code $code is used ${occurrences.size} times but we require each error code to be unique! " +
            s"Make these error code unique to make this assertion run through"
        )
    }

    val groups = getInstances[ErrorGroup]
    groups.view.map(_.errorClass).groupBy(identity).collect {
      case (group, occurrences) if occurrences.size > 1 =>
        sys.error(
          s"There are ${occurrences.size} groups named $group but we require each group class name to be unique! " +
            s"Make these group class namers unique to make this assertion run through"
        )
    }

    (errorCodes.map(convertToDocItem).sortBy(_.code), groups.map(convertToDocItem))
  }

  private def getInstances[T: ru.TypeTag]: Seq[T] =
    new Reflections(prefixes)
      .getSubTypesOf(runtimeMirror.runtimeClass(typeOf[T]))
      .asScala
      .view
      .collect {
        case clazz if !Modifier.isAbstract(clazz.getModifiers) =>
          clazz.getDeclaredField("MODULE$").get(clazz).asInstanceOf[T]
      }
      .toSeq

  private def convertToDocItem(error: ErrorCode): ErrorDocItem = {
    val ErrorDocumentationAnnotations(deprecation, explanation, resolution) =
      getErrorDocumentationAnnotations(error)

    ErrorDocItem(
      className = error.getClass.getName,
      category = simpleClassName(error.category),
      hierarchicalGrouping = error.parent,
      conveyance = error.errorConveyanceDocString,
      code = error.id,
      deprecation = deprecation,
      explanation = explanation,
      resolution = resolution,
    )
  }

  private def convertToDocItem(group: ErrorGroup): GroupDocItem = {
    val GroupDocumentationAnnotations(explanation) =
      getGroupDocumentationAnnotations(group)

    GroupDocItem(
      errorClass = group.errorClass,
      className = group.fullClassName,
      explanation = explanation,
    )
  }

  private def simpleClassName(any: Any): String =
    any.getClass.getSimpleName.replace("$", "")

  private case class ErrorDocumentationAnnotations(
      deprecation: Option[DeprecatedDocs],
      explanation: Option[Explanation],
      resolution: Option[Resolution],
  )

  private def getErrorDocumentationAnnotations(error: ErrorCode): ErrorDocumentationAnnotations = {
    val mirroredType = runtimeMirror.reflect(error)
    val annotations: Seq[ru.Annotation] = mirroredType.symbol.annotations
    getErrorAnnotations(annotations)
  }

  private case class GroupDocumentationAnnotations(
      explanation: Option[Explanation]
  )

  private def getGroupDocumentationAnnotations(group: ErrorGroup): GroupDocumentationAnnotations = {
    val mirroredType = runtimeMirror.reflect(group)
    GroupDocumentationAnnotations(
      mirroredType.symbol.annotations
        .find { annotation =>
          isAnnotation(annotation, explanationTypeName)
        }
        .map { annotation =>
          Explanation(parseAnnotationValue(annotation.tree))
        }
    )
  }

  private case class GetAnnotationsState(
      deprecation: Option[DeprecatedDocs],
      explanation: Option[Explanation],
      resolution: Option[Resolution],
  )

  private def getErrorAnnotations(
      annotations: Seq[ru.Annotation]
  ): ErrorDocumentationAnnotations = {

    def update(
        state: GetAnnotationsState,
        updatedDeprecation: Option[String] = None,
        updatedExplanation: Option[String] = None,
        updatedResolution: Option[String] = None,
    ): GetAnnotationsState = {

      def updateString(
          existing: Option[String],
          updated: Option[String],
          designation: String,
      ): Option[String] =
        updated.fold(ifEmpty = existing) { value: String =>
          if (existing.isDefined)
            sys.error(s"Multiple $designation annotations detected")
          else
            Some(value)
        }

      val existingDeprecation = state.deprecation
      val updatedDeprecationString =
        updateString(existingDeprecation.map(_.deprecation), updatedDeprecation, "deprecation")
      val existingExplanation = state.explanation
      val updatedExplanationString =
        updateString(existingExplanation.map(_.explanation), updatedExplanation, "explanation")
      val existingResolution = state.resolution
      val updatedResolutionString =
        updateString(existingResolution.map(_.resolution), updatedResolution, "resolution")
      GetAnnotationsState(
        updatedDeprecationString.map(DeprecatedDocs),
        updatedExplanationString.map(Explanation),
        updatedResolutionString.map(Resolution),
      )
    }

    val doc = annotations.foldLeft(GetAnnotationsState(None, None, None)) {
      case (state, annotation) =>
        if (isAnnotation(annotation, deprecatedDocsTypeName))
          update(state, updatedDeprecation = Some(parseAnnotationValue(annotation.tree)))
        else if (isAnnotation(annotation, explanationTypeName))
          update(state, updatedExplanation = Some(parseAnnotationValue(annotation.tree)))
        else if (isAnnotation(annotation, resolutionTypeName))
          update(state, updatedResolution = Some(parseAnnotationValue(annotation.tree)))
        else if (isAnnotation(annotation, deprecatedTypeName)) {
          // Ignore @deprecate annotations
          // TODO Scala 2.13: Use these annotations to extract the docs description instead of the redundant [[DeprecatedDocs]]
          state
        } else
          sys.error(
            s"Unexpected annotation detected (${annotations.map(annotationTypeName)} but the only supported ones are $acceptedTypeNamesAnnotationsForErrorCodes)."
          )
    }

    ErrorDocumentationAnnotations(doc.deprecation, doc.explanation, doc.resolution)
  }

}

object ErrorCodeDocumentationGenerator {

  private val runtimeMirror: ru.Mirror = ru.runtimeMirror(getClass.getClassLoader)

  private val deprecatedTypeName = classOf[deprecated].getTypeName.replace("scala.", "")
  private val deprecatedDocsTypeName = classOf[DeprecatedDocs].getTypeName.replace("$", ".")
  private val explanationTypeName = classOf[Explanation].getTypeName.replace("$", ".")
  private val resolutionTypeName = classOf[Resolution].getTypeName.replace("$", ".")
  private val descriptionTypeName = classOf[Description].getTypeName.replace("$", ".")
  private val retryStrategyTypeName = classOf[RetryStrategy].getTypeName.replace("$", ".")

  private val acceptedTypeNamesAnnotationsForErrorCodes =
    Set(deprecatedTypeName, deprecatedDocsTypeName, explanationTypeName, resolutionTypeName)
  private val acceptedTypeNamesAnnotationsForErrorCategories =
    Set(descriptionTypeName, resolutionTypeName, retryStrategyTypeName)

  def getErrorCategoryAnnotations(errorCategory: ErrorCategory): ErrorCategoryAnnotations = {
    val mirroredType = runtimeMirror.reflect(errorCategory)
    val annotations: Seq[ru.Annotation] = mirroredType.symbol.annotations

    annotations.foldLeft(ErrorCategoryAnnotations.empty) { (acc, annotation) =>
      val typeName = annotationTypeName(annotation)
      if (acceptedTypeNamesAnnotationsForErrorCategories.contains(typeName)) {
        val parsedValue = Option(parseAnnotationValue(annotation.tree))
        if (typeName == descriptionTypeName) {
          require(acc.description.isEmpty, "Duplicate description found!")
          acc.copy(description = parsedValue)
        } else if (typeName == resolutionTypeName) {
          require(acc.resolution.isEmpty, "Duplicate resolution found!")
          acc.copy(resolution = parsedValue)
        } else if (typeName == retryStrategyTypeName) {
          require(acc.retryStrategy.isEmpty, "Duplicate retryStrategy found!")
          acc.copy(retryStrategy = parsedValue)
        } else {
          throw new IllegalStateException(s"Unexpected annotations of name: $typeName")
        }
      } else {
        acc
      }
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def parseAnnotationValue(tree: ru.Tree): String = {
    try {
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

  private def isAnnotation(annotation: ru.Annotation, typeName: String): Boolean =
    annotationTypeName(annotation) == typeName

  private def annotationTypeName(annotation: ru.Annotation) =
    annotation.tree.tpe.toString
}
