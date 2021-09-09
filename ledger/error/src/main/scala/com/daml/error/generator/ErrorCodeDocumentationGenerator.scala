package com.daml.error.generator

import java.lang.reflect.Modifier

import com.daml.error.ErrorCode
import com.daml.error.{Explanation, Resolution}
import org.reflections.Reflections

import scala.reflect.runtime.{universe => ru}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Var",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.TraversableOps",
  )
)
case class ErrorCodeDocumentationGenerator(prefix: String = "com.daml") {
  private val explanationTypeName = classOf[Explanation].getTypeName
  private val resolutionTypeName = classOf[Resolution].getTypeName

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

  private def getErrorCodeInstances = {
    val reflections = new Reflections(prefix)
    val errorCodeClasses = reflections.getSubTypesOf(classOf[ErrorCode])
    var errorCodes = Seq[ErrorCode]()
    // I had a hard time getting things to compile with map, so using this sort-of loop instead
    errorCodeClasses.forEach(c => {
      if (!Modifier.isAbstract(c.getModifiers)) {
        val instance: ErrorCode = c.getDeclaredField("MODULE$").get(c).asInstanceOf[ErrorCode]
        // scala compiler insists on val _ = ...
        errorCodes = errorCodes :+ instance
      }
    })
    errorCodes
  }

  private def isResolutionAnnotation(annotation: ru.Annotation): Boolean =
    annotation.tree.tpe.toString == resolutionTypeName

  private def isExplanationAnnotation(annotation: ru.Annotation): Boolean =
    annotation.tree.tpe.toString == explanationTypeName

  private def convertToDocItem(error: ErrorCode): DocItem = {
    val (expl, res) = getErrorNameAndAnnotations(error)
    DocItem(
      error.getClass.getName,
      error.category.getClass.getSimpleName,
      error.parent.docNames.filter(_ != ""),
      error.errorConveyanceDocString.getOrElse(""),
      error.id,
      expl,
      res,
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
            s"Did you rename the error code annotations `CantonError.Explanation` or `CantonError.Resolution`?" // TODO update error message
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
}
