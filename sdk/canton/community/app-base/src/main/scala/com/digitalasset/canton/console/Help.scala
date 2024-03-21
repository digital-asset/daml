// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import cats.syntax.functor.*
import com.digitalasset.canton.version.ProtocolVersion

import scala.annotation.StaticAnnotation
import scala.reflect.ClassTag
import scala.reflect.runtime.universe as ru

/** User friendly help messages generator.
  */
object Help {

  private val defaultTopLevelTopicStr = "Top-level Commands"
  val defaultTopLevelTopic = Seq(defaultTopLevelTopicStr)

  /** A short summary of the method (to be displayed in a list)
    *
    * Note that the annotation parser is also hard-coded to the default flag Stable
    */
  final case class Summary(s: String, flag: FeatureFlag = FeatureFlag.Stable)
      extends StaticAnnotation {
    override def toString: String = s
  }

  /** A longer description of the method */
  final case class Description(s: String) extends StaticAnnotation {
    override def toString: String = s
  }

  /** Indicates that a command is only available for domain running at least the specified protocolVersion. */
  final case class AvailableFrom(protocolVersion: ProtocolVersion) extends StaticAnnotation {
    override def toString: String = protocolVersion.toString
  }

  /** A sequence of strings classifying the method breadcrumb style (e.g. Seq("Participant", "Diagnostics")).
    * Used as headings in the displayed help.
    */
  final case class Topic(t: Seq[String]) extends StaticAnnotation {
    override def toString(): String = t.mkString(": ")
  }

  /** A tag to indicate nesting of items */
  final case class Group(name: String) extends StaticAnnotation {
    override def toString: String = name
  }

  final case class MethodSignature(argsWithTypes: Seq[(String, String)], retType: String) {
    val argString = "(" + argsWithTypes.map(arg => s"${arg._1}: ${arg._2}").mkString(", ") + ")"
    val retString = s": $retType"
    override def toString(): String = argString + retString
    def noUnits(): String =
      (if (argsWithTypes.isEmpty) "" else argString) +
        (if (retType == "Unit") "" else retString)
  }

  final case class Item(
      name: String,
      signature: Option[MethodSignature],
      summary: Summary,
      description: Description,
      topic: Topic,
      subItems: Seq[Item] = Seq.empty,
  )

  /** Generate help messages from an object instance using reflection, using only the given summaries.
    *
    * ARGUMENTS OF THE ANNOTATIONS MUST BE LITERALS (CONSTANTS) (e.g., Topic(topicVariable) does not work).
    *
    * All methods with a [[Summary]] annotation will be included. [[Description]] or [[Topic]]
    * are also included if present, and are set to the empty string otherwise.
    * We attempt to make as friendly as possible:
    *  - Unit types are not displayed
    *  - Empty argument lists are dropped
    *  - Implicits are hidden
    *  See corresponding tests for examples.
    */
  def forInstance[T: ClassTag](
      instance: T,
      baseTopic: Seq[String] = Seq(),
      scope: Set[FeatureFlag] = FeatureFlag.all,
  ): String = {
    // type extractor
    val items = getItems(instance, baseTopic, scope)
    format(items: _*)
  }

  def forMethod[T: ClassTag](
      instance: T,
      methodName: String,
      scope: Set[FeatureFlag] = FeatureFlag.all,
  ): String =
    forMethod(getItems(instance, scope = scope), methodName)

  def forMethod(items: Seq[Item], methodName: String): String = {
    def expand(item: Item): Seq[(String, Item)] = {
      (item.name, item) +: item.subItems.flatMap(expand).map { case (mn, itm) =>
        (item.name + "." + mn, itm)
      }
    }
    val expanded = items.flatMap(expand)
    val matching = expanded.filter { case (itemName, _) => itemName == methodName }
    if (matching.nonEmpty) {
      matching
        .map { case (itemName, item) =>
          formatItem(item.copy(name = itemName))
        }
        .mkString(System.lineSeparator())
    } else {
      val similarItems = expanded.map(_._1).filter(_.contains(methodName)).sorted.take(10)
      if (similarItems.isEmpty)
        s"Error: method $methodName not found; check your spelling"
      else {
        s"Error: method $methodName not found; are you looking for one of the following?\n  ${similarItems
            .mkString("\n  ")}"
      }
    }
  }

  def formatItem(item: Item): String = item match {
    case Item(name, optSignature, summary, description, topic, group) =>
      val sigString = optSignature.map(_.noUnits()).getOrElse("")
      val text = if (description.s.nonEmpty) description.toString else summary.toString
      Seq(name + sigString, text).mkString(System.lineSeparator)
  }

  private def extractItem(
      mirror: ru.Mirror,
      member: ru.Symbol,
      baseTopic: Seq[String],
      scope: Set[FeatureFlag],
  ): Option[Item] =
    memberDescription(member)
      .filter { case (summary, _, _, _) => scope.contains(summary.flag) }
      .map { case (summary, description, topic, group) =>
        val methodName = member.name.toString
        val info = member.info
        val name = methodName
        val myTopic = Topic(baseTopic ++ topic.t ++ group.toList)
        val summaries = member.typeSignature.members
          .flatMap(s =>
            extractItem(mirror, s, myTopic.t, scope).toList
          ) // filter to members that have `@Help.Summary` applied
        val signature = Some(methodSignature(info))
        val topicOrDefault =
          if (summaries.isEmpty && topic.t.isEmpty && group.isEmpty)
            Topic(baseTopic ++ defaultTopLevelTopic)
          else myTopic
        Item(name, signature, summary, description, topicOrDefault, summaries.toSeq)
      }

  def getItems[T: ClassTag](
      instance: T,
      baseTopic: Seq[String] = Seq(),
      scope: Set[FeatureFlag] = FeatureFlag.all,
  ): Seq[Item] = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val mirroredType = mirror.reflect(instance)
    mirroredType.symbol.typeSignature.members
      .flatMap(m => extractItem(mirror, m, baseTopic, scope).toList)
      .toSeq
  }

  def flattenItem(path: Seq[String])(item: Item): Seq[Item] = {
    val newPath = path :+ item.name
    item.copy(name = newPath.mkString(".")) +: item.subItems.flatMap(flattenItem(newPath))
  }

  def flattenItemsForManual(items: Seq[Item]): Seq[Item] =
    items
      .flatMap(flattenItem(Seq()))
      .filter(_.subItems.isEmpty)
      // strip trailing default topic such that we don't have to add "Top-level Commands" to every group in the manual
      .map { itm =>
        itm.copy(
          topic =
            if (
              itm.topic.t.lengthCompare(1) > 0 && itm.topic.t.lastOption
                .contains(defaultTopLevelTopicStr)
            )
              Topic(itm.topic.t.take(itm.topic.t.length - 1))
            else itm.topic
        )
      }

  def getItemsFlattenedForManual[T: ClassTag](
      instance: T,
      baseTopic: Seq[String] = Seq(),
  ): Seq[Item] =
    flattenItemsForManual(getItems(instance, baseTopic))

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def methodSignature[T](typ: ru.Type): MethodSignature = {
    val methodType = typ.asInstanceOf[ru.TypeApi]

    def excludeImplicits(symbols: List[ru.Symbol]): List[ru.Symbol] =
      symbols.filter(!_.isImplicit)

    def until[U](p: U => Boolean, f: U => U)(x: U): U =
      if (p(x)) x else until(p, f)(f(x))

    val args =
      excludeImplicits(methodType.paramLists.flatten).map(symb =>
        (symb.name.toString, symb.typeSignature.toString)
      )
    // return types can contain implicit parameter lists; ensure that these are excluded
    val returnType =
      until[ru.Type](typ => !typ.paramLists.flatten.exists(_.isImplicit), _.resultType)(
        methodType.resultType
      )
    MethodSignature(args, returnType.toString)
  }

  def fromObject[T: ClassTag](instance: T): Seq[Item] =
    getItems(instance)

  /** Format help for named items and their descriptions
    * @param items Tuple of name and description
    */
  def format(items: Item*): String = {
    def underline(s: String) = s + System.lineSeparator + "-" * s.length + System.lineSeparator()
    val grouped = items
      .filter(_.subItems.nonEmpty)
      .sortBy(_.name)
      .map { case Item(name, signature, Summary(summary, flag), description, topic, _) =>
        s"$name - $summary"
      }
      .toList

    val topLevel = items
      .filter(_.subItems.isEmpty)
      .groupBy(_.topic)
      .fmap(descs =>
        descs
          .sortBy(_.name) // sort alphabetically
          .map { case Item(name, signature, Summary(summary, flag), description, topic, _) =>
            s"$name - $summary"
          }
          .mkString(System.lineSeparator)
      )
      .toList
      .sortWith { (x, y) =>
        (x._1.t, y._1.t) match {
          case (`defaultTopLevelTopic`, _) => true
          case (_, `defaultTopLevelTopic`) => false
          case (lft, rght) => lft.mkString(".") < rght.mkString(".")
        }
      }
      .map({ case (tobj @ Topic(topic), h) =>
        (if (topic.nonEmpty) underline(tobj.toString) else "") + h
      })
      .mkString(System.lineSeparator + System.lineSeparator)
    if (grouped.nonEmpty) {
      topLevel + System.lineSeparator + System.lineSeparator + underline("Command Groups") + grouped
        .mkString(System.lineSeparator)
    } else topLevel
  }

  private def memberDescription(
      member: ru.Symbol
  ): Option[(Summary, Description, Topic, Option[String])] = {
    (
      member.annotations.map(fromAnnotation(_, summaryParser)).collect { case Some(s) => s },
      member.annotations.map(fromAnnotation(_, descriptionParser)).collect { case Some(s) => s },
      member.annotations.map(fromAnnotation(_, tagParser)).collect { case Some(s) => s },
      member.annotations.map(fromAnnotation(_, groupParser)).collect { case Some(s) => s },
    ) match {
      case (Nil, _, _, _) => None
      case (summary :: _sums, l2, l3, g4) =>
        Some(
          (
            summary,
            l2.headOption.getOrElse(Description("")),
            l3.headOption.getOrElse(Topic(Seq())),
            g4.headOption.map(_.name),
          )
        )
    }
  }

  /** The following definitions (fromAnnotation and Xparser) are quite nasty.
    * They hackily reconstruct the Scala values from annotations, which contain ASTs.
    * It's possible to use a reflection Toolbox to eval the annotations, but this is very slow (as it entails compiling the ASTs
    * from the annotations) and results in large latencies for the help command.
    */
  private def fromAnnotation[T: ru.TypeTag](
      annotation: ru.Annotation,
      parser: ru.Tree => T,
  ): Option[T] = {
    if (annotation.tree.tpe.typeSymbol == ru.typeOf[T].typeSymbol) {
      Some(parser(annotation.tree))
    } else None
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def grabStringTag(tree: ru.Tree): String =
    tree
      .children(1)
      .asInstanceOf[ru.Literal]
      .value
      .value
      .asInstanceOf[String]

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def summaryParser(tree: ru.Tree): Summary = {
    def grabFeatureFlagFromSummary(tree: ru.Tree): FeatureFlag =
      if (tree.children.lengthCompare(2) > 0) {
        val tmp = tree.children(2).asInstanceOf[ru.Select]
        if (tmp.symbol.isModule) {
          reflect.runtime.currentMirror
            .reflectModule(tmp.symbol.asModule)
            .instance
            .asInstanceOf[FeatureFlag]
        } else FeatureFlag.Stable
      } else FeatureFlag.Stable
    Summary(grabStringTag(tree), grabFeatureFlagFromSummary(tree))
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def descriptionParser(tree: ru.Tree): Description = {
    try {
      Description(grabStringTag(tree).stripMargin)
    } catch {
      case x: RuntimeException =>
        // leave a comment for the poor developer that might run into the same issue ...
        println(
          "Failed to process description (description needs to be a string. i.e. don't apply stripmargin here ...): " + tree.toString
        )
        throw x
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def tagParser(tree: ru.Tree): Topic = {
    val args = tree
      .children(1)
      .children
      .drop(1)
      .map(l => l.asInstanceOf[ru.Literal].value.value.asInstanceOf[String])
    Topic(args)
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def groupParser(tree: ru.Tree): Group = Group(grabStringTag(tree))

}
