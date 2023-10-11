// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricDoc.MetricQualification.*
import com.daml.metrics.api.MetricDoc.*
import com.daml.metrics.api.MetricHandle as DamlMetricHandle

import scala.annotation.nowarn
import scala.reflect.ClassTag
import scala.reflect.runtime.universe as ru

object MetricDoc {

  final case class GroupInfo(
      instances: Seq[String],
      fullNames: Seq[String],
  )

  final case class Item(
      tag: Tag,
      name: String,
      metricType: String,
      groupingInfo: Option[GroupInfo] = None,
  )

  private def toItem(tags: Seq[Tag], groupTags: Seq[GroupTag], x: DamlMetricHandle): Option[Item] =
    (tags, groupTags) match {
      case (List(tag), List()) => Some(Item(tag = tag, name = x.name, metricType = x.metricType))
      case (List(tag), _) => Some(groupTagToItem(tag, groupTags, x))
      case _ => None
    }

  private def fromFanTag(
      tags: Seq[FanInstanceTag],
      fanTags: Seq[FanTag],
      x: DamlMetricHandle,
  ): Option[Item] =
    (tags, fanTags) match {
      case (List(_), List(fanTag)) =>
        val representative = fanTag.representative
        Some(
          Item(
            tag = Tag(
              summary = fanTag.summary,
              description = fanTag.description,
              qualification = fanTag.qualification,
              labelsWithDescription =
                Map.empty, // fan tags are the legacy way of representing labels
            ),
            name = representative,
            metricType = x.metricType,
            groupingInfo = Some(
              GroupInfo(
                instances = Seq(x.name.substring(representative.indexOf('<'))),
                fullNames = Seq(x.name),
              )
            ),
          )
        )
      case _ => None
    }

  // Converts a Tag accompanied with a GroupTag to a MetricDoc.Item. If the metric's name matches
  // one of the representatives then the item's name is set to be the corresponding representative.
  // The instance is set to equal the part of the metric's name replaced by the wildcard.
  // Otherwise, the item is constructed as if the GroupTag was missing.
  private def groupTagToItem(tag: Tag, groupTags: Seq[GroupTag], x: DamlMetricHandle): Item = {
    val wildcard = "<.*>".r // wildcard must be inside angle brackets (<,>)
    val matchingRepresentative = groupTags
      .map(_.representative)
      .find(representative => {
        val escaped = representative.replace(".", "\\.")
        val pattern = wildcard.replaceFirstIn(escaped, ".*")
        // check if the pattern exists in the name and is a prefix
        pattern.r.findFirstIn(x.name).fold(false)(x.name.startsWith(_))
      })
    matchingRepresentative match {
      case None => Item(tag = tag, name = x.name, metricType = x.metricType, groupingInfo = None)
      case Some(representative) => {
        // representative lacks the suffix of the metrics' name and it should be appended
        val commonPrefixLength = x.name.zip(representative).takeWhile(Function.tupled(_ == _)).size
        // the instance is after the common prefix and until the first dot (.) encountered
        val instance = x.name.drop(commonPrefixLength).takeWhile(_ != '.')
        // extend the representative by the name of the specific metric
        val suffixName = x.name.substring(commonPrefixLength + instance.size)
        val representativeExtended =
          representative.substring(0, representative.indexOf('>') + 1) + suffixName
        Item(
          tag = tag,
          name = representativeExtended,
          metricType = x.metricType,
          groupingInfo = Some(
            GroupInfo(
              instances = Seq(instance),
              fullNames = Seq(x.name),
            )
          ),
        )
      }
    }
  }

  // ignore scala / java packages
  private val ignorePackages = Seq("scala.", "java.")
  private def includeSymbol(symbol: ru.Symbol): Boolean =
    !ignorePackages.exists(symbol.fullName.startsWith)

  // Deduplicate Items that have the same name and collect the instances of those into one item
  private def group(grouped: Seq[Item]): Seq[Item] =
    grouped
      .groupBy(_.name)
      .values
      .map((s: Seq[Item]) =>
        s.headOption
          .map(
            _.copy(groupingInfo =
              Some(
                GroupInfo(
                  instances = s.map(_.groupingInfo).flatMap(_.toList).flatMap(_.instances).distinct,
                  fullNames = s.map(_.groupingInfo).flatMap(_.toList).flatMap(_.fullNames).distinct,
                )
              )
            )
          )
      )
      .flatMap(_.toList)
      .toSeq

  /** Get MetricDoc.Tag annotated metrics from any instance in a nested way
    *
    * NOTE: does not support lazy val metrics
    */
  def getItems[T: ClassTag](instance: T): Seq[Item] = {
    val (unique, groupable) = getItemsAll[T](instance, Seq()).partition(_.groupingInfo.isEmpty)
    val grouped = group(groupable)
    // remove the items that are grouped already
    val fullNames = grouped.map(_.groupingInfo).flatMap(_.toList).flatMap(_.fullNames).toSet
    val ungrouped = unique.filterNot(fullNames contains _.name)
    ungrouped ++ grouped
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def getItemsAll[T: ClassTag](
      instance: T,
      inheritedGroupTags: Seq[GroupTag],
  ): Seq[Item] = {

    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val mirroredType = mirror.reflect(instance)

    // baseClasses includes the entire dependency path, therefore we need to filter that out as otherwise, we get infinite loops
    val symbols = mirroredType.symbol.baseClasses.filter(includeSymbol).toSet
    symbols.toSeq.flatMap(symbol => {
      val classGroupTags =
        if (symbol.isClass) extractTag(symbol.annotations, groupTagParser) else Seq()
      val fanTags = {
        if (symbol.isClass) extractTag(symbol.annotations, fanTagParser) else Seq()
      }
      symbol.typeSignature.members.flatMap { m =>
        // do not pick methods
        if (m.isMethod) {
          Seq()
        } else if (m.isModule) {
          // descend into objects nested in classes (which appear as modules)
          val ts = m.asInstanceOf[ru.ModuleSymbol]
          val fm = mirroredType.reflectModule(ts)
          // pass the group tags of the object deeper in the hierarchy
          getItemsAll(fm.instance, inheritedGroupTags ++ classGroupTags)
        } else if (m.isTerm && !m.isJava) {
          val ts = m.asInstanceOf[ru.TermSymbol]
          // let's just skip java collections
          if (includeSymbol(ts)) {
            try {
              val rf = mirroredType.reflectField(ts)
              // ignore java symbols
              if (rf.symbol.isJava) {
                Seq()
              } else {
                rf.get match {
                  // if it is a metric handle, try to grab the annotation and the name
                  case x: DamlMetricHandle =>
                    val tag = extractTag(rf.symbol.annotations, tagParser)
                    if (tag.isEmpty) {
                      // if there is no Tag check if there exists a MetricDoc.FanInstanceTag
                      val fanInstanceTag = extractTag(rf.symbol.annotations, _ => FanInstanceTag())
                      fromFanTag(fanInstanceTag, fanTags, x).toList
                    } else {
                      val groupTags =
                        extractTag(rf.symbol.annotations, groupTagParser)
                      val allGroupTags = inheritedGroupTags ++ classGroupTags ++ groupTags
                      // collect the group tags that are relevant to the groupable class
                      val groupTagsMatching =
                        if (symbol.isClass)
                          allGroupTags.filter(gt =>
                            gt.groupableClass == mirror.runtimeClass(symbol.asClass)
                          )
                        else Seq()
                      toItem(tag, groupTagsMatching, x).toList
                    }
                  // otherwise, continue scanning for metrics
                  case _ =>
                    val fm = rf.get
                    getItemsAll(fm, inheritedGroupTags ++ classGroupTags)
                }
              }
            } catch {
              // this is dirty, but we'll get quite a few reflection and class loader errors
              // just by scanning our objects, and i haven't figured out a way to prevent these to
              // happen ...
              case _: Throwable => Seq()
            }
          } else Seq()
        } else Seq()
      }.toSeq
    })
  }

  private def extractTag[T: ClassTag, S: ru.TypeTag](
      annotations: Seq[ru.Annotation],
      tagParser: ru.Tree => S,
  ): Seq[S] = {
    val filtered = annotations.map(fromAnnotation[S](_, tagParser)).collect({ case Some(s) => s })
    filtered match {
      case a :: b :: rest =>
        a match {
          case a: Tag => throw new IllegalArgumentException(s"Multiple tags observed! $filtered")
          case _ => filtered
        }
      case _ => filtered
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def getString(tree: ru.Tree) =
    tree.asInstanceOf[ru.Literal].value.value.asInstanceOf[String]

  private def getQualification(tree: ru.Tree): MetricQualification = {
    val typeOfQualification: ru.Type = tree.tpe
    typeOfQualification match {
      case q if q =:= ru.typeOf[Latency.type] => Latency
      case q if q =:= ru.typeOf[Traffic.type] => Traffic
      case q if q =:= ru.typeOf[Errors.type] => Errors
      case q if q =:= ru.typeOf[Saturation.type] => Saturation
      case q if q =:= ru.typeOf[Debug.type] => Debug
      case _ => throw new IllegalStateException("Unreachable code.")
    }
  }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Product",
      "org.wartremover.warts.Serializable",
    )
  )
  private def tagParser(tree: ru.Tree): Tag = {
    try {
      Seq(1, 2).map(pos => getString(tree.children(pos))) match {
        case s :: d :: Nil =>
          val q = getQualification(tree.children(3))
          val labels: Map[String, String] =
            if (tree.children.size >= 4) {
              getLabels(tree.children(4))
            } else {
              Map.empty
            }
          Tag(
            summary = s,
            description = d.stripMargin,
            qualification = q,
            labelsWithDescription = labels,
          )
        case _ => throw new IllegalStateException("Unreachable code.")
      }
    } catch {
      case x: RuntimeException =>
        println(
          """Failed to process Tag annotation:
            |summary and description need to be constant-string, i.e. don't apply stripmargin here ...).
            |MetricQualification must be an object of MetricQualification and labels must be a Map of keys to description:
            |""".stripMargin + ru.showRaw(tree)
        )
        println(s"Error: $x")
        throw x
    }
  }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf"
    )
  )
  @nowarn("msg=unchecked|cannot be checked at run time")
  private def getLabels(labelsChild: ru.Tree) = {
    def getConstantsFromTree(tree: ru.Tree): List[String] = {
      tree match {
        case ru.Constant(constant: String) => constant :: Nil
        case ru.Literal(ru.Constant(constant: String)) => constant :: Nil
        case _ => tree.children.flatMap(getConstantsFromTree)
      }
    }

    // The workaround is for labels declared as Map("key" -> "value", "key2" -> "value2") and so on.
    // The Tree for the given expression becomes too complex to parse, but with a depth first traverse
    // we can collect the constant strings in the order of appearance and re-construct the label map
    val constants = getConstantsFromTree(labelsChild)
    constants
      .grouped(2)
      .map {
        case key :: value :: Nil => key -> value
        case _ => throw new IllegalArgumentException(s"Cannot build labels from $constants")
      }
      .toMap
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def groupTagParser(tree: ru.Tree): GroupTag = {
    def getRuntimeClass(tree: ru.Tree) = {
      val gcClassSymbol =
        tree.asInstanceOf[ru.Literal].value.tpe.typeArgs(0).typeSymbol.asClass
      val mirror = ru.runtimeMirror(getClass.getClassLoader)
      mirror.runtimeClass(gcClassSymbol)
    }
    try {
      val representative = getString(tree.children(1))
      val groupableClass = getRuntimeClass(tree.children(2))
      GroupTag(representative = representative, groupableClass = groupableClass)
    } catch {
      case x: RuntimeException =>
        println(
          """Failed to process GroupTag annotation:
            |representative needs to be a constant-string, i.e. don't apply stripmargin here ...),
            |and groupableClass must be defined as classOf[CLASSNAME]:
            |""".stripMargin + tree.toString
        )
        throw x
    }
  }

  private def fanTagParser(tree: ru.Tree): FanTag = {
    try {
      Seq(1, 2, 3).map(pos => getString(tree.children(pos))) match {
        case representative :: summary :: description :: Nil =>
          val qualification = getQualification(tree.children(4))
          FanTag(
            representative = representative,
            summary = summary,
            description = description.stripMargin,
            qualification = qualification,
          )
        case _ => throw new IllegalStateException("Unreachable code.")
      }

    } catch {
      case x: RuntimeException =>
        println(
          """Failed to process FanTag annotation:
            |representative, summary and description need to be constant-string, i.e. don't apply stripmargin here ...),
            |and MetricQualification must be an object of MetricQualification:
            |""".stripMargin + tree.toString
        )
        throw x
    }
  }

  private def fromAnnotation[T: ru.TypeTag](
      annotation: ru.Annotation,
      parser: ru.Tree => T,
  ): Option[T] = {
    if (annotation.tree.tpe.typeSymbol == ru.typeOf[T].typeSymbol) {
      Some(parser(annotation.tree))
    } else None
  }

}
