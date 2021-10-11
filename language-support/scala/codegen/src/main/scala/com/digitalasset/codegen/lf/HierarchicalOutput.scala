// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen.lf

import com.daml.{codegen => parent}
import parent.Util
import parent.types.Namespace
import parent.exception.UnsupportedDamlTypeException

import java.io._
import scala.reflect.runtime.universe._

import com.daml.lf.data.Ref.Identifier
import scalaz.{Tree => _, _}
import scalaz.std.tuple._
import scalaz.std.vector._
import scalaz.syntax.bifunctor._
import scalaz.syntax.monoid._
import scalaz.syntax.traverse._

/** Functions related to the folding of the tree of templates and types. */
private[codegen] object HierarchicalOutput {
  // the argument is whether 'here' is contained within a companion
  private[this] type Rec = Boolean => RecOut
  private[this] type RecOut = (Vector[String], SingleOr[(File, Set[Tree], Iterable[Tree])])

  // Generally in this file, left means "single file, already incorporates
  // immediate parent name" and right means "have not incorporated immediate
  // parent name".  This ordering is most convenient because the monoid's zero
  // is correct for us.
  private[this] type SingleOr[A] = A \/ Vector[A]

  // Rec minus the left case (which is only needed when considering a fold step)
  type ErrorsAndFiles[E, F] = (Vector[E], Vector[(F, Iterable[Tree])])

  type TemplateOrDatatype =
    (Identifier, DefTemplateWithRecord.FWT \/ DamlDataTypeGen.DataType)

  /** Pull up each `Rec` into the companion implied, or not, by the keys. */
  private[this] def liftSubtrees[S, F](
      subtrees: String ==>> (Boolean => (Vector[S], SingleOr[(F, Set[Tree], Iterable[Tree])]))
  )(inCompanion: Boolean): (Vector[S], Vector[(F, Set[Tree], Iterable[Tree])]) =
    subtrees.map(_(inCompanion)).foldMapWithKey { case (k, (errs, file)) =>
      (
        errs,
        file match {
          case -\/(single) => Vector(single)
          case \/-(multiple @ Vector((firstFile, _, _), _*)) =>
            if (inCompanion)
              Vector(
                (
                  firstFile,
                  (multiple.map(_._2) foldLeft Set.empty[Tree])(_ | _),
                  Iterable(q"""object ${TermName(k)} {
                                ..${multiple flatMap (_._3)}
                              }"""),
                )
              )
            else multiple map (_ map (contents => Iterable(q"""package ${TermName(k)} {
                                                                 ..$contents
                                                               }""")))
          case \/-(_) => Vector()
        },
      )
    }

  def discoverFiles(
      treeified: Namespace[String, Option[TemplateOrDatatype]],
      util: LFUtil,
  ): ErrorsAndFiles[String, File] =
    treeified
      .foldTreeStrict[Rec] {
        case (None, subtrees) =>
          (liftSubtrees(subtrees) _).andThen(_.map(\/-(_)))
        case (Some(codeGenElt), subtrees) =>
          val (subErrs, subFiles) = liftSubtrees(subtrees)(true)
          val companionMembers = subFiles flatMap (_._3)

          val (generate, log @ _, errorMsg) = codeGenElt match {
            case (templateId, -\/(templateInterface)) =>
              (
                () =>
                  DamlContractTemplateGen.generate(
                    util,
                    templateId,
                    templateInterface,
                    companionMembers,
                  ),
                s"Writing template for $templateId",
                s"Cannot generate Scala code for template $templateId",
              )

            case (name, \/-(ntd)) =>
              (
                () => DamlDataTypeGen.generate(util, ntd, companionMembers),
                s"Writing type declaration for $name",
                s"Cannot generate Scala code for type declaration with name $name",
              )
          }

          val errorsImportsAndFile: RecOut =
            try { (subErrs, -\/(generate())) }
            catch {
              case e: UnsupportedDamlTypeException =>
                (subErrs :+ s"$errorMsg because: ${e.getLocalizedMessage}", \/-(subFiles))
            }

          _ => errorsImportsAndFile
      }
      .apply(false)
      .rightMap(_.valueOr(Vector(_)).map { case (err, imports, subTrees) =>
        err -> standardTopFileBehavior(util)(imports, subTrees)
      })

  private[this] def standardTopFileBehavior(
      util: LFUtil
  )(imports: Set[Tree], trees: Iterable[Tree]): Iterable[Tree] =
    imports.toSeq :+ q"""
      package ${Util.packageNameToRefTree(util.packageName)} {
        ..$trees
      }"""

  implicit final class `scalaz ==>> future`[A, B](private val self: A ==>> B) extends AnyVal {
    // added (more efficiently) in scalaz 7.3
    def foldMapWithKey[C: Monoid](f: (A, B) => C): C =
      self.foldlWithKey(mzero[C])((c, a, b) => c |+| f(a, b))
  }
}
