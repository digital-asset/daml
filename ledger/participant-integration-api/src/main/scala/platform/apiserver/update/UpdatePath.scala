// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

case class UpdatePath(fieldPath: List[String], modifier: UpdatePathModifier) {
  def toRawString: String = fieldPath.mkString(".") + modifier.toRawString
}

object UpdatePath {

  def parseAll(rawPaths: Seq[String]): Result[Seq[UpdatePath]] = {
    val parsedPathsResult: Result[Seq[UpdatePath]] = rawPaths
      .map { rawPath: String =>
        UpdatePath.parseSingle(rawPath)
      }
      .foldLeft[Result[Seq[UpdatePath]]](Right(Seq.empty)) { (ax, next) =>
        for {
          a <- ax
          b <- next
        } yield {
          a :+ b
        }
      }
    parsedPathsResult
  }

  private[update] def parseSingle(rawPath: String): Result[UpdatePath] = {
    for {
      pathAndModifierO <- {
        rawPath.split('!') match {
          case Array() => Left(UpdatePathError.InvalidUpdatePathSyntax(rawPath))
          case Array(path) => Right((path, None))
          case Array(path, updateModifier) => Right((path, Some(updateModifier)))
          case _ => Left(UpdatePathError.InvalidUpdatePathSyntax(rawPath))
        }
      }
      (fieldPathRaw, modifierO) = pathAndModifierO
      _ <-
        if (fieldPathRaw.isEmpty) {
          Left(UpdatePathError.InvalidUpdatePathSyntax(rawPath))
        } else Right(())
      modifier <- {
        modifierO match {
          case None => Right(UpdatePathModifier.NoModifier)
          case Some("merge") => Right(UpdatePathModifier.Merge)
          case Some("replace") => Right(UpdatePathModifier.Replace)
          case _ => Left(UpdatePathError.UnknownUpdateModifier(rawPath))
        }
      }
    } yield {
      UpdatePath(
        fieldPathRaw.split('.').toList,
        modifier,
      )
    }
  }

}
