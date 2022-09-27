// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

case class UpdatePath(fieldPath: List[String]) {
  def toRawString: String = fieldPath.mkString(".")
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
    Right(UpdatePath(rawPath.split('.').toList))
  }

}
