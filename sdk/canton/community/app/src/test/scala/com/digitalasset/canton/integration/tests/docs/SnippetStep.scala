// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.docs

import cats.syntax.option.*
import com.typesafe.scalalogging.LazyLogging

sealed trait SnippetStep extends Product with Serializable {
  def line: Int

  // If defined, truncate the output to the specified number of lines
  def truncateOutput: Option[Int]

  // Append more content to the command
  def appendToCmd(string: String): SnippetStep

  // String of whitespaces / tabs of the snippet's indentation in the rst file
  def indent: String
}

object SnippetStep extends LazyLogging {

  final case class Success(cmd: String, truncateOutput: Option[Int], line: Int, indent: String = "")
      extends SnippetStep {
    override def appendToCmd(string: String): SnippetStep = this.copy(cmd = cmd + string)
  }
  final case class Failure(cmd: String, truncateOutput: Option[Int], line: Int, indent: String = "")
      extends SnippetStep {
    override def appendToCmd(string: String): SnippetStep = this.copy(cmd = cmd + string)
  }
  final case class Assert(cmd: String, line: Int, indent: String = "") extends SnippetStep {
    override def truncateOutput: Option[Int] = None
    override def appendToCmd(string: String): SnippetStep = this.copy(cmd = cmd + string)
  }
  final case class Hidden(cmd: String, line: Int, indent: String = "") extends SnippetStep {
    override def truncateOutput: Option[Int] = None
    override def appendToCmd(string: String): SnippetStep = this.copy(cmd = cmd + string)
  }

  private def extractOutputLines(str: String): Option[Int] = if (str.length < 2) None
  else {
    // this piece of code is neither correct, nor checking really for errors
    // or anything like that, except, we'll just dump at least some error
    str
      .substring(1, str.length - 1)
      .split(",")
      .map(_.split("=").toList)
      .map {
        case "output" :: value :: Nil =>
          value.toInt.some

        case x =>
          throw new IllegalArgumentException(s"Invalid arguments ${x.toString} in $str")
      }
      .headOption
      .flatten
  }

  def parse(line: String, idx: Int): Option[SnippetStep] = Seq[Option[SnippetStep]](
    snippetSuccess.findFirstMatchIn(line).map { matched =>
      logger.debug(s"Matches success: $line")
      Success(matched.group(3).trim, extractOutputLines(matched.group(2)), idx, matched.group(1))
    },
    snippetFailure.findFirstMatchIn(line).map { matched =>
      logger.debug(s"Matches failure: $line")
      Failure(matched.group(3).trim, extractOutputLines(matched.group(2)), idx, matched.group(1))
    },
    snippetAssert.findFirstMatchIn(line).map { matched =>
      logger.debug(s"Matches assert: $line")
      Assert(matched.group(2).trim, idx, matched.group(1))
    },
    snippetHidden.findFirstMatchIn(line).map { matched =>
      logger.debug(s"Matches hidden: $line")
      Hidden(matched.group(2).trim, idx, matched.group(1))
    },
  ).flatten.headOption

  private[docs] val snippetKey = "^\\s*\\.\\. snippet::(.*)$".r
  private[docs] val snippetSuccess = "^(\\s+)\\.\\. success(.*?)::(.*)$".r
  private[docs] val snippetFailure = "^(\\s+)\\.\\. failure(.*?)::(.*)$".r
  private[docs] val snippetAssert = "^(\\s+)\\.\\. assert::(.*)$".r
  private[docs] val snippetHidden = "^(\\s+)\\.\\. hidden::(.*)$".r
  private[docs] val snippetInvalid = "^(\\s+)\\.\\.".r

  private[docs] val cmdStartsWithVal = "^\\s*val (.*?)=".r
}
