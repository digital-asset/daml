// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

  /** A shell command that will be run using a scala.sys.process.Process. The command itself is run
    * within a shell via `/bin/sh -c` such that redirections (|, >, < etc..) work
    * @param cmd
    *   command to run
    * @param cwd
    *   current working directory: directory where to run the command
    * @param line
    *   line in the docs page at which the snippet is located
    * @param indent
    *   indentation string
    */
  final case class Shell(cmd: String, cwd: Option[String], line: Int, indent: String = "")
      extends SnippetStep {
    override def truncateOutput: Option[Int] = None
    override def appendToCmd(string: String): SnippetStep = this.copy(cmd = cmd + string)
  }
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

  /** Extract the value of a single parameter in a snippet command e.g: str = ..success(output=14)::
    * ... parameterKey = output will return "14"
    * @param str
    *   snippet command
    * @param parameterKey
    *   snippet parameter key to extract
    * @return
    */
  private def extractParameter(str: String, parameterKey: String): Option[String] = if (
    str.length < 2
  ) None
  else {
    // this piece of code is neither correct, nor checking really for errors
    // or anything like that, except, we'll just dump at least some error
    str
      .substring(1, str.length - 1)
      .split(",")
      .map(_.split("=").toList)
      .map {
        case `parameterKey` :: value :: Nil =>
          value.some

        case x =>
          throw new IllegalArgumentException(s"Invalid arguments ${x.toString} in $str")
      }
      .headOption
      .flatten
  }

  def parse(line: String, idx: Int): Option[SnippetStep] = Seq[Option[SnippetStep]](
    snippetSuccess.findFirstMatchIn(line).map { matched =>
      logger.debug(s"Matches success: $line")
      Success(
        matched.group(3).trim,
        extractParameter(matched.group(2), "output").map(_.toInt),
        idx,
        matched.group(1),
      )
    },
    snippetFailure.findFirstMatchIn(line).map { matched =>
      logger.debug(s"Matches failure: $line")
      Failure(
        matched.group(3).trim,
        extractParameter(matched.group(2), "output").map(_.toInt),
        idx,
        matched.group(1),
      )
    },
    snippetAssert.findFirstMatchIn(line).map { matched =>
      logger.debug(s"Matches assert: $line")
      Assert(matched.group(2).trim, idx, matched.group(1))
    },
    snippetHidden.findFirstMatchIn(line).map { matched =>
      logger.debug(s"Matches hidden: $line")
      Hidden(matched.group(2).trim, idx, matched.group(1))
    },
    snippetShell.findFirstMatchIn(line).map { matched =>
      logger.debug(s"Matches shell: $line")
      Shell(matched.group(3).trim, extractParameter(matched.group(2), "cwd"), idx, matched.group(1))
    },
  ).flatten.headOption

  private[docs] val snippetKey = "^\\s*\\.\\. snippet::(.*)$".r
  private[docs] val snippetSuccess = "^(\\s+)\\.\\. success(.*?)::(.*)$".r
  private[docs] val snippetShell = "^(\\s+)\\.\\. shell(.*?)::(.*)$".r
  private[docs] val snippetFailure = "^(\\s+)\\.\\. failure(.*?)::(.*)$".r
  private[docs] val snippetAssert = "^(\\s+)\\.\\. assert::(.*)$".r
  private[docs] val snippetHidden = "^(\\s+)\\.\\. hidden::(.*)$".r
  private[docs] val snippetInvalid = "^(\\s+)\\.\\.".r

  private[docs] val cmdStartsWithVal = "^\\s*val (.*?)=".r
}
