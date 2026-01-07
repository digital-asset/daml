// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.docs

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.typesafe.scalalogging.LazyLogging

final case class SnippetScenario(name: String, steps: Seq[Seq[SnippetStep]])

final case class SnippetStepResult(step: SnippetStep, output: Seq[String]) extends LazyLogging {

  def prependAtSign: Boolean = step match {
    // Don't prepend the @ sign in the code block for shell scripts
    case _: SnippetStep.Shell => false
    case _ => true
  }

  def language: String = step match {
    case _: SnippetStep.Shell => "shell"
    case _ => "none"
  }

  /** prepare the output for our sphinx document
    *
    * @return
    *   the indentation string, the command that should be written into the manual and the output,
    *   truncated to the requested number of lines
    */
  def truncatedOutput: (String, String, String) = {
    val cmd = step match {
      case SnippetStep.Success(cmd, _, _, _) => cmd
      case SnippetStep.Failure(cmd, _, _, _) => cmd
      case SnippetStep.Assert(_, _, _) => ""
      case SnippetStep.Hidden(_, _, _) => ""
      case SnippetStep.Shell(cmd, _, _, _) => cmd
    }
    val tmp = output.flatMap(_.split('\n').toList)

    step.truncateOutput match {
      case Some(maxLines) =>
        val truncatedOutput =
          if (tmp.sizeIs > maxLines) tmp.take(maxLines) :+ ".."
          else tmp

        (step.indent, cmd, truncatedOutput.mkString("\n"))

      case None => (step.indent, cmd, tmp.mkString("\n"))
    }
  }
}

object SnippetScenario extends LazyLogging {

  def parse(lines: Seq[String]): Seq[SnippetScenario] = {

    import SnippetStep.*

    trait Result {

      /** Accumulate all the commands to run
        *   - The key is the environment name (e.g., getting_started)
        *   - The value is the sequence of snippet occurrences; while each snippet is a sequence of
        *     snippet steps to be run
        */
      def pending: Map[String, Seq[Seq[SnippetStep]]]

      def update(line: String, idx: Int): Result

      def result: Seq[SnippetScenario] =
        pending.map { case (name, steps) => SnippetScenario(name, steps) }.toSeq
    }

    /** @param name
      *   Name of the environment
      */
    final case class InSnippet(name: String, pending: Map[String, Seq[Seq[SnippetStep]]])
        extends Result {
      def update(line: String, idx: Int): Result =
        if (line.isBlank) {
          // reset snippet context
          exitSnippet(idx)
        } else {
          snippetKey.findFirstMatchIn(line).foreach { matched =>
            logger.debug(s"Matches snippet $line")
            throw new IllegalArgumentException(
              s"Found snippet ${matched.group(1).trim()} but already in snippet $name"
            )
          }

          // not snippet command, let's see if it is step
          SnippetStep.parse(line, idx) match {
            case Some(step) => // valid step command in a valid context
              logger.debug(s"Found step $step")
              copy(pending = pending + (name -> {
                val seqs = pending.getOrElse(name, Seq(Seq()))
                if (seqs.isEmpty) Seq(Seq(step))
                else {
                  val (allButLastSeq, lastSeq) = (seqs.init, seqs.lastOption.getOrElse(Seq()))
                  val steps = lastSeq :+ step
                  val seq = allButLastSeq :+ steps
                  seq
                }
              }))

            case None => // Not a step
              // Make sure it is not some unrecognized command (e.g., `  .. unknownKeyword::`)
              snippetInvalid.findFirstIn(line).foreach { _ =>
                throw new IllegalArgumentException(
                  s"Invalid command within snippet $name on $idx: $line"
                )
              }

              /*
              This is now handled as a multi-line snippet, which means that the content
              will be appended to the last step of the same environment.
               */
              val updatedSteps = appendToLastStep(line).valueOr { err =>
                throw new IllegalArgumentException(err)
              }
              copy(pending = pending + (name -> updatedSteps))
          }
        }

      /** Append line to last steps of the current environment. Fails if there are no steps for the
        * current environment.
        */
      private def appendToLastStep(line: String): Either[String, Seq[Seq[SnippetStep]]] =
        for {
          steps <- pending
            .get(name)
            .toRight(
              s"""
                  |Unable to find steps for environment $name.
                  |Snippet environment should start with a command."""
            )

          stepsNE <- NonEmpty
            .from(steps)
            .toRight(s"""
              |Found empty steps for environment $name.
              |Ensure multi-line snippet start in the same line as the command.
              |""".stripMargin)
        } yield {
          val lastStep = stepsNE.last1
          val updatedStep = lastStep.last.appendToCmd("\n" + line)
          steps match {
            case allButLastSeq :+ (allButLastStep :+ _currentLastStep) =>
              allButLastSeq :+ (allButLastStep :+ updatedStep)
            case _ => steps
          }
        }

      private def exitSnippet(line: Int): OutsideSnippet = {
        logger.debug(s"Exiting snippet at $line")
        OutsideSnippet(pending)
      }
    }

    final case class OutsideSnippet(pending: Map[String, Seq[Seq[SnippetStep]]]) extends Result {
      def update(line: String, idx: Int): Result =
        if (line.isBlank) this
        else {
          // adjust snippet if there is one
          snippetKey
            .findFirstMatchIn(line)
            .fold[Result] {
              // not snippet command, let's see if it is step
              SnippetStep.parse(line, idx).foreach { _ =>
                throw new IllegalArgumentException(s"Command out of context on $idx: $line")
              }

              this
            } { matched =>
              logger.debug(s"Matches snippet $line")
              enterSnippet(matched.group(1).trim())
            }
        }

      def enterSnippet(name: String): InSnippet = {
        val prepNextSnippet = pending.updated(name, pending.getOrElse(name, Seq()) :+ Seq())
        InSnippet(name, prepNextSnippet)
      }
    }

    lines.zipWithIndex
      .foldLeft[Result](OutsideSnippet(Map())) { case (acc, (line, idx)) =>
        acc.update(line, idx)
      }
      .result
  }
}
