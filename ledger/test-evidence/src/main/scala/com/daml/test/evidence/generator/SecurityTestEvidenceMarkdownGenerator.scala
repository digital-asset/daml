// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.test.evidence.generator

import com.daml.test.evidence.tag.Security

import java.nio.file.{Files, Paths, StandardOpenOption}

object SecurityTestEvidenceMarkdownGenerator {
  private val GitHubSourceLinkTemplate = "https://github.com/digital-asset/daml/blob/%s/%s#L%d"

  private def genText(version: String): String = {
    val securityTestEntries = TestEntryLookup.securityTestEntries

    def scenarioDescription(scenario: Option[Security.HappyOrAttack]) = scenario match {
      case None => ""
      case Some(Right(attack)) =>
        s"""    :Attack Actor: ${attack.actor}
           |    :Attack Threat: ${attack.threat}
           |    :Attack Mitigation: ${attack.mitigation}
           |    """.stripMargin
      case Some(Left(happyCase)) =>
        s"    :Happy Case: ${happyCase.description}"
    }

    def singleEvidence[T, TS](
        version: String
    )(entry: TestEntry[Security.SecurityTest, TS]): String = {
      val url = GitHubSourceLinkTemplate.format(version, entry.tag.file, entry.tag.line)
      s"""`${entry.description} <$url>`_
         |${"-" * 300}
         |    :Asset: ${entry.tag.asset}
         |${scenarioDescription(entry.tag.scenario)}
         |""".stripMargin
    }

    val categories = securityTestEntries
      .groupBy(_.tag.property)
      .map { case (property, entries) =>
        val docItems = entries.groupBy(_.suiteName).map { case (suiteName, entries) =>
          val items = entries.map(singleEvidence(version))

          s"""
             |$suiteName
             |${"-" * 300}
             |${items.mkString("\n")}
             |""".stripMargin
        }

        s"""
           |$property
           |${"=" * 300}
           |${docItems.mkString("\n")}
           |""".stripMargin
      }

    s"${categories.mkString("\n")}"
  }

  def main(args: Array[String]): Unit = {
    if (args.length >= 1) {
      val outputFile = Paths.get(args(0))
      val version = if (args.length >= 2) {
        args(1)
      } else
        "main"
      val outputText = genText(version)
      Files.write(outputFile, outputText.getBytes, StandardOpenOption.CREATE_NEW): Unit
    } else {
      val outputText = genText("main")
      println(outputText)
    }
  }

}
