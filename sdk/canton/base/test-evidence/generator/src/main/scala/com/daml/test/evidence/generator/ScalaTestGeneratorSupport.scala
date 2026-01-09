// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.test.evidence.generator

import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import com.daml.test.evidence.scalatest.JsonCodec.*
import com.daml.test.evidence.tag.EvidenceTag
import io.circe.parser.decode
import org.scalatest.Suite
import org.scalatest.daml.ScalaTestAdapter

import scala.reflect.ClassTag

object ScalaTestGeneratorSupport {

  private def testNameWithTags(tags: Map[String, Set[String]]): List[(String, List[EvidenceTag])] =
    tags.fmap { tagNames =>
      tagNames.toList
        .filter(_.startsWith("{")) // Check if we have a JSON encoded tag
        .traverse(decode[EvidenceTag])
        .valueOr(err => sys.error(s"Failed to parse JSON tag: $err"))
    }.toList

  private def isIgnored(suite: Suite, testName: String): Boolean =
    suite.tags.getOrElse(testName, Set()).contains(ScalaTestAdapter.IgnoreTagName)

  def testEntries[TT: ClassTag, TS: ClassTag, TE](
      suites: List[Suite],
      testEntry: (String, String, TT, Boolean, Option[TS]) => TE,
  ): List[TE] =
    suites.flatMap { suite =>
      val testSuite = suite match {
        case testSuite: TS => Some(testSuite)
        case _ => None
      }

      testNameWithTags(suite.tags).mapFilter { case (testName, testTags) =>
        testTags.collectFirst { case testTag: TT =>
          testEntry(suite.suiteName, testName, testTag, isIgnored(suite, testName), testSuite)
        }
      }
    }
}
