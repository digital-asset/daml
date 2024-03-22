// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.test.evidence.tag

/** A functional test case.
  *
  * Captures a test for a functional requirement in addition to the non-functional requirements such as security or
  * reliability.
  *
  * TODO test evidencing: Refine data captured in a functional test case tag.
  *
  * @param topics high-level topics such as key requirements (e.g. no double spends, party migration)
  *               This can be officially stated key requirements or other important topics.
  * @param features features tested (e.g. roll sequencer keys, repair.add)
  */
final case class FuncTest(topics: Seq[String], features: Seq[String]) extends EvidenceTag
