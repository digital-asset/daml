// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.utils

object TestModels {
  val com_daml_ledger_test_ModelTestDar_path = "model-tests-3.1.0.dar"
  val com_daml_ledger_test_SemanticTestDar_path = "semantic-tests-3.1.0.dar"

  // TODO(#12303) Dar files generated in Daml SDK with `bazel build //daml-lf/encoder:testing-dar-<lf-version>`
  //              and copied from `bazel-bin/daml-lf/encoder/test-<lf-version>.dar`.
  def daml_lf_encoder_test_dar(lfVersion: String): String =
    s"test-models/daml-lf/encoder/test-$lfVersion.dar"
}
