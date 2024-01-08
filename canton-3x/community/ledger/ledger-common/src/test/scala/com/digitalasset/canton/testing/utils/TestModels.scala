// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.utils

// TODO(#12303) Path builders to test files generated in Daml SDK and copied to Canton
//              community/ledger/test-common/src/test/resources/test-models.
//              Replace these static paths and their referenced files once
//              the test file generators have been migrated to Canton.
object TestModels {
  // TODO(#12303) Dar file generated in Daml SDK with `bazel build //test-common:model-tests-2.1.build`
  //              and copied from `bazel-bin/test-common/model-tests-2.1.dar`.
  val com_daml_ledger_test_ModelTestDar_2_1_path = "test-models/model-tests-2.1.dar"
  val com_daml_ledger_test_BenchtoolTestDar_2_1_path = "test-models/benchtool-tests-2.1.dar"
  val com_daml_ledger_test_SemanticTestDar_2_1_path = "test-models/semantic-tests-2.1.dar"

  // TODO(#12303) Dar files generated in Daml SDK with `bazel build //daml-lf/encoder:testing-dar-<lf-version>`
  //              and copied from `bazel-bin/daml-lf/encoder/test-<lf-version>.dar`.
  def daml_lf_encoder_test_dar(lfVersion: String): String =
    s"test-models/daml-lf/encoder/test-$lfVersion.dar"
}
