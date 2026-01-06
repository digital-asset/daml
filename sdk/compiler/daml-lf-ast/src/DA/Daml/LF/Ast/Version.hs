-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Ast.Version(
  module VER
) where

import DA.Daml.LF.Ast.Version.VersionType as VER
import DA.Daml.LF.Ast.Version.VersionUtil as VER

--generated modules

-- generated versions and features, based on versions defined in
-- //compiler/daml-lf/language/daml-lf.bzl

-- build //compiler/daml-lf-ast:generated_haskell_versions_src to find file in
-- bazel-out. We hide the concrete versions to push the usage of variables with
-- some kind of meaning (e.g. latestStableVersion) that can be changed in the
-- bazel file
import DA.Daml.LF.Ast.Version.GeneratedVersions as VER hiding (version2_1, version2_2, version2_dev)

-- build //compiler/daml-lf-ast:generated_haskell_features_src to find file in
-- bazel-out
import DA.Daml.LF.Ast.Version.GeneratedFeatures as VER
