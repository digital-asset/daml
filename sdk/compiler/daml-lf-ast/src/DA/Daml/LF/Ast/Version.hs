-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Ast.Version(
  module VER
) where

import DA.Daml.LF.Ast.Version.VersionType as VER
import DA.Daml.LF.Ast.Version.VersionUtil as VER

--generated modules

-- build //compiler/daml-lf-ast:generated_haskell_versions_src to find file in bazel-out
import DA.Daml.LF.Ast.Version.GeneratedVersions as VER hiding (version2_1, version2_2, version2_dev)

-- build //compiler/daml-lf-ast:generated_haskell_features_src to find file in bazel-out
import DA.Daml.LF.Ast.Version.GeneratedFeatures as VER
