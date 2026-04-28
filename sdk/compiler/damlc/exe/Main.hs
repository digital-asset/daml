-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import ComponentVersion (withComponentVersions)

import qualified DA.Cli.Damlc

main :: IO ()
main = withComponentVersions DA.Cli.Damlc.main
