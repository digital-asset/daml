-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import SdkVersion (withSdkVersions)

import qualified DA.Cli.Damlc

main :: IO ()
main = withSdkVersions DA.Cli.Damlc.main
