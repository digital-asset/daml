-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module Main (main) where

import System.Environment (getArgs)
import DA.Sdk.Cli.Repository.Bintray.Server (runMockServerBasedTest)

main :: IO ()
main = do
  args <- getArgs
  runMockServerBasedTest args
