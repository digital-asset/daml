-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import qualified Data.Text as T
import System.Environment
import System.Process

import WithPostgres

main :: IO ()
main = do
    (arg : args) <- getArgs
    withPostgres $ \jdbcUrl ->
        callProcess arg (args <> ["--jdbcurl=" <> T.unpack jdbcUrl])
