-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import qualified Data.Text as T
import System.Environment
import System.Process

import WithOracle

main :: IO ()
main = do
    (arg : args) <- getArgs
    withOracle $ \jdbcUrl ->
        callProcess arg (map (T.unpack . T.replace "__jdbcurl__" jdbcUrl . T.pack) args)
