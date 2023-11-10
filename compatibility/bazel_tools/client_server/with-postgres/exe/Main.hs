-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import Data.Text qualified as T
import System.Environment
import System.Process
import WithPostgres

main :: IO ()
main = do
    (arg : args) <- getArgs
    withPostgres $ \jdbcUrl ->
        callProcess arg (map (T.unpack . T.replace "__jdbcurl__" jdbcUrl . T.pack) args)
