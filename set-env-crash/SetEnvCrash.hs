-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module SetEnvCrash(main) where

import System.Environment.Blank
import Test.Tasty
import Test.Tasty.HUnit

main :: IO ()
main =
  mkMain
    -- swap the following lines
    (setEnv "aaa" "bbb" True)
    (pure ())

mkMain :: IO () -> IO () -> IO ()
mkMain resourceSetEnv bodySetEnv = do
  setEnv "TASTY_NUM_THREADS" "1" True
  defaultMain $ do
    withResource resourceSetEnv (const (pure ())) $ \_ -> do
      testGroup "setEnv" $
        [ testCase "setEnv crash" $ do
            bodySetEnv
            pure ()
        ]
