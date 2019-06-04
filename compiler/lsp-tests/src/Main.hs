-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
module Main (main) where

import DA.Bazel.Runfiles
import qualified Data.Text as T
import Language.Haskell.LSP.Test
import Language.Haskell.LSP.Types
import System.FilePath
import System.IO.Extra
import Test.Tasty
import Test.Tasty.HUnit

import Daml.Lsp.Test.Util

main :: IO ()
main = do
    damlcPath <- locateRunfiles $
        mainWorkspace </> "daml-foundations" </> "daml-tools" </>
        "da-hs-damlc-app" </> "da-hs-damlc-app"
    let run s = withTempDir $ \dir -> runSessionWithConfig conf (damlcPath <> " ide") fullCaps dir s
    defaultMain $ testGroup "LSP"
        [ diagnosticTests run
        ]
    where
        conf = defaultConfig
            -- If you uncomment this you can see all messages
            -- which can be quite useful for debugging.
            -- { logMessages = True }

diagnosticTests :: (forall a. Session a -> IO a) -> TestTree
diagnosticTests run = testGroup "diagnostics"
    [ testCase "diagnostics on parse error" $ run $ do
          test <- openDoc' "Test.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Test where"
              , "f 1"
              ]
          expectDiagnostics [(DsError, (2, 0), "Parse error")]
          closeDoc test
    , testCase "diagnostics disappear after error is fixed" $ run $ do
          test <- openDoc' "Test.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Test where"
              , "f 1"
              ]
          expectDiagnostics [(DsError, (2,0), "Parse error")]
          replaceDoc test $ T.unlines
              [ "daml 1.2"
              , "module Test where"
              , "f = ()"
              ]
          expectDiagnostics []
          closeDoc test
    , testCase "diagnostics appear after introducing an error" $ run $ do
          test <- openDoc' "Test.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Test where"
              , "f = ()"
              ]
          replaceDoc test $ T.unlines
              [ "daml 1.2"
              , "module Test where"
              , "f 1"
              ]
          expectDiagnostics [(DsError, (2, 0), "Parse error")]
          closeDoc test
    ]

