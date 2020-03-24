-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Helper.Test (main) where

import DA.Bazel.Runfiles
import DA.Test.Util
import System.Directory
import System.Environment.Blank
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    damlHelper <- locateRunfiles (mainWorkspace </> "daml-assistant" </> "daml-helper" </> exe "daml-helper")
    defaultMain $
        testGroup "daml-helper"
            [ createDamlAppTests damlHelper
            ]

createDamlAppTests :: FilePath -> TestTree
createDamlAppTests damlHelper = testGroup "create-daml-app"
    [ testCase "Succeeds with SDK 0.13.55" $ withTempDir $ \dir -> do
          (exit, out, err) <- readCreateProcessWithExitCode
              (proc damlHelper ["create-daml-app", dir </> "foobar"])
                   { env = Just [("DAML_SDK_VERSION", "0.13.55")] }
              ""
          err @?= ""
          assertInfixOf "Created" out
          exit @?= ExitSuccess
          assertBool "daml.yaml does not exist" =<<
              doesFileExist (dir </> "foobar" </> "daml.yaml")
    , testCase "Fails with SDK 0.0.1" $ withTempDir $ \dir -> do
          -- Note that we do not test 0.0.0 since people
          -- might be tempted to create that tag temporarily for
          -- testing purposes.
          (exit, out, err) <- readCreateProcessWithExitCode
              (proc damlHelper ["create-daml-app", dir </> "foobar"])
                   { env = Just [("DAML_SDK_VERSION", "0.0.1")] }
              ""
          assertInfixOf "not available for SDK version 0.0.1" err
          out @?= ""
          exit @?= ExitFailure 1
    , testCase "Fails if directory already exists" $ withTempDir $ \dir -> do
          createDirectory (dir </> "foobar")
          (exit, out, err) <- readCreateProcessWithExitCode
              (proc damlHelper ["create-daml-app", dir </> "foobar"])
              ""
          assertInfixOf "already exists" err
          out @?= ""
          exit @?= ExitFailure 1
    ]
