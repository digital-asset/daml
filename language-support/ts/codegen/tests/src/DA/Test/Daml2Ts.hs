-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.Daml2Ts (main) where

import Control.Monad.Extra
import System.FilePath
import System.IO.Extra
import System.Environment.Blank
import System.Directory.Extra
import System.Process
import System.Exit
import DA.Bazel.Runfiles
import Data.Maybe
import Data.List.Extra
import Test.Tasty
import Test.Tasty.HUnit

import SdkVersion

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    daml2ts <- locateRunfiles (mainWorkspace </> "language-support" </> "ts" </> "codegen" </> exe "daml2ts")
    davl <- locateRunfiles ("davl" </> "released")
    defaultMain $ tests damlc daml2ts davl

tests :: FilePath -> FilePath -> FilePath -> TestTree
tests damlc daml2ts davl = testGroup "daml2Ts"
  [ testCaseSteps "Breathing test" $ \step -> withTempDir $ \tmpDir -> do
      let grover = tmpDir </> "grover"
      let groverTs = tmpDir </> "grover-ts"
      let groverDar = grover </> ".daml" </> "dist" </> "grover-1.0.dar"
      step "Creating project 'grover'..."
      createDirectoryIfMissing True (grover </> "daml")
      createDirectoryIfMissing True groverTs
      writeFileUTF8 (grover </> "daml" </> "Grover.daml") $ unlines
        [ "daml 1.2"
        , "module Grover where"
        , "template Grover"
        , "  with puppeteer : Party"
        , "  where"
        , "    signatory puppeteer"
        , "    choice Grover_GoSuper: ContractId Grover"
        , "      controller puppeteer"
        , "      do"
        , "        return self"
        ]
      writeFileUTF8 (grover </> "daml.yaml") $ unlines
        [ "sdk-version: " <> sdkVersion
        , "name: grover"
        , "version: \"1.0\""
        , "source: daml"
        , "exposed-modules: [Grover]"
        , "dependencies:"
        , "  - daml-prim"
        , "  - daml-stdlib"
        ]
      buildProject grover []
      assertBool "grover-1.0.dar was not created." =<< doesFileExist groverDar
      step "Generating TypeScript of 'grover'..."
      daml2tsProject [groverDar] groverTs
      assertBool "'Grover.ts' was not created." =<< doesFileExist (groverTs </> "grover-1.0" </> "Grover.ts")
      assertBool "'packageId.ts' was not created." =<< doesFileExist (groverTs </> "grover-1.0" </> "packageId.ts")

  ,  testCaseSteps "Dependency test" $ \step -> withTempDir $ \tmpDir -> do
      let grover = tmpDir </> "grover"
      let groverDar = grover </> ".daml" </> "dist" </> "grover-1.0.dar"
      step "Creating project 'grover'..."
      createDirectoryIfMissing True (grover </> "daml")
      writeFileUTF8 (grover </> "daml" </> "Grover.daml") $ unlines
        [ "daml 1.2"
        , "module Grover where"
        , "template Grover"
        , "  with puppeteer : Party"
        , "  where"
        , "    signatory puppeteer"
        , "    choice Grover_GoSuper: ContractId Grover"
        , "      controller puppeteer"
        , "      do"
        , "        return self"
        ]
      writeFileUTF8 (grover </> "daml.yaml") $ unlines
        [ "sdk-version: " <> sdkVersion
        , "name: grover"
        , "version: \"1.0\""
        , "source: daml"
        , "exposed-modules: [Grover]"
        , "dependencies:"
        , "  - daml-prim"
        , "  - daml-stdlib"
        ]
      buildProject grover []
      assertBool "grover-1.0.dar was not created." =<< doesFileExist groverDar
      let charliesRestaurant = tmpDir </> "charlies-restaurant"
      let charliesRestaurantDar = charliesRestaurant </> ".daml" </> "dist" </> "charlies-restaurant-1.0.dar"
      let charliesRestaurantTs = tmpDir </> "charlies-restaurant-ts"
      step "Creating project 'charlies-restaurant'..."
      createDirectoryIfMissing True charliesRestaurantTs
      createDirectoryIfMissing True (charliesRestaurant </> "daml")
      writeFileUTF8 (charliesRestaurant </> "daml" </> "CharliesRestaurant.daml") $ unlines
        [ "daml 1.2"
        , "module CharliesRestaurant where"
        , "import Grover"
        , "template CharliesRestaurant"
        , "  with  puppeteer : Party"
        , "  where"
        , "    signatory puppeteer"
        , "    choice CharliesRestaurant_SummonGrover: ContractId Grover"
        , "      controller puppeteer"
        , "      do"
        , "        create Grover with puppeteer"
        ]
      writeFileUTF8 (charliesRestaurant </> "daml.yaml") $ unlines
        [ "sdk-version: " <> sdkVersion
        , "name: charlies-restaurant"
        , "version: \"1.0\""
        , "source: daml"
        , "exposed-modules: [CharliesRestaurant]"
        , "dependencies:"
        , "  - daml-prim"
        , "  - daml-stdlib"
        , "  - " <> groverDar
        ]
      buildProject charliesRestaurant []
      assertBool "'charlies-restaurant-1.0.dar' was not created." =<< doesFileExist charliesRestaurantDar
      step "Generating TypeScript of 'charlies-restaurant'..."
      daml2tsProject [charliesRestaurantDar] charliesRestaurantTs
      assertBool "'CharliesRestaurant.ts' was not created." =<< doesFileExist (charliesRestaurantTs </> "charlies-restaurant-1.0" </> "CharliesRestaurant.ts")
      assertBool "'packageId.ts' was not created." =<< doesFileExist (charliesRestaurantTs </> "charlies-restaurant-1.0" </> "packageId.ts")
      assertBool "'Grover.ts' was created." . not =<< doesFileExist (charliesRestaurantTs </> "grover-1.0" </> "Grover.ts")
      removeDirectoryRecursive charliesRestaurantTs
      assertBool "'charlies-restaurant-ts' should not exist." . not =<< doesDirectoryExist charliesRestaurantTs
      createDirectoryIfMissing True charliesRestaurantTs
      assertBool "'charlies-restaurant-ts' should exist." =<< doesDirectoryExist charliesRestaurantTs
      daml2tsProject [charliesRestaurantDar, groverDar] charliesRestaurantTs
      assertBool "'CharliesRestaurant.ts' was not created." =<< doesFileExist (charliesRestaurantTs </> "charlies-restaurant-1.0" </> "CharliesRestaurant.ts")
      assertBool "'packageId.ts' was not created." =<< doesFileExist (charliesRestaurantTs </> "charlies-restaurant-1.0" </> "packageId.ts")
      assertBool "'Grover.ts' was not created." =<< doesFileExist (charliesRestaurantTs </> "grover-1.0" </> "Grover.ts")
      assertBool "'packageId.ts' was not created." =<< doesFileExist (charliesRestaurantTs </> "grover-1.0" </> "packageId.ts")

  , testCaseSteps "Package name collision test" $ \step -> withTempDir $ \tmpDir -> do
      let grover = tmpDir </> "grover"
      let groverTs = tmpDir </> "grover-ts"
      let groverDar = grover </> ".daml" </> "dist" </> "grover-1.0.dar"
      step "Creating project 'grover'..."
      createDirectoryIfMissing True (grover </> "daml")
      createDirectoryIfMissing True groverTs
      writeFileUTF8 (grover </> "daml" </> "Grover.daml") $ unlines
        [ "daml 1.2"
        , "module Grover where"
        , "template Grover"
        , "  with puppeteer : Party"
        , "  where"
        , "    signatory puppeteer"
        , "    choice Grover_GoSuper: ContractId Grover"
        , "      controller puppeteer"
        , "      do"
        , "        return self"
        ]
      writeFileUTF8 (grover </> "daml.yaml") $ unlines
        [ "sdk-version: " <> sdkVersion
        , "name: grover"
        , "version: \"1.0\""
        , "source: daml"
        , "exposed-modules: [Grover]"
        , "dependencies:"
        , "  - daml-prim"
        , "  - daml-stdlib"
        ]
      buildProject grover []
      assertBool "grover-1.0.dar was not created." =<< doesFileExist groverDar
      let elmo = tmpDir </> "elmo"
      let elmoTs = tmpDir </> "elmo-ts"
      let elmoDar = elmo </> ".daml" </> "dist" </> "elmo-1.0.dar"
      step "Creating project 'elmo'..."
      createDirectoryIfMissing True (elmo </> "daml")
      createDirectoryIfMissing True elmoTs
      writeFileUTF8 (elmo </> "daml" </> "Elmo.daml") $ unlines
        [ "daml 1.2"
        , "module Elmo where"
        , "template Elmo"
        , "  with puppeteer : Party"
        , "  where"
        , "    signatory puppeteer"
        ]
      writeFileUTF8 (elmo </> "daml.yaml") $ unlines
        [ "sdk-version: " <> sdkVersion
        , "name: grover" -- Note this!
        , "version: \"1.0\""
        , "source: daml"
        , "exposed-modules: [Elmo]"
        , "dependencies:"
        , "  - daml-prim"
        , "  - daml-stdlib"
        ]
      buildProject elmo ["-o", ".daml" </> "dist" </> "elmo-1.0.dar"]
      assertBool "elmo-1.0.dar was not created." =<< doesFileExist elmoDar
      step "Generating TypeScript of 'grover' and 'elmo'..."
      (exitCode, _, err) <- readProcessWithExitCode daml2ts ([groverDar, elmoDar] ++ ["-o", elmoTs]) ""
      assertBool "A name collision error was expected." (exitCode /= ExitSuccess && isJust (stripInfix "Duplicate name 'grover-1.0' for different packages detected" err))

   , testCase "DAVL test" $ withTempDir $ \tmpDir -> do
       let davlTs = tmpDir </> "davl-ts"
       createDirectoryIfMissing True davlTs
       daml2tsProject [ davl </> "davl-v4.dar", davl </> "davl-v5.dar", davl </> "davl-upgrade-v4-v5.dar" ] davlTs
       assertBool "davl-0.0.4/DAVL.ts was not created." =<< doesFileExist (davlTs </> "davl-0.0.4" </> "DAVL.ts")
       assertBool "davl-0.0.5/DAVL.ts was not created." =<< doesFileExist (davlTs </> "davl-0.0.5" </> "DAVL.ts")
       assertBool "davl-upgrade-v4-v5-0.0.5/Upgrade.ts was not created." =<< doesFileExist (davlTs </> "davl-upgrade-v4-v5-0.0.5" </> "Upgrade.ts")
     ]
  where
    buildProject' :: FilePath -> FilePath -> [String] -> IO ()
    buildProject' damlc dir args = withCurrentDirectory dir $ callProcessSilent damlc (["build"] ++ args)
    buildProject = buildProject' damlc

    daml2tsProject' :: FilePath -> [FilePath] -> FilePath -> IO ()
    daml2tsProject' daml2ts dars outDir = callProcessSilent daml2ts $ dars ++ ["-o", outDir]
    daml2tsProject = daml2tsProject' daml2ts

-- | Only displays stdout and stderr on errors
callProcessSilent :: FilePath -> [String] -> IO ()
callProcessSilent cmd args = do
    (exitCode, out, err) <- readProcessWithExitCode cmd args ""
    unless (exitCode == ExitSuccess) $ do
      hPutStrLn stderr $ "Failure: Command \"" <> cmd <> " " <> unwords args <> "\" exited with " <> show exitCode
      hPutStrLn stderr $ unlines ["stdout:", out]
      hPutStrLn stderr $ unlines ["stderr: ", err]
      exitFailure
