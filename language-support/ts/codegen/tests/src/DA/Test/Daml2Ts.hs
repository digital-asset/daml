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
import DA.Directory
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
    yarnPath : damlTypes : args <- getArgs
    yarn <- locateRunfiles (mainWorkspace </> yarnPath)
    withTempDir $ \rootDir ->
      withArgs args (
        defaultMain $
          withResource
          (yarnInstall yarn damlTypes rootDir) (\_ -> pure ())
          (\_ -> tests rootDir yarn damlc daml2ts davl)
        )

yarnInstall :: FilePath -> FilePath -> FilePath -> IO ()
yarnInstall yarn damlTypes rootDir = do
  let here = rootDir </> "pre-test"
  let dummyTs = here </> "dummy-ts"
  createDirectoryIfMissing True dummyTs
  copyDirectory damlTypes (rootDir </> "daml-types")
  writePackageConfigs dummyTs
  withCurrentDirectory rootDir $ yarnProject' yarn ["install"]

tests :: FilePath -> FilePath -> FilePath -> FilePath -> FilePath -> TestTree
tests rootDir yarn damlc daml2ts davl = testGroup "daml2Ts"
  [ testCase "Pre-test yarn check" $ do
      assertBool "'node_modules' does not exist" =<< doesDirectoryExist rootDir
      assertBool "'yarn.lock' does not exist " =<< doesFileExist (rootDir </> "yarn.lock")

  , testCaseSteps "Breathing test" $ \step -> do
      let here = rootDir </> "breathing-test"
          grover = here </> "grover"
          groverDaml = grover </> "daml"
          groverTs = here </> "grover-ts"
          groverTsSrc = groverTs </> "src"
          groverTsLib = groverTs </> "lib"
          groverDar = grover </> ".daml" </> "dist" </> "grover-1.0.dar"
      step "Creating project 'grover'..."
      createDirectoryIfMissing True groverDaml
      createDirectoryIfMissing True groverTs
      writeFileUTF8 (groverDaml </> "Grover.daml") $ unlines
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
      daml2tsProject [groverDar] (groverTs </> "src")
      assertBool "'Grover.ts' was not created." =<< doesFileExist (groverTsSrc </> "grover-1.0" </> "Grover.ts")
      assertBool "'packageId.ts' was not created." =<< doesFileExist (groverTsSrc </> "grover-1.0" </> "packageId.ts")
      step "Compiling 'grover-ts' to JavaScript... "
      writePackageConfigs groverTs
      withCurrentDirectory groverTs $ do
        yarnProject ["run", "build"]
        assertBool "'Grover.js' was not created." =<< doesFileExist (groverTsLib </> "grover-1.0" </> "Grover.js")
        step "Linting 'grover-ts' ... "
        yarnProject ["run", "lint"]

  , testCaseSteps "Dependency test" $ \step -> do
      let here = rootDir </> "dependency-test"
          grover = here </> "grover"
          groverDaml = grover </> "daml"
          groverDar = grover </> ".daml" </> "dist" </> "grover-1.0.dar"
      step "Creating project 'grover'..."
      createDirectoryIfMissing True groverDaml
      writeFileUTF8 (groverDaml </> "Grover.daml") $ unlines
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
      let charliesRestaurant = here </> "charlies-restaurant"
          charliesRestaurantDaml = charliesRestaurant </> "daml"
          charliesRestaurantTs = here </> "charlies-restaurant-ts"
          charliesRestaurantTsSrc = charliesRestaurantTs </> "src"
          charliesRestaurantTsLib = charliesRestaurantTs </> "lib"
          charliesRestaurantDar = charliesRestaurant </> ".daml" </> "dist" </> "charlies-restaurant-1.0.dar"
      step "Creating project 'charlies-restaurant'..."
      createDirectoryIfMissing True charliesRestaurantDaml
      createDirectoryIfMissing True charliesRestaurantTs
      writeFileUTF8 (charliesRestaurantDaml </> "CharliesRestaurant.daml") $ unlines
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
      daml2tsProject [charliesRestaurantDar] charliesRestaurantTsSrc
      assertBool "'CharliesRestaurant.ts' was not created." =<< doesFileExist (charliesRestaurantTsSrc </> "charlies-restaurant-1.0" </> "CharliesRestaurant.ts")
      assertBool "'packageId.ts' was not created." =<< doesFileExist (charliesRestaurantTsSrc </> "charlies-restaurant-1.0" </> "packageId.ts")
      assertBool "'Grover.ts' was created." . not =<< doesFileExist (charliesRestaurantTsSrc </> "grover-1.0" </> "Grover.ts")
      removeDirectoryRecursive charliesRestaurantTs
      assertBool "'charlies-restaurant-ts' should not exist." . not =<< doesDirectoryExist charliesRestaurantTs
      createDirectoryIfMissing True charliesRestaurantTs
      assertBool "'charlies-restaurant-ts' should exist." =<< doesDirectoryExist charliesRestaurantTs
      daml2tsProject [charliesRestaurantDar, groverDar] charliesRestaurantTsSrc
      assertBool "'CharliesRestaurant.ts' was not created." =<< doesFileExist (charliesRestaurantTsSrc </> "charlies-restaurant-1.0" </> "CharliesRestaurant.ts")
      assertBool "'packageId.ts' was not created." =<< doesFileExist (charliesRestaurantTsSrc </> "charlies-restaurant-1.0" </> "packageId.ts")
      assertBool "'Grover.ts' was not created." =<< doesFileExist (charliesRestaurantTsSrc </> "grover-1.0" </> "Grover.ts")
      assertBool "'packageId.ts' was not created." =<< doesFileExist (charliesRestaurantTsSrc </> "grover-1.0" </> "packageId.ts")
      step "Compiling 'charlies-restaurant-ts' to JavaScript... "
      writePackageConfigs charliesRestaurantTs
      withCurrentDirectory charliesRestaurantTs $ do
        yarnProject ["run", "build"]
        assertBool "'Grover.js' was not created." =<< doesFileExist (charliesRestaurantTsLib </> "grover-1.0" </> "Grover.js")
        assertBool "'CharliesRestaurant.js' was not created." =<< doesFileExist (charliesRestaurantTsLib </> "charlies-restaurant-1.0" </> "CharliesRestaurant.js")
        step "Linting 'charlies-restaurant' ... "
        yarnProject ["run", "lint"]

  , testCaseSteps "Package name collision test" $ \step -> withTempDir $ \tmpDir -> do
      let grover = tmpDir </> "grover"
          groverDaml = grover </> "daml"
          groverTs = tmpDir </> "grover-ts"
          groverDar = grover </> ".daml" </> "dist" </> "grover-1.0.dar"
      step "Creating project 'grover'..."
      createDirectoryIfMissing True groverDaml
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
          elmoDaml = elmo </> "daml"
          elmoTs = tmpDir </> "elmo-ts"
          elmoDar = elmo </> ".daml" </> "dist" </> "elmo-1.0.dar"
      step "Creating project 'elmo'..."
      createDirectoryIfMissing True elmoDaml
      createDirectoryIfMissing True elmoTs
      writeFileUTF8 (elmoDaml </> "Elmo.daml") $ unlines
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

  , testCaseSteps "Different names for the same package test" $ \step -> withTempDir $ \tmpDir -> do
      let grover = tmpDir </> "grover"
          groverDaml = grover </> "daml"
          groverDar = grover </> ".daml" </> "dist" </> "grover-1.0.dar"
      step "Creating project 'grover'..."
      createDirectoryIfMissing True groverDaml
      writeFileUTF8 (groverDaml </> "Grover.daml") $ unlines
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
      let superGrover = tmpDir </> "super-grover"
          superGroverDaml = superGrover </> "daml"
          superGroverDar = superGrover </> ".daml" </> "dist" </> "super-grover-1.0.dar"
      step "Creating project 'superGrover'..."
      createDirectoryIfMissing True superGroverDaml
      writeFileUTF8 (superGroverDaml </> "Grover.daml") $ unlines
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
      writeFileUTF8 (superGrover </> "daml.yaml") $ unlines
        [ "sdk-version: " <> sdkVersion
        , "name: super-grover"
        , "version: \"1.0\""
        , "source: daml"
        , "exposed-modules: [Grover]"
        , "dependencies:"
        , "  - daml-prim"
        , "  - daml-stdlib"
        ]
      buildProject superGrover []
      assertBool "super-grover-1.0.dar was not created." =<< doesFileExist superGroverDar
      step "Generating TypeScript of 'grover' and 'super-grover'..."
      let charliesRestaurantTs = tmpDir </> "charlies-restaurant-ts"
      createDirectoryIfMissing True charliesRestaurantTs
      (exitCode, _, err) <- readProcessWithExitCode daml2ts ([groverDar, superGroverDar] ++ ["-o", charliesRestaurantTs]) ""
      assertBool "An error resulting from the same name for different packages was expected." (exitCode /= ExitSuccess && isJust (stripInfix "Different names ('grover-1.0' and 'super-grover-1.0') for the same package detected" err))

  , testCaseSteps "Same package, same name test" $ \step -> do
      let here = rootDir </> "duplicate-package-test"
          grover = here </> "grover"
          groverDaml = grover </> "daml"
          groverDar = grover </> ".daml" </> "dist" </> "grover-1.0.dar"
      step "Creating project 'grover'..."
      createDirectoryIfMissing True groverDaml
      writeFileUTF8 (groverDaml </> "Grover.daml") $ unlines
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
      step "Generating TypeScript of 'grover' and 'grover'..."
      let charliesRestaurantTs = here </> "charlies-restaurant-ts"
          charliesRestaurantTsSrc = charliesRestaurantTs </> "src"
          charliesRestaurantTsLib = charliesRestaurantTs </> "lib"
      createDirectoryIfMissing True charliesRestaurantTs
      daml2tsProject [groverDar, groverDar] charliesRestaurantTsSrc
      assertBool "'Grover.ts' was not created." =<< doesFileExist (charliesRestaurantTsSrc </> "grover-1.0" </> "Grover.ts")
      step "Compiling 'charlies-restaurant-ts' to JavaScript... "
      writePackageConfigs charliesRestaurantTs
      withCurrentDirectory charliesRestaurantTs $ do
        yarnProject ["run", "build"]
        assertBool "'Grover.js' was not created." =<< doesFileExist (charliesRestaurantTsLib </> "grover-1.0" </> "Grover.js")
        step "Linting 'charlies-restaurant' ... "
        yarnProject ["run", "lint"]

  , testCaseSteps "DAVL test" $ \step -> do
      let here = rootDir </> "davl-test"
          davlTs = here </> "davl-ts"
          davlTsSrc = davlTs </> "src"
          davlTsLib = davlTs </> "lib"
      createDirectoryIfMissing True davlTs
      step "Generating TypeScript of davl..."
      daml2tsProject [ davl </> "davl-v4.dar", davl </> "davl-v5.dar", davl </> "davl-upgrade-v4-v5.dar" ] (davlTs </> "src")
      assertBool "davl-0.0.4/DAVL.ts was not created." =<< doesFileExist (davlTsSrc </> "davl-0.0.4" </> "DAVL.ts")
      assertBool "davl-0.0.5/DAVL.ts was not created." =<< doesFileExist (davlTsSrc </> "davl-0.0.5" </> "DAVL.ts")
      assertBool "davl-upgrade-v4-v5-0.0.5/Upgrade.ts was not created." =<< doesFileExist (davlTsSrc </> "davl-upgrade-v4-v5-0.0.5" </> "Upgrade.ts")
      step "Compiling 'davl-ts' to JavaScript... "
      writePackageConfigs davlTs
      withCurrentDirectory davlTs $ do
        yarnProject ["run", "build"]
        assertBool "'davl-0.0.4/DAVL.js' was not created." =<< doesFileExist (davlTsLib </> "davl-0.0.4" </> "DAVL.js")
        assertBool "'davl-0.0.5/DAVL.js' was not created." =<< doesFileExist (davlTsLib </> "davl-0.0.5" </> "DAVL.js")
        assertBool "'davl-upgrade-v4-v5-0.0.5/Upgrade.js' was not created." =<< doesFileExist (davlTsLib </> "davl-upgrade-v4-v5-0.0.5" </> "Upgrade.js")
        step "Linting 'davl' ... "
        yarnProject ["run", "lint"]
     ]
  where
    buildProject' :: FilePath -> FilePath -> [String] -> IO ()
    buildProject' damlc dir args = withCurrentDirectory dir $ callProcessSilent damlc (["build"] ++ args)
    buildProject = buildProject' damlc

    daml2tsProject' :: FilePath -> [FilePath] -> FilePath -> IO ()
    daml2tsProject' daml2ts dars outDir = callProcessSilent daml2ts $ dars ++ ["-o", outDir]
    daml2tsProject = daml2tsProject' daml2ts

    yarnProject = yarnProject' yarn

yarnProject' :: FilePath -> [String] -> IO ()
yarnProject' yarn args = callProcessSilent yarn args

-- | Only displays stdout and stderr on errors
callProcessSilent :: FilePath -> [String] -> IO ()
callProcessSilent cmd args = do
    (exitCode, out, err) <- readProcessWithExitCode cmd args ""
    unless (exitCode == ExitSuccess) $ do
      hPutStrLn stderr $ "Failure: Command \"" <> cmd <> " " <> unwords args <> "\" exited with " <> show exitCode
      hPutStrLn stderr $ unlines ["stdout:", out]
      hPutStrLn stderr $ unlines ["stderr: ", err]
      exitFailure

writePackageConfigs :: FilePath -> IO ()
writePackageConfigs dir = do
  -- e.g. /path/to/root/pre-test/dummy-ts
  --        tsDir = dummy-ts
  --        testDir = pre-test
  --        rootDir = /path/to/root
  --        workspace = pre-test/dummy-ts
  let tsDir = takeFileName dir
      testDir = takeFileName (takeDirectory dir)
      rootDir = takeDirectory (takeDirectory dir)
      workspace = testDir <> "/" <> tsDir
  writeTsConfig dir
  writeEsLintConfig dir
  writePackageJson dir
  -- The existence of 'package.json' at root level is critical to
  -- making our scheme of doing 'yarn install' just once work.
  writeRootPackageJson rootDir workspace

  where
    writeTsConfig :: FilePath -> IO ()
    writeTsConfig dir = writeFileUTF8 (dir </> "tsconfig.json") $ unlines
        [ "{"
        , "  \"compilerOptions\": {"
        , "    \"target\": \"es5\","
        , "    \"lib\": ["
        , "      \"es2015\""
        , "     ],"
        , "    \"strict\": true,"
        , "    \"noUnusedLocals\": true,"
        , "    \"noUnusedParameters\": false,"
        , "    \"noImplicitReturns\": true,"
        , "    \"noFallthroughCasesInSwitch\": true,"
        , "    \"outDir\": \"lib\","
        , "    \"module\": \"commonjs\","
        , "    \"declaration\": true,"
        , "    \"sourceMap\": true"
        , "    },"
        , "  \"include\": [\"src/**/*.ts\"],"
        , "}"
        ]

    writeEsLintConfig :: FilePath -> IO ()
    writeEsLintConfig dir = writeFileUTF8 (dir </> ".eslintrc.json") $ unlines
      [ "{"
      , "  \"parser\": \"@typescript-eslint/parser\","
      , "  \"parserOptions\": {"
      , "    \"project\": \"./tsconfig.json\""
      , "  },"
      , "  \"plugins\": ["
      , "    \"@typescript-eslint\""
      , "  ],"
      , "  \"extends\": ["
      , "    \"eslint:recommended\","
      , "    \"plugin:@typescript-eslint/eslint-recommended\","
      , "    \"plugin:@typescript-eslint/recommended\","
      , "    \"plugin:@typescript-eslint/recommended-requiring-type-checking\""
      , "  ],"
      , "  \"rules\": {"
      , "    \"@typescript-eslint/explicit-function-return-type\": \"off\","
      , "    \"@typescript-eslint/no-inferrable-types\": \"off\""
      , "  }"
      , "}"
      ]

    writePackageJson :: FilePath -> IO ()
    writePackageJson dir = let name = takeFileName dir in writeFileUTF8 (dir </> "package.json") $ unlines
            ["{"
            , "  \"private\": true,"
            , "  \"name\": \"@daml2ts/" <> name <> "\","
            , "  \"version\": \"" <> sdkVersion <> "\","
            , "  \"description\": \"Produced by daml2ts\","
            , "  \"license\": \"Apache-2.0\","
            , "  \"dependencies\": {"
            , "    \"@daml/types\": \"" <> sdkVersion <> "\","
            , "    \"@mojotech/json-type-validation\": \"^3.1.0\""
            , "  },"
            , "  \"scripts\": {"
            , "    \"build\": \"tsc --build\","
            , "    \"lint\": \"eslint --ext .ts src/ --max-warnings 0\""
            , "  },"
            , "  \"devDependencies\": {"
            , "    \"@typescript-eslint/eslint-plugin\": \"^2.11.0\","
            , "    \"@typescript-eslint/parser\": \"^2.11.0\","
            , "    \"eslint\": \"^6.7.2\","
            , "    \"typescript\": \"~3.7.3\""
            , "  }"
            , "}"
            ]

    writeRootPackageJson :: FilePath -> String -> IO ()
    writeRootPackageJson rootDir workspace =
       writeFileUTF8 (rootDir </> "package.json") $ unlines
         [ "{"
         , "  \"private\": true,"
         , "  \"workspaces\": ["
         , "    \"daml-types\","
         , "    \"" <> workspace <> "\""
         , "  ]"
         , "}"
         ]
