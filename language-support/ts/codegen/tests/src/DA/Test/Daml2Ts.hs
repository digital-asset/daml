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

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    daml2ts <- locateRunfiles (mainWorkspace </> "language-support" </> "ts" </> "codegen" </> exe "daml2ts")
    davl <- locateRunfiles ("davl" </> "released")
    yarnPath : damlTypesPath : args <- getArgs
    yarn <- locateRunfiles (mainWorkspace </> yarnPath)
    damlTypes <- (</> damlTypesPath) <$> getCurrentDirectory
    withArgs args (defaultMain $ tests damlTypes yarn damlc daml2ts davl)

-- It may help to keep in mind for the following tests, this quick
-- refresher on the layout of a simple project:
--   grover/
--     .daml/dist/grover-1.0.dar
--     daml.yaml
--     daml/
--       Grover.daml
--     package.json
--     daml2ts/
--       grover-1.0/
--         package.json
--         tsconfig.json
--         src/ *.ts
--         lib/ *.js
--
-- Also, you'll see this idiom:
--   copyDirectory damlTypesDir (here </> "daml-types")
--   writeRootPackageJson
--   daml2tsProject [darFile] daml2tsDir (here </> "package.json")
--   yarnProject ["install"]
--   writeFile "package.json" .  replace "    \"daml-types\"," "" =<< readFile' "package.json"
--   yarnProject ["workspaces", "run", "build"]
-- This is because we treat "@daml/types" as a yarn workspace for
-- dependency resolution, but don't allow it to be included in a 'yarn
-- workspaces run build' (which would fail to compile for lack of
-- 'index.ts').

tests :: FilePath -> FilePath -> FilePath -> FilePath -> FilePath -> TestTree
tests damlTypes yarn damlc daml2ts davl = testGroup "daml2ts tests"
  [
    testCaseSteps "Different package, same name test" $ \step -> withTempDir $ \here -> do
      copyDirectory damlTypes (here </> "daml-types")
      let grover = here </> "grover"
          groverDaml = grover </> "daml"
          daml2tsDir = here </> "daml2ts"
          groverDar = grover </> ".daml" </> "dist" </> "grover-1.0.dar"
      createDirectoryIfMissing True groverDaml
      withCurrentDirectory grover $ do
        writeFileUTF8 (grover </> "daml" </> "Grover.daml") $ unlines
          [ "module Grover where"
          , "template Grover"
          , "  with puppeteer : Party"
          , "  where"
          , "    signatory puppeteer"
          , "    choice Grover_GoSuper: ContractId Grover"
          , "      controller puppeteer"
          , "      do"
          , "        return self"
          ]
        writeDamlYaml "grover" ["Grover"] ["daml-prim", "daml-stdlib"]
        step "daml build..."
        buildProject []
        assertBool "grover-1.0.dar was not created." =<< doesFileExist groverDar
      let elmo = here </> "elmo"
          elmoDaml = elmo </> "daml"
          elmoDar = elmo </> ".daml" </> "dist" </> "elmo-1.0.dar"
      createDirectoryIfMissing True elmoDaml
      withCurrentDirectory elmo $ do
        writeFileUTF8 (elmoDaml </> "Elmo.daml") $ unlines
          [ "module Elmo where"
          , "template Elmo"
          , "  with puppeteer : Party"
          , "  where"
          , "    signatory puppeteer"
          ]
        writeDamlYaml "grover" ["Elmo"] ["daml-prim", "daml-stdlib"]
        step "daml build..."
        buildProject ["-o", ".daml" </> "dist" </> "elmo-1.0.dar"]
        assertBool "elmo-1.0.dar was not created." =<< doesFileExist elmoDar
        step "daml2ts..."
        writeRootPackageJson
        (exitCode, _, err) <- readProcessWithExitCode daml2ts ([groverDar, elmoDar] ++ ["-o", daml2tsDir, "-p", here </> "package.json"]) ""
        assertBool "A duplicate name for different packages error was expected." (exitCode /= ExitSuccess && isJust (stripInfix "Duplicate name 'grover-1.0' for different packages detected" err))

  , testCaseSteps "Different name, same package test" $ \step -> withTempDir $ \here -> do
      copyDirectory damlTypes (here </> "daml-types")
      let daml2tsDir = here </> "daml2ts"
      let grover = here </> "grover"
          groverDaml = grover </> "daml"
          groverDar = grover </> ".daml" </> "dist" </> "grover-1.0.dar"
      createDirectoryIfMissing True groverDaml
      withCurrentDirectory grover $ do
        writeFileUTF8 (groverDaml </> "Grover.daml") $ unlines
          [ "module Grover where"
          , "template Grover"
          , "  with puppeteer : Party"
          , "  where"
          , "    signatory puppeteer"
          , "    choice Grover_GoSuper: ContractId Grover"
          , "      controller puppeteer"
          , "      do"
          , "        return self"
          ]
        writeDamlYaml "grover" ["Grover"] ["daml-prim", "daml-stdlib"]
        step "daml build..."
        buildProject []
        assertBool "grover-1.0.dar was not created." =<< doesFileExist groverDar
      let superGrover = here </> "super-grover"
          superGroverDaml = superGrover </> "daml"
          superGroverDar = superGrover </> ".daml" </> "dist" </> "super-grover-1.0.dar"
      createDirectoryIfMissing True superGroverDaml
      withCurrentDirectory superGrover $ do
        writeFileUTF8 (superGroverDaml </> "Grover.daml") $ unlines
          [ "module Grover where"
          , "template Grover"
          , "  with puppeteer : Party"
          , "  where"
          , "    signatory puppeteer"
          , "    choice Grover_GoSuper: ContractId Grover"
          , "      controller puppeteer"
          , "      do"
          , "        return self"
          ]
        writeDamlYaml "super-grover" ["Grover"] ["daml-prim", "daml-stdlib"]
        step "daml build..."
        buildProject []
        assertBool "super-grover-1.0.dar was not created." =<< doesFileExist superGroverDar
      withCurrentDirectory here $ do
        step "daml2ts..."
        writeRootPackageJson
        (exitCode, _, err) <- readProcessWithExitCode daml2ts ([groverDar, superGroverDar] ++ ["-o", daml2tsDir, "-p", here </> "package.json"]) ""
        assertBool "A different names for same package error was expected." (exitCode /= ExitSuccess && isJust (stripInfix "Different names ('grover-1.0' and 'super-grover-1.0') for the same package detected" err))

  , testCaseSteps "Same package, same name test" $ \step -> withTempDir $ \here -> do
      copyDirectory damlTypes (here </> "daml-types")
      let grover = here </> "grover"
          groverDaml = grover </> "daml"
          daml2tsDir = here </> "daml2ts"
          groverTs =  daml2tsDir </> "grover-1.0"
          groverTsSrc = groverTs </> "src"
          groverDar = grover </> ".daml" </> "dist" </> "grover-1.0.dar"
      createDirectoryIfMissing True groverDaml
      copyDirectory damlTypes (here </> "daml-types")
      withCurrentDirectory grover $ do
        writeFileUTF8 (groverDaml </> "Grover.daml") $ unlines
          [ "module Grover where"
          , "template Grover"
          , "  with puppeteer : Party"
          , "  where"
          , "    signatory puppeteer"
          , "    choice Grover_GoSuper: ContractId Grover"
          , "      controller puppeteer"
          , "      do"
          , "        return self"
          ]
        writeDamlYaml "grover" ["Grover"] ["daml-prim", "daml-stdlib"]
        step "daml build..."
        buildProject []
        assertBool "grover-1.0.dar was not created." =<< doesFileExist groverDar
      withCurrentDirectory here $ do
        step "daml2ts..."
        writeRootPackageJson
        daml2tsProject [groverDar, groverDar] daml2tsDir (here </> "package.json")
        assertBool "'Grover.ts' was not created." =<< doesFileExist (groverTsSrc </> "Grover.ts")
        assertBool "'packageId.ts' was not created." =<< doesFileExist (groverTsSrc </> "packageId.ts")

  , testCaseSteps "DAVL test" $ \step -> withTempDir $ \here -> do
      let daml2tsDir = here </> "daml2ts"
      withCurrentDirectory here $ do
        step "daml2ts..."
        -- In this test, '@daml/types-0.13.51' comes from the npm
        -- package registry.
        writeFileUTF8 "package.json" $ unlines
         [ "{"
         , "  \"private\": true,"
         , "  \"workspaces\": []"
         , "}" ]
        callProcessSilent daml2ts $
          [ davl </> "davl-v4.dar"
          , davl </> "davl-v5.dar"
          , davl </> "davl-upgrade-v4-v5.dar" ] ++
          ["-o", daml2tsDir, "-p", here </> "package.json", "--daml-types-version", "0.13.51"]
        assertBool "davl-0.0.4/src/DAVL.ts was not created." =<< doesFileExist (daml2tsDir </> "davl-0.0.4" </> "src" </> "DAVL.ts")
        assertBool "davl-0.0.5/src/DAVL.ts was not created." =<< doesFileExist (daml2tsDir </> "davl-0.0.5" </> "src" </> "DAVL.ts")
        assertBool "davl-upgrade-v4-v5-0.0.5/src/Upgrade.ts was not created." =<< doesFileExist (daml2tsDir </> "davl-upgrade-v4-v5-0.0.5" </> "src" </> "Upgrade.ts")
        step "yarn install..."
        yarnProject ["install"]
        step "yarn workspaces run build..."
        yarnProject ["workspaces", "run", "build"]
        assertBool "'davl-0.0.4/lib/DAVL.js' was not created." =<< doesFileExist (daml2tsDir </> "davl-0.0.4" </> "lib" </> "DAVL.js")
        assertBool "'davl-0.0.5/lib/DAVL.js' was not created." =<< doesFileExist (daml2tsDir </> "davl-0.0.5" </> "lib" </> "DAVL.js")
        assertBool "'davl-upgrade-v4-v5-0.0.5/lib/Upgrade.js' was not created." =<< doesFileExist (daml2tsDir </> "davl-upgrade-v4-v5-0.0.5" </> "lib" </> "Upgrade.js")
     ]
  where
    buildProject :: [String] -> IO ()
    buildProject args = callProcessSilent damlc (["build"] ++ args)

    daml2tsProject :: [FilePath] -> FilePath -> FilePath -> IO ()
    daml2tsProject dars outDir packageJson = callProcessSilent daml2ts $ dars ++ ["-o", outDir, "-p", packageJson]

    yarnProject :: [String] -> IO ()
    yarnProject args = callProcessSilent yarn args

    callProcessSilent :: FilePath -> [String] -> IO ()
    callProcessSilent cmd args = do
        (exitCode, out, err) <- readProcessWithExitCode cmd args ""
        unless (exitCode == ExitSuccess) $ do
          hPutStrLn stderr $ "Failure: Command \"" <> cmd <> " " <> unwords args <> "\" exited with " <> show exitCode
          hPutStrLn stderr $ unlines ["stdout:", out]
          hPutStrLn stderr $ unlines ["stderr: ", err]

    writeRootPackageJson :: IO ()
    writeRootPackageJson =
       writeFileUTF8 "package.json" $ unlines
         [ "{"
         , "  \"private\": true,"
         , "  \"workspaces\": ["
         , "    \"daml-types\""
         , "  ]"
         , "}"
         ]

    writeDamlYaml :: String -> [String] -> [String] -> IO ()
    writeDamlYaml mainPackageName exposedModules dependencies =
      writeFileUTF8 "daml.yaml" $ unlines (
        [ "sdk-version: 0.0.0"
        , "name: " <> mainPackageName
        , "version: \"1.0\""
        , "source: daml"
        , "exposed-modules: [" <> intercalate "," exposedModules <> "]"
        , "dependencies:"] ++ ["  - " ++ dependency | dependency <- dependencies]
      )
