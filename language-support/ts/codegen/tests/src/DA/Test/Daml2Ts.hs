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
import qualified DA.Daml.LF.Ast.Version as LF
import DA.Directory
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
    -- daml2ts requires yarn be in PATH.
    let yarnDir = takeDirectory yarn
    path <- fromJust <$> getEnv "PATH"
    setEnv "PATH" (path ++ searchPathSeparator : yarnDir) True
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
--     daml-types  <-- referred to by the "resolutions" field in package.json

tests :: FilePath -> FilePath -> FilePath -> FilePath -> FilePath -> TestTree
tests damlTypes _yarn damlc daml2ts davl = testGroup "daml2ts tests"
  [
    testCaseSteps "Different package, same name test" $ \step -> withTempDir $ \here -> do
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
        writeDamlYaml "grover" ["Grover"] ["daml-prim", "daml-stdlib"] Nothing
        step "daml build..."
        buildProject []
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
        writeDamlYaml "grover" ["Elmo"] ["daml-prim", "daml-stdlib"] Nothing
        step "daml build..."
        buildProject ["-o", ".daml" </> "dist" </> "elmo-1.0.dar"]
        step "daml2ts..."
        copyDirectory damlTypes "daml-types"
        (exitCode, _, err) <- readProcessWithExitCode daml2ts ([groverDar, elmoDar] ++ ["-o", daml2tsDir]) ""
        assertBool "A duplicate name for different packages error was expected." (exitCode /= ExitSuccess && isJust (stripInfix "Duplicate name 'grover-1.0' for different packages detected" err))

  , testCaseSteps "Different name, same package test" $ \step -> withTempDir $ \here -> do
      let daml2tsDir = here </> "daml2ts"
      let grover = here </> "grover"
          groverDaml = grover </> "daml"
          groverDar = grover </> ".daml" </> "dist" </> "grover-1.0.dar"
      createDirectoryIfMissing True groverDaml
      -- Locked to DAML-LF 1.7 since we get different package ids due to
      -- package metadata in DAML-LF 1.8.
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
        writeDamlYaml "grover" ["Grover"] ["daml-prim", "daml-stdlib"] (Just LF.version1_7)
        step "daml build..."
        buildProject []
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
        writeDamlYaml "super-grover" ["Grover"] ["daml-prim", "daml-stdlib"] (Just LF.version1_7)
        step "daml build..."
        buildProject []
      withCurrentDirectory here $ do
        step "daml2ts..."
        copyDirectory damlTypes "daml-types"
        (exitCode, _, err) <- readProcessWithExitCode daml2ts ([groverDar, superGroverDar] ++ ["-o", daml2tsDir]) ""
        assertBool "A different names for same package error was expected." (exitCode /= ExitSuccess && isJust (stripInfix "Different names ('grover-1.0' and 'super-grover-1.0') for the same package detected" err))

  , testCaseSteps "Same package, same name test" $ \step -> withTempDir $ \here -> do
      let grover = here </> "grover"
          groverDaml = grover </> "daml"
          daml2tsDir = here </> "daml2ts"
          groverTs =  daml2tsDir </> "grover-1.0"
          groverTsSrc = groverTs </> "src"
          groverTsLib = groverTs </> "lib"
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
        writeDamlYaml "grover" ["Grover"] ["daml-prim", "daml-stdlib"] Nothing
        step "daml build..."
        buildProject []
      withCurrentDirectory here $ do
        step "daml2ts..."
        copyDirectory damlTypes "daml-types"
        daml2tsProject [groverDar, groverDar] daml2tsDir
        assertFileExists (groverTsSrc </> "Grover.ts")
        assertFileExists (groverTsLib </> "Grover.js")
        assertFileExists (groverTsLib </> "Grover.d.ts")
        assertFileExists (groverTsSrc </> "packageId.ts")
        assertFileExists (groverTsLib </> "packageId.js")
        assertFileExists (groverTsLib </> "packageId.d.ts")

  , testCaseSteps "DAVL test" $ \step -> withTempDir $ \here -> do
      let daml2tsDir = here </> "daml2ts"
      withCurrentDirectory here $ do
        copyDirectory damlTypes "daml-types"
        step "daml2ts..."
        callProcessSilent daml2ts $
          [ davl </> "davl-v4.dar"
          , davl </> "davl-v5.dar"
          , davl </> "davl-upgrade-v4-v5.dar" ] ++
          ["-o", daml2tsDir]
        assertFileExists (daml2tsDir </> "davl-0.0.4" </> "src" </> "DAVL.ts")
        assertFileExists (daml2tsDir </> "davl-0.0.4" </> "lib" </> "DAVL.js")
        assertFileExists (daml2tsDir </> "davl-0.0.4" </> "lib" </> "DAVL.d.ts")
        assertFileExists (daml2tsDir </> "davl-0.0.5" </> "src" </> "DAVL.ts")
        assertFileExists (daml2tsDir </> "davl-0.0.5" </> "lib" </> "DAVL.js")
        assertFileExists (daml2tsDir </> "davl-0.0.5" </> "lib" </> "DAVL.d.ts")
        assertFileExists (daml2tsDir </> "davl-upgrade-v4-v5-0.0.5" </> "src" </> "Upgrade.ts")
        assertFileExists (daml2tsDir </> "davl-upgrade-v4-v5-0.0.5" </> "lib" </> "Upgrade.js")
        assertFileExists (daml2tsDir </> "davl-upgrade-v4-v5-0.0.5" </> "lib" </> "Upgrade.d.ts")
     {-
     -- Unfortunately, this test is failing in CI because we can't
     -- install eslint on the fly like this (the filesystem it
     -- requires writing to is not writeable). Disabling for the time
     -- being while I work out what to do about it.

      withCurrentDirectory daml2tsDir $ do
        step "yarn lint..."
        writeFileUTF8 "package.json" $ unlines
          [ "{"
          , "    \"devDependencies\": {"
          , "        \"eslint\": \"^6.7.2\","
          , "        \"@typescript-eslint/eslint-plugin\": \"2.11.0\","
          , "        \"typescript\": \"~3.7.3\","
          , "        \"@typescript-eslint/parser\": \"2.11.0\""
          , "    },"
          , "    \"dependencies\": {"
          , "        \"@daml/types\": \"file:../daml-types\","
          , "        \"@mojotech/json-type-validation\": \"^3.1.0\""
          , "    }"
          , "}"
          ]
        -- I'm surprised by this. eslint can only be found when
        -- installed globally like this.
        callProcessSilent yarn ["global", "add", "eslint@^6.7.2"]
        callProcessSilent yarn ["install", "--pure-lockfile"]
        pkgs <- (\\ ["package.json", "node_modules"]) <$> listDirectory daml2tsDir
        forM_ pkgs (\pkg -> withCurrentDirectory pkg $ callProcessSilent "yarn" ["lint"])
     -}
  ]
  where
    buildProject :: [String] -> IO ()
    buildProject args = callProcessSilent damlc (["build"] ++ args)

    daml2tsProject :: [FilePath] -> FilePath -> IO ()
    daml2tsProject dars outDir = callProcessSilent daml2ts $ dars ++ ["-o", outDir]

    callProcessSilent :: FilePath -> [String] -> IO ()
    callProcessSilent cmd args = do
        (exitCode, out, err) <- readProcessWithExitCode cmd args ""
        unless (exitCode == ExitSuccess) $ do
          hPutStrLn stderr $ "Failure: Command \"" <> cmd <> " " <> unwords args <> "\" exited with " <> show exitCode
          hPutStrLn stderr $ unlines ["stdout:", out]
          hPutStrLn stderr $ unlines ["stderr:", err]
          exitFailure

    writeDamlYaml :: String -> [String] -> [String] -> Maybe LF.Version -> IO ()
    writeDamlYaml mainPackageName exposedModules dependencies mbLfVersion =
      writeFileUTF8 "daml.yaml" $ unlines $
        [ "sdk-version: 0.0.0"
        , "name: " <> mainPackageName
        , "version: \"1.0\""
        , "source: daml"
        , "exposed-modules: [" <> intercalate "," exposedModules <> "]"
        , "dependencies:"
        ] ++
        ["  - " ++ dependency | dependency <- dependencies] ++
        ["build-options: [--target=" <> LF.renderVersion ver <> "]" | Just ver <- [mbLfVersion]]

    assertFileExists :: FilePath -> IO ()
    assertFileExists file = doesFileExist file >>= assertBool (file ++ " was not created")
