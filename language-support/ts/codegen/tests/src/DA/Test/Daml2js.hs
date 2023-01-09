-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE DerivingStrategies #-}
module DA.Test.Daml2js (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import System.FilePath
import System.IO.Extra
import System.Environment.Blank
import System.Directory.Extra
import System.Process
import System.Exit
import DA.Bazel.Runfiles
import qualified DA.Daml.LF.Ast.Version as LF
import DA.Test.Daml2jsUtils
import Data.List.Extra
import qualified Data.Text.Extended as T
import Test.Tasty
import Test.Tasty.HUnit
import DA.Test.Process
import DA.Test.Util

main :: IO ()
main = do
    args <- getArgs
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    daml2js <- locateRunfiles (mainWorkspace </> "language-support" </> "ts" </> "codegen" </> exe "daml2js")
    oldPath <- getSearchPath
    withArgs args $ withEnv
        [ ("PATH", Just $ intercalate [searchPathSeparator] oldPath)
        , ("TASTY_NUM_THREADS", Just "1")
        ] $ defaultMain (tests damlc daml2js)

-- It may help to keep in mind for the following tests, this quick
-- refresher on the layout of a simple project:
--   grover/
--     .daml/dist/grover-1.0.dar
--     daml.yaml
--     daml/
--       Grover.daml
--     package.json
--     daml2js/
--       grover-1.0/
--         package.json
--         tsconfig.json
--         src/
--           index.ts
--           Grover/
--             index.ts
--             module.ts
--         lib/
--           index.{js,d.ts}
--           Grover/
--             index.{js,d.ts}
--             module.{js,d.ts}
--       ...
--     daml-types  <-- referred to by the "resolutions" field in package.json

tests :: FilePath -> FilePath -> TestTree
tests damlc daml2js = testGroup "daml2js tests"
  [
    testCaseSteps "Different package, same name test" $ \step -> withTempDir $ \here -> do
      let grover = here </> "grover"
          groverDaml = grover </> "daml"
          daml2jsDir = here </> "daml2js"
          groverDar = grover </> ".daml" </> "dist" </> "grover-1.0.dar"
      createDirectoryIfMissing True groverDaml
      withCurrentDirectory grover $ do
        writeFileUTF8 (grover </> "daml" </> "Grover.daml")
          "module Grover where data Grover = Grover"
        writeDamlYaml "grover" ["Grover"] ["daml-prim", "daml-stdlib"] Nothing
        step "daml build..."
        buildProject []
      let elmo = here </> "elmo"
          elmoDaml = elmo </> "daml"
          elmoDar = elmo </> ".daml" </> "dist" </> "elmo-1.0.dar"
      createDirectoryIfMissing True elmoDaml
      withCurrentDirectory elmo $ do
        writeFileUTF8 (elmoDaml </> "Elmo.daml") "module Elmo where data Elmo = Elmo"
        writeDamlYaml "grover" ["Elmo"] ["daml-prim", "daml-stdlib"] Nothing
        step "daml build..."
        buildProject ["-o", ".daml" </> "dist" </> "elmo-1.0.dar"]
        step "daml2js..."
        setupYarnEnvironment
        (exitCode, _, err) <- readProcessWithExitCode daml2js ([groverDar, elmoDar] ++ ["-o", daml2jsDir]) ""
        assertBool "daml2js is expected to fail but succeeded" (exitCode /= ExitSuccess)
        assertInfixOf "Duplicate name 'grover-1.0' for different packages detected" err

  , testCaseSteps "Same package, same name test" $ \step -> withTempDir $ \here -> do
      let grover = here </> "grover"
          groverDaml = grover </> "daml"
          daml2jsDir = here </> "daml2js"
          groverTs =  daml2jsDir </> "grover-1.0"
          groverDar = grover </> ".daml" </> "dist" </> "grover-1.0.dar"
      createDirectoryIfMissing True groverDaml
      withCurrentDirectory grover $ do
        writeFileUTF8 (groverDaml </> "Grover.daml")
          "module Grover where data Grver = Grover"
        writeDamlYaml "grover" ["Grover"] ["daml-prim", "daml-stdlib"] Nothing
        step "daml build..."
        buildProject []
      withCurrentDirectory here $ do
        step "daml2js..."
        setupYarnEnvironment
        daml2jsProject [groverDar, groverDar] daml2jsDir
        mapM_ (assertTsFileExists groverTs) [ "index", "Grover" </> "index", "Grover" </> "module" ]

  , testCaseSteps "IndexTree test" $ \step -> withTempDir $ \here -> do
      let projectRoot = here </> "project"
          daml2jsDir = here </> "daml2js"
          projectTs =  daml2jsDir </> "project-1.0"
          projectDar = projectRoot </> ".daml" </> "dist" </> "project-1.0.dar"
      createDirectoryIfMissing True projectRoot
      withCurrentDirectory projectRoot $ do
        createDirectoryIfMissing True ("daml" </> "A" </> "B")
        writeFileUTF8 ("daml" </> "A.daml") "module A where data X = X"
        writeFileUTF8 ("daml" </> "A" </> "B" </> "C.daml") "module A.B.C where data Y = Y"
        writeFileUTF8 ("daml" </> "A" </> "B" </> "D.daml") "module A.B.D where data Z = Z"
        writeDamlYaml "project" ["A"] ["daml-prim", "daml-stdlib"] Nothing
        step "daml build..."
        buildProject []
      withCurrentDirectory here $ do
        step "daml2js..."
        setupYarnEnvironment
        daml2jsProject [projectDar] daml2jsDir
        mapM_ (assertTsFileExists projectTs)
          [ "index"
          , "A" </> "index"
          , "A" </> "module"
          , "A" </> "B" </> "index"
          , "A" </> "B" </> "C" </> "index"
          , "A" </> "B" </> "C" </> "module"
          , "A" </> "B" </> "D" </> "index"
          , "A" </> "B" </> "D" </> "module"
          ]
        assertFileDoesNotExist (projectTs </> "lib" </> "A" </> "B" </> "module.js")

        withCurrentDirectory (projectTs </> "lib") $ do
          let reexportIndex name =
                [ "import * as " <> name <> " from './" <> name <> "';"
                , "export { " <> name <> " } ;"
                ]
          let reexportModule = ["export * from './module';"]
          indexContents <- T.lines <$> T.readFileUtf8 "index.d.ts"
          assertBool "index.ts does not reexport A" (reexportIndex "A" `isPrefixOf` indexContents)
          assertFileLines ("A" </> "index.d.ts") (reexportIndex "B" ++ reexportModule)
          assertFileLines ("A" </> "B" </> "index.d.ts") (reexportIndex "C" ++ reexportIndex "D")
          assertFileLines ("A" </> "B" </> "C" </> "index.d.ts") reexportModule
          assertFileLines ("A" </> "B" </> "D" </> "index.d.ts") reexportModule
  ]
  where
    setupYarnEnvironment :: IO ()
    setupYarnEnvironment = do
      setupYarnEnv "." (Workspaces ["daml2js"]) [DamlTypes, DamlLedger]

    buildProject :: [String] -> IO ()
    buildProject args = callProcessSilent damlc (["build"] ++ args)

    daml2jsProject :: [FilePath] -> FilePath -> IO ()
    daml2jsProject dars outDir = callProcessSilent daml2js $ dars ++ ["-o", outDir]

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

    assertTsFileExists :: FilePath -> String -> IO ()
    assertTsFileExists proj file = do
      assertFileExists (proj </> "lib" </> file <.> "js")
      assertFileExists (proj </> "lib" </> file <.> "d.ts")

    assertFileLines :: FilePath -> [T.Text] -> IO ()
    assertFileLines file expectedContent = do
      actualContent <- T.lines <$> T.readFileUtf8 file
      assertEqual ("The content of file '" ++ file ++ "' does not match") expectedContent actualContent
