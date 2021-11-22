-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE DerivingStrategies #-}
module DA.Test.Daml2js (main) where

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
import qualified Data.ByteString.Lazy as BSL
import Data.Aeson
import Test.Tasty
import Test.Tasty.HUnit
import DA.Test.Process
import DA.Test.Util

-- Version of eslint we use for linting the generated code.
eslintVersion :: T.Text
eslintVersion = "^6.8.0"

main :: IO ()
main = withTempDir $ \yarnCache -> do
    setEnv "YARN_CACHE_FOLDER" yarnCache True
    yarnPath : args <- getArgs
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    damlcLegacy <- locateRunfiles ("damlc_legacy" </> "damlc_legacy")
    daml2js <- locateRunfiles (mainWorkspace </> "language-support" </> "ts" </> "codegen" </> exe "daml2js")
    yarn <- locateRunfiles (mainWorkspace </> yarnPath)
    davl <- locateRunfiles ("davl" </> "released")
    oldPath <- getSearchPath
    withArgs args $ withEnv
        [ ("PATH", Just $ intercalate [searchPathSeparator] $ takeDirectory yarn : oldPath)
        , ("TASTY_NUM_THREADS", Just "1")
        ] $ defaultMain (tests yarn damlc damlcLegacy daml2js davl)

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

tests :: FilePath -> FilePath -> FilePath -> FilePath -> FilePath -> TestTree
tests yarn damlc damlcLegacy daml2js davl = testGroup "daml2js tests"
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

  , testCaseSteps "Different name, same package test" $ \step -> withTempDir $ \here -> do
      let daml2jsDir = here </> "daml2js"
      let grover = here </> "grover"
          groverDaml = grover </> "daml"
          groverDar = grover </> ".daml" </> "dist" </> "grover-1.0.dar"
      createDirectoryIfMissing True groverDaml
      -- Locked to DAML-LF 1.7 since we get different package ids due to
      -- package metadata in DAML-LF 1.8.
      withCurrentDirectory grover $ do
        writeFileUTF8 (groverDaml </> "Grover.daml")
          "module Grover where data Grover = Grover"
        writeDamlYaml "grover" ["Grover"] ["daml-prim", "daml-stdlib"] (Just LF.version1_7)
        step "daml build..."
        buildProjectLegacy []
      let superGrover = here </> "super-grover"
          superGroverDaml = superGrover </> "daml"
          superGroverDar = superGrover </> ".daml" </> "dist" </> "super-grover-1.0.dar"
      createDirectoryIfMissing True superGroverDaml
      withCurrentDirectory superGrover $ do
        writeFileUTF8 (superGroverDaml </> "Grover.daml")
          "module Grover where data Grover = Grover"
        writeDamlYaml "super-grover" ["Grover"] ["daml-prim", "daml-stdlib"] (Just LF.version1_7)
        step "daml build..."
        buildProjectLegacy []
      withCurrentDirectory here $ do
        step "daml2js..."
        setupYarnEnvironment
        (exitCode, _, err) <- readProcessWithExitCode daml2js ([groverDar, superGroverDar] ++ ["-o", daml2jsDir]) ""
        assertBool "daml2js is expected to fail but succeeded" (exitCode /= ExitSuccess)
        assertInfixOf "Different names ('grover-1.0' and 'super-grover-1.0') for the same package detected" err

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

  , testCaseSteps "DAVL test" $ \step -> withTempDir $ \here -> do
      let daml2jsDir = here </> "daml2js"
      withCurrentDirectory here $ do
        step "daml2js..."
        setupYarnEnvironment
        callProcessSilent daml2js $
          [ davl </> "davl-v4.dar"
          , davl </> "davl-v5.dar"
          , davl </> "davl-upgrade-v4-v5.dar" ] ++
          ["-o", daml2jsDir]
        mapM_ (assertTsFileExists (daml2jsDir </> "davl-0.0.4"))
          [ "index"
          , "DAVL" </> "index"
          , "DAVL" </> "module"
          ]
        mapM_ (assertTsFileExists (daml2jsDir </> "davl-0.0.5"))
          [ "index"
          , "DAVL" </> "index"
          , "DAVL" </> "V5" </> "index"
          , "DAVL" </> "V5" </> "module"
          ]
        mapM_ (assertTsFileExists (daml2jsDir </> "davl-upgrade-v4-v5-0.0.5"))
          [ "index"
          , "Upgrade" </> "index"
          , "Upgrade" </> "module"
          ]
      step "eslint..."
      withCurrentDirectory daml2jsDir $ do
        pkgs <- (\\ ["package.json", "node_modules"]) <$> listDirectory daml2jsDir
        BSL.writeFile "package.json" $ encode $
          object
            [ "private" .= True
            , "devDependencies" .= object
                [ "eslint" .= eslintVersion
                ]
            , "workspaces" .= pkgs
            , "resolutions" .= object
              [ "@daml/types" .= ("file:../daml-types" :: T.Text)
              , "@daml/ledger" .= ("file:../daml-ledger" :: T.Text)]
            ]
        BSL.writeFile ".eslintrc.json" $ encode $ object
            [ "extends" .=
                [ "eslint:recommended" :: T.Text
                ]
            , "env" .= object [ "commonjs" .= True ] -- We generate commonjs modules
            , "rules" .= object
                [ "no-unused-vars" .=
                -- We disable the unused argument warning since that gets
                -- triggered for decoders of phantom type arguments.
                    [ "error" :: Value , object [ "args" .= ("none" :: T.Text) ] ]
                ]
            ]
        callProcessSilent yarn ["install", "--pure-lockfile"]
        callProcessSilent yarn ["workspaces", "run", "eslint", "-c", ".." </> ".eslintrc.json", "--max-warnings", "0", "lib/"]
  ]
  where
    setupYarnEnvironment :: IO ()
    setupYarnEnvironment = do
      setupYarnEnv "." (Workspaces ["daml2js"]) [DamlTypes, DamlLedger]

    buildProject :: [String] -> IO ()
    buildProject args = callProcessSilent damlc (["build"] ++ args)

    buildProjectLegacy :: [String] -> IO ()
    buildProjectLegacy args = callProcessSilent damlcLegacy (["build"] ++ args)

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
