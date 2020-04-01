-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE DerivingStrategies #-}
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
import DA.Test.Daml2TsUtils
import Data.List.Extra
import qualified Data.Text.Extended as T
import qualified Data.ByteString.Lazy as BSL
import Data.Aeson
import Test.Tasty
import Test.Tasty.HUnit
import DA.Test.Util

-- Version of eslint we use for linting the generated code.
eslintVersion :: T.Text
eslintVersion = "^6.8.0"

-- Version of typescript-eslint for linting the generated code.
typescriptEslintVersion :: T.Text
typescriptEslintVersion = "^2.16.0"

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    -- We manipulate global state via the working directory and
    -- the environment so running tests in parallel will cause trouble.
    yarnPath : damlTypesPath : args <- getArgs
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    daml2ts <- locateRunfiles (mainWorkspace </> "language-support" </> "ts" </> "codegen" </> exe "daml2ts")
    yarn <- locateRunfiles (mainWorkspace </> yarnPath)
    damlTypes <- locateRunfiles (mainWorkspace </> damlTypesPath)
    davl <- locateRunfiles ("davl" </> "released")
    -- TODO (SF,2020-03-24): Factor out 'withEnv' from
    -- 'DA/DamlAssistant/Tests.hs' into a library function and use it here.
    oldPath <- getSearchPath
    setEnv "PATH" (intercalate [searchPathSeparator] $ takeDirectory yarn : oldPath) True
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
tests damlTypes yarn damlc daml2ts davl = testGroup "daml2ts tests"
  [
    testCaseSteps "Different package, same name test" $ \step -> withTempDir $ \here -> do
      let grover = here </> "grover"
          groverDaml = grover </> "daml"
          daml2tsDir = here </> "daml2ts"
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
        step "daml2ts..."
        setupYarnEnvironment
        (exitCode, _, err) <- readProcessWithExitCode daml2ts ([groverDar, elmoDar] ++ ["-o", daml2tsDir]) ""
        assertBool "daml2ts is expected to fail but succeeded" (exitCode /= ExitSuccess)
        assertInfixOf "Duplicate name 'grover-1.0' for different packages detected" err

  , testCaseSteps "Different name, same package test" $ \step -> withTempDir $ \here -> do
      let daml2tsDir = here </> "daml2ts"
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
        buildProject []
      let superGrover = here </> "super-grover"
          superGroverDaml = superGrover </> "daml"
          superGroverDar = superGrover </> ".daml" </> "dist" </> "super-grover-1.0.dar"
      createDirectoryIfMissing True superGroverDaml
      withCurrentDirectory superGrover $ do
        writeFileUTF8 (superGroverDaml </> "Grover.daml")
          "module Grover where data Grover = Grover"
        writeDamlYaml "super-grover" ["Grover"] ["daml-prim", "daml-stdlib"] (Just LF.version1_7)
        step "daml build..."
        buildProject []
      withCurrentDirectory here $ do
        step "daml2ts..."
        setupYarnEnvironment
        (exitCode, _, err) <- readProcessWithExitCode daml2ts ([groverDar, superGroverDar] ++ ["-o", daml2tsDir]) ""
        assertBool "daml2ts is expected to fail but succeeded" (exitCode /= ExitSuccess)
        assertInfixOf "Different names ('grover-1.0' and 'super-grover-1.0') for the same package detected" err

  , testCaseSteps "Same package, same name test" $ \step -> withTempDir $ \here -> do
      let grover = here </> "grover"
          groverDaml = grover </> "daml"
          daml2tsDir = here </> "daml2ts"
          groverTs =  daml2tsDir </> "grover-1.0"
          groverDar = grover </> ".daml" </> "dist" </> "grover-1.0.dar"
      createDirectoryIfMissing True groverDaml
      withCurrentDirectory grover $ do
        writeFileUTF8 (groverDaml </> "Grover.daml")
          "module Grover where data Grver = Grover"
        writeDamlYaml "grover" ["Grover"] ["daml-prim", "daml-stdlib"] Nothing
        step "daml build..."
        buildProject []
      withCurrentDirectory here $ do
        step "daml2ts..."
        setupYarnEnvironment
        daml2tsProject [groverDar, groverDar] daml2tsDir
        mapM_ (assertTsFileExists groverTs) [ "index", "Grover" </> "index", "Grover" </> "module" ]

  , testCaseSteps "IndexTree test" $ \step -> withTempDir $ \here -> do
      let projectRoot = here </> "project"
          daml2tsDir = here </> "daml2ts"
          projectTs =  daml2tsDir </> "project-1.0"
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
        step "daml2ts..."
        setupYarnEnvironment
        daml2tsProject [projectDar] daml2tsDir
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
        assertFileDoesNotExist (projectTs </> "src" </> "A" </> "B" </> "module.ts")

        withCurrentDirectory (projectTs </> "src") $ do
          let reexportIndex name =
                [ "import * as " <> name <> " from './" <> name <> "';"
                , "export import " <> name <> " = " <> name <> ";"
                ]
          let reexportModule = ["export * from './module';"]
          indexContents <- T.lines <$> T.readFileUtf8 "index.ts"
          assertBool "index.ts does not reexport A" (reexportIndex "A" `isPrefixOf` indexContents)
          assertFileLines ("A" </> "index.ts") (reexportIndex "B" ++ reexportModule)
          assertFileLines ("A" </> "B" </> "index.ts") (reexportIndex "C" ++ reexportIndex "D")
          assertFileLines ("A" </> "B" </> "C" </> "index.ts") reexportModule
          assertFileLines ("A" </> "B" </> "D" </> "index.ts") reexportModule

  , testCaseSteps "DAVL test" $ \step -> withTempDir $ \here -> do
      let daml2tsDir = here </> "daml2ts"
      withCurrentDirectory here $ do
        step "daml2ts..."
        setupYarnEnvironment
        callProcessSilent daml2ts $
          [ davl </> "davl-v4.dar"
          , davl </> "davl-v5.dar"
          , davl </> "davl-upgrade-v4-v5.dar" ] ++
          ["-o", daml2tsDir]
        mapM_ (assertTsFileExists (daml2tsDir </> "davl-0.0.4")) [ "index", "DAVL" </> "module" ]
        mapM_ (assertTsFileExists (daml2tsDir </> "davl-0.0.5")) [ "index", "DAVL" </> "module" ]
        mapM_ (assertTsFileExists (daml2tsDir </> "davl-upgrade-v4-v5-0.0.5")) [ "index", "Upgrade" </> "module" ]
      step "eslint..."
      withCurrentDirectory daml2tsDir $ do
        pkgs <- (\\ ["package.json", "node_modules"]) <$> listDirectory daml2tsDir
        BSL.writeFile "package.json" $ encode $
          object
            [ "private" .= True
            , "devDependencies" .= object
                [ "eslint" .= eslintVersion
                , "@typescript-eslint/eslint-plugin" .= typescriptEslintVersion
                , "@typescript-eslint/parser" .= typescriptEslintVersion
                ]
            , "workspaces" .= pkgs
            , "name" .= ("daml2ts" :: T.Text)
            , "version" .= ("0.0.0" :: T.Text)
            ]
        BSL.writeFile ".eslintrc.json" $ encode $
          object
            [ "parser" .= ("@typescript-eslint/parser" :: T.Text)
            , "parserOptions" .= object [("project", "./tsconfig.json")]
            , "plugins" .= (["@typescript-eslint"] :: [T.Text])
            , "extends" .= (
                [ "eslint:recommended"
                , "plugin:@typescript-eslint/eslint-recommended"
                , "plugin:@typescript-eslint/recommended"
                , "plugin:@typescript-eslint/recommended-requiring-type-checking"
                ] :: [T.Text])
            , "rules" .= object
              [ ("@typescript-eslint/explicit-function-return-type", "off")
              , ("@typescript-eslint/no-inferrable-types", "off")
              ]
          ]
        callProcessSilent yarn ["install", "--pure-lockfile"]
        callProcessSilent yarn ["workspaces", "run", "eslint", "-c", ".." </> ".eslintrc.json", "--ext", ".ts", "--max-warnings", "0", "src/"]
  ]
  where
    setupYarnEnvironment :: IO ()
    setupYarnEnvironment = do
      copyDirectory damlTypes "daml-types"
      writeRootPackageJson Nothing ["daml2ts"]

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

    assertTsFileExists :: FilePath -> String -> IO ()
    assertTsFileExists proj file = do
      assertFileExists (proj </> "src" </> file <.> "ts")
      assertFileExists (proj </> "lib" </> file <.> "js")
      assertFileExists (proj </> "lib" </> file <.> "d.ts")
        where
          assertFileExists :: FilePath -> IO ()
          assertFileExists file = doesFileExist file >>= assertBool (file ++ " was not created")
    assertFileDoesNotExist :: FilePath -> IO ()
    assertFileDoesNotExist file = doesFileExist file >>= assertBool (file ++ " should not exist") . not

    assertFileLines :: FilePath -> [T.Text] -> IO ()
    assertFileLines file expectedContent = do
      actualContent <- T.lines <$> T.readFileUtf8 file
      assertEqual ("The content of file '" ++ file ++ "' does not match") expectedContent actualContent
