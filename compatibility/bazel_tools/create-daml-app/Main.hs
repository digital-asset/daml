-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module Main (main) where

import qualified Bazel.Runfiles
import Control.Exception.Extra
import Control.Monad
import DA.Test.Process
import DA.Test.Tar
import DA.Test.Util
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Extra as Aeson
import Data.Conduit ((.|), runConduitRes)
import qualified Data.Conduit.Combinators as Conduit
import qualified Data.Conduit.Tar as Tar
import qualified Data.Conduit.Zlib as Zlib
import Data.List.Extra
import Data.Maybe
import Data.Proxy
import Data.Tagged
import qualified Data.Text as T
import System.Directory.Extra
import System.Environment.Blank
import System.FilePath
import System.IO.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit
import Test.Tasty.Options

data Tools = Tools
  { damlBinary :: FilePath
  , damlLedgerPath :: FilePath
  , damlTypesPath :: FilePath
  , damlReactPath :: FilePath
  , messagingPatch :: FilePath
  , yarnPath :: FilePath
  , patchPath :: FilePath
  , testDepsPath :: FilePath
  , testTsPath :: FilePath
  }

newtype DamlOption = DamlOption FilePath
instance IsOption DamlOption where
  defaultValue = DamlOption $ "daml"
  parseValue = Just . DamlOption
  optionName = Tagged "daml"
  optionHelp = Tagged "runfiles path to the daml executable"

newtype DamlLedgerOption = DamlLedgerOption FilePath
instance IsOption DamlLedgerOption where
  defaultValue = DamlLedgerOption []
  parseValue = Just . DamlLedgerOption
  optionName = Tagged "daml-ledger"
  optionHelp = Tagged "path to extracted daml-ledger package"

newtype DamlTypesOption = DamlTypesOption FilePath
instance IsOption DamlTypesOption where
  defaultValue = DamlTypesOption []
  parseValue = Just . DamlTypesOption
  optionName = Tagged "daml-types"
  optionHelp = Tagged "path to extracted daml-types package"

newtype DamlReactOption = DamlReactOption FilePath
instance IsOption DamlReactOption where
  defaultValue = DamlReactOption []
  parseValue = Just . DamlReactOption
  optionName = Tagged "daml-react"
  optionHelp = Tagged "path to extracted daml-react package"

newtype MessagingPatchOption = MessagingPatchOption FilePath
instance IsOption MessagingPatchOption where
  defaultValue = MessagingPatchOption ""
  parseValue = Just . MessagingPatchOption
  optionName = Tagged "messaging-patch"
  optionHelp = Tagged "path to messaging patch"

newtype YarnOption = YarnOption FilePath
instance IsOption YarnOption where
  defaultValue = YarnOption ""
  parseValue = Just . YarnOption
  optionName = Tagged "yarn"
  optionHelp = Tagged "path to yarn"

newtype PatchOption = PatchOption FilePath
instance IsOption PatchOption where
  defaultValue = PatchOption ""
  parseValue = Just . PatchOption
  optionName = Tagged "patch"
  optionHelp = Tagged "path to patch"

newtype TestDepsOption = TestDepsOption FilePath
instance IsOption TestDepsOption where
  defaultValue = TestDepsOption ""
  parseValue = Just . TestDepsOption
  optionName = Tagged "test-deps"
  optionHelp = Tagged "path to testDeps.json"

newtype TestTsOption = TestTsOption FilePath
instance IsOption TestTsOption where
  defaultValue = TestTsOption ""
  parseValue = Just . TestTsOption
  optionName = Tagged "test-ts"
  optionHelp = Tagged "path to index.test.ts"

withTools :: (IO Tools -> TestTree) -> TestTree
withTools tests = do
  askOption $ \(DamlOption damlPath) -> do
  askOption $ \(DamlLedgerOption damlLedgerPath) -> do
  askOption $ \(DamlTypesOption damlTypesPath) -> do
  askOption $ \(DamlReactOption damlReactPath) -> do
  askOption $ \(MessagingPatchOption messagingPatch) -> do
  askOption $ \(YarnOption yarnPath) -> do
  askOption $ \(PatchOption patchPath) -> do
  askOption $ \(TestDepsOption testDepsPath) -> do
  askOption $ \(TestTsOption testTsPath) -> do
  let createRunfiles :: IO (FilePath -> FilePath)
      createRunfiles = do
        runfiles <- Bazel.Runfiles.create
        mainWorkspace <- fromMaybe "compatibility" <$> getEnv "TEST_WORKSPACE"
        pure (\path -> Bazel.Runfiles.rlocation runfiles $ mainWorkspace </> path)
  withResource createRunfiles (\_ -> pure ()) $ \locateRunfiles -> do
  let tools = do
        damlBinary <- locateRunfiles <*> pure damlPath
        pure Tools
          { damlBinary
          , damlLedgerPath
          , damlTypesPath
          , damlReactPath
          , messagingPatch
          , yarnPath
          , patchPath
          , testDepsPath
          , testTsPath
          }
  tests tools

main :: IO ()
main = do
  setEnv "TASTY_NUM_THREADS" "1" True
  let options =
        [ Option @DamlOption Proxy
        , Option @DamlLedgerOption Proxy
        , Option @DamlTypesOption Proxy
        , Option @DamlReactOption Proxy
        , Option @MessagingPatchOption Proxy
        , Option @YarnOption Proxy
        , Option @PatchOption Proxy
        , Option @TestDepsOption Proxy
        , Option @TestTsOption Proxy
        ]
  let ingredients = defaultIngredients ++ [includingOptions options]
  defaultMainWithIngredients ingredients $
    withTools $ \getTools -> do
    testGroup "Create DAML App tests"
        [ test getTools
        ]
  where
    test getTools = testCaseSteps "Getting Starte Guide" $ \step -> withTempDir $ \tmpDir -> do
        Tools{..} <- getTools
        -- daml codegen js assumes Yarn is in PATH.
        -- Setting PATH in the bash script ends up in a mess of
        -- Unix/Windows path conversions so we do it here instead.
        path <- getSearchPath
        setEnv "PATH" (intercalate [searchPathSeparator] (takeDirectory yarnPath : path)) True
        setEnv "CI" "yes" True
        step "Create app from template"
        withCurrentDirectory tmpDir $ do
          callProcess damlBinary ["new", "create-daml-app", "create-daml-app"]
        let cdaDir = tmpDir </> "create-daml-app"
        -- First test the base application (without the user-added feature).
        withCurrentDirectory cdaDir $ do
          step "Build DAML model for base application"
          callProcess damlBinary ["build"]
          step "Set up TypeScript libraries and Yarn workspaces for codegen"
          setupYarnEnv tmpDir (Workspaces ["create-daml-app/daml.js"])
              [(DamlTypes, damlTypesPath), (DamlLedger, damlLedgerPath)]
          step "Run JavaScript codegen"
          callProcess damlBinary ["codegen", "js", "-o", "daml.js", ".daml/dist/create-daml-app-0.1.0.dar"]
        assertFileDoesNotExist (cdaDir </> "ui" </> "build" </> "index.html")
        withCurrentDirectory (cdaDir </> "ui") $ do
          -- NOTE(MH): We set up the yarn env again to avoid having all the
          -- dependencies of the UI already in scope when `daml2js` runs
          -- `yarn install`. Some of the UI dependencies are a bit flaky to
          -- install and might need some retries.
          step "Set up libraries and workspaces again for UI build"
          setupYarnEnv tmpDir (Workspaces ["create-daml-app/ui"])
              [(DamlLedger, damlLedgerPath), (DamlReact, damlReactPath), (DamlTypes, damlTypesPath)]
          step "Install dependencies for UI"
          retry 3 (callProcessSilent yarnPath ["install"])
          step "Run linter"
          callProcessSilent yarnPath ["lint", "--max-warnings", "0"]
          step "Build the application UI"
          callProcessSilent yarnPath ["build"]
        assertFileExists (cdaDir </> "ui" </> "build" </> "index.html")

        step "Patch the application code with messaging feature"
        withCurrentDirectory cdaDir $ do
          callProcessSilent patchPath ["-p2", "-i", messagingPatch]
          forM_ ["MessageEdit", "MessageList"] $ \messageComponent ->
            assertFileExists ("ui" </> "src" </> "components" </> messageComponent <.> "tsx")
          step "Build the new DAML model"
          callProcessSilent damlBinary ["build"]
          step "Set up TypeScript libraries and Yarn workspaces for codegen again"
          setupYarnEnv tmpDir (Workspaces ["create-daml-app/daml.js"])
            [ (DamlTypes, damlTypesPath), (DamlLedger, damlLedgerPath) ]
          step "Run JavaScript codegen for new DAML model"
          callProcessSilent damlBinary ["codegen", "js", "-o", "daml.js", ".daml/dist/create-daml-app-0.1.0.dar"]
        withCurrentDirectory (cdaDir </> "ui") $ do
          step "Set up libraries and workspaces again for UI build"
          setupYarnEnv tmpDir (Workspaces ["create-daml-app/ui"])
            [(DamlLedger, damlLedgerPath), (DamlReact, damlReactPath), (DamlTypes, damlTypesPath)]
          step "Install UI dependencies again, forcing rebuild of generated code"
          callProcessSilent yarnPath ["install", "--force", "--frozen-lockfile"]
          step "Run linter again"
          callProcessSilent yarnPath ["lint", "--max-warnings", "0"]
          step "Build the new UI"
          callProcessSilent yarnPath ["build"]

        -- Run end to end testing for the app.
        withCurrentDirectory (cdaDir </> "ui") $ do
          step "Install Jest, Puppeteer and other dependencies"
          addTestDependencies "package.json" testDepsPath
          retry 3 (callProcessSilent yarnPath ["install"])
          step "Run Puppeteer end-to-end tests"
          copyFile testTsPath (cdaDir </> "ui" </> "src" </> "index.test.ts")
          callProcess yarnPath ["node", "--version"]
          callProcess yarnPath ["run", "test", "--ci", "--all"]

addTestDependencies :: FilePath -> FilePath -> IO ()
addTestDependencies packageJsonFile extraDepsFile = do
    packageJson <- readJsonFile packageJsonFile
    extraDeps <- readJsonFile extraDepsFile
    let newPackageJson = Aeson.lodashMerge packageJson extraDeps
    Aeson.encodeFile packageJsonFile newPackageJson
  where
    readJsonFile :: FilePath -> IO Aeson.Value
    readJsonFile path = do
        -- Read file strictly to avoid lock being held when we subsequently write to it.
        mbContent <- Aeson.decodeFileStrict' path
        case mbContent of
            Nothing -> fail ("Could not decode JSON object from " <> path)
            Just val -> return val

data TsLibrary
    = DamlLedger
    | DamlReact
    | DamlTypes
    deriving (Bounded, Enum)

newtype Workspaces = Workspaces [FilePath]

tsLibraryName :: TsLibrary -> String
tsLibraryName = \case
    DamlLedger -> "daml-ledger"
    DamlReact -> "daml-react"
    DamlTypes -> "daml-types"

-- NOTE(MH): In some tests we need our TS libraries like `@daml/types` in
-- scope. We achieve this by putting a `package.json` file further up in the
-- directory tree. This file sets up a yarn workspace that includes the TS
-- libraries via the `resolutions` field.
setupYarnEnv :: FilePath -> Workspaces -> [(TsLibrary, FilePath)] -> IO ()
setupYarnEnv rootDir (Workspaces workspaces) tsLibs = do
    forM_  tsLibs $ \(tsLib, libLocation) -> do
        let name = tsLibraryName tsLib
        removePathForcibly (rootDir </> name)
        runConduitRes
            $ Conduit.sourceFile libLocation
            .| Zlib.ungzip
            .| Tar.untar (restoreFile (\a b -> fail (T.unpack $ a <> b)) (rootDir </> name))
    Aeson.encodeFile (rootDir </> "package.json") $ Aeson.object
        [ "private" Aeson..= True
        , "workspaces" Aeson..= workspaces
        , "resolutions" Aeson..= Aeson.object
            [ pkgName Aeson..= ("file:./" ++ name)
            | (tsLib, _) <- tsLibs
            , let name = tsLibraryName tsLib
            , let pkgName = "@" <> T.replace "-" "/"  (T.pack name)
            ]
        ]
