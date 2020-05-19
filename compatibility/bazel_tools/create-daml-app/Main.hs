-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module Main (main) where

import qualified Bazel.Runfiles
import Control.Applicative
import Control.Exception.Extra
import Control.Monad
import DA.Test.Process
import DA.Test.Tar
import DA.Test.Util
import qualified Data.Aeson as Aeson
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
  , sandboxBinary :: FilePath
  , sandboxArgs :: [String]
  , jsonApiBinary :: FilePath
  , jsonApiArgs :: [String]
  , damlLedgerPath :: FilePath
  , damlTypesPath :: FilePath
  , damlReactPath :: FilePath
  , messagingPatch :: FilePath
  , yarnPath :: FilePath
  , patchPath :: FilePath
  }

newtype DamlOption = DamlOption FilePath
instance IsOption DamlOption where
  defaultValue = DamlOption $ "daml"
  parseValue = Just . DamlOption
  optionName = Tagged "daml"
  optionHelp = Tagged "runfiles path to the daml executable"

newtype SandboxOption = SandboxOption FilePath
instance IsOption SandboxOption where
  defaultValue = SandboxOption $ "sandbox"
  parseValue = Just . SandboxOption
  optionName = Tagged "sandbox"
  optionHelp = Tagged "runfiles path to the sandbox executable"

newtype SandboxArgsOption = SandboxArgsOption { unSandboxArgsOption :: [String] }
instance IsOption SandboxArgsOption where
  defaultValue = SandboxArgsOption []
  parseValue = Just . SandboxArgsOption . (:[])
  optionName = Tagged "sandbox-arg"
  optionHelp = Tagged "extra arguments to pass to sandbox executable"
  optionCLParser = concatMany (mkOptionCLParser mempty)
    where concatMany = fmap (SandboxArgsOption . concat) . many . fmap unSandboxArgsOption

newtype JsonApiOption = JsonApiOption FilePath
instance IsOption JsonApiOption where
  defaultValue = JsonApiOption $ "json-api"
  parseValue = Just . JsonApiOption
  optionName = Tagged "json-api"
  optionHelp = Tagged "runfiles path to the json-api executable"

newtype JsonApiArgsOption = JsonApiArgsOption { unJsonApiArgsOption :: [String] }
instance IsOption JsonApiArgsOption where
  defaultValue = JsonApiArgsOption []
  parseValue = Just . JsonApiArgsOption . (:[])
  optionName = Tagged "json-api-arg"
  optionHelp = Tagged "extra arguments to pass to json-api executable"
  optionCLParser = concatMany (mkOptionCLParser mempty)
    where concatMany = fmap (JsonApiArgsOption . concat) . many . fmap unJsonApiArgsOption

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

withTools :: (IO Tools -> TestTree) -> TestTree
withTools tests = do
  askOption $ \(DamlOption damlPath) -> do
  askOption $ \(SandboxOption sandboxPath) -> do
  askOption $ \(SandboxArgsOption sandboxArgs) -> do
  askOption $ \(JsonApiOption jsonApiPath) -> do
  askOption $ \(JsonApiArgsOption jsonApiArgs) -> do
  askOption $ \(DamlLedgerOption damlLedgerPath) -> do
  askOption $ \(DamlTypesOption damlTypesPath) -> do
  askOption $ \(DamlReactOption damlReactPath) -> do
  askOption $ \(MessagingPatchOption messagingPatch) -> do
  askOption $ \(YarnOption yarnPath) -> do
  askOption $ \(PatchOption patchPath) -> do
  let createRunfiles :: IO (FilePath -> FilePath)
      createRunfiles = do
        runfiles <- Bazel.Runfiles.create
        mainWorkspace <- fromMaybe "compatibility" <$> getEnv "TEST_WORKSPACE"
        pure (\path -> Bazel.Runfiles.rlocation runfiles $ mainWorkspace </> path)
  withResource createRunfiles (\_ -> pure ()) $ \locateRunfiles -> do
  let tools = do
        damlBinary <- locateRunfiles <*> pure damlPath
        sandboxBinary <- locateRunfiles <*> pure sandboxPath
        jsonApiBinary <- locateRunfiles <*> pure jsonApiPath
        pure Tools
          { damlBinary
          , sandboxBinary
          , jsonApiBinary
          , sandboxArgs
          , jsonApiArgs
          , damlLedgerPath
          , damlTypesPath
          , damlReactPath
          , messagingPatch
          , yarnPath
          , patchPath
          }
  tests tools

main :: IO ()
main = do
  setEnv "TASTY_NUM_THREADS" "1" True
  let options =
        [ Option @DamlOption Proxy
        , Option @SandboxOption Proxy
        , Option @SandboxArgsOption Proxy
        , Option @JsonApiOption Proxy
        , Option @JsonApiArgsOption Proxy
        , Option @DamlLedgerOption Proxy
        , Option @DamlTypesOption Proxy
        , Option @DamlReactOption Proxy
        , Option @MessagingPatchOption Proxy
        , Option @YarnOption Proxy
        , Option @PatchOption Proxy
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
        -- To keep things as simple as possible, we just use `setEnv`
        -- instead of copying `withEnv` from the `daml` workspace.
        path <- getSearchPath
        setEnv "PATH" (intercalate [searchPathSeparator] (takeDirectory yarnPath : path)) True
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
