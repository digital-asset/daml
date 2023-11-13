-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module Main (main) where

import Bazel.Runfiles qualified
import Control.Exception.Extra
import Control.Monad
import DA.Test.Process
import DA.Test.Tar
import DA.Test.Util
import Data.Aeson qualified as Aeson
import Data.Aeson.Extra qualified as Aeson
import Data.Aeson.Key qualified as AK
import Data.Aeson.KeyMap qualified as KM
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BSL
import Data.Conduit ((.|), runConduitRes)
import Data.Conduit.Combinators qualified as Conduit
import Data.Conduit.Tar qualified as Tar
import Data.Conduit.Zlib qualified as Zlib
import Data.Maybe
import Data.Proxy
import Data.Tagged
import Data.Text qualified as T
import System.Directory.Extra
import System.Environment.Blank
import System.FilePath
import System.IO.Extra
import System.Info.Extra
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
  , npmPath :: FilePath
  , nodePath :: FilePath
  , patchPath :: FilePath
  , testDepsPath :: FilePath
  , testTsPath :: FilePath
  , codegenPath :: FilePath
  }

newtype DamlOption = DamlOption FilePath
instance IsOption DamlOption where
  defaultValue = DamlOption "daml"
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

newtype NpmOption = NpmOption FilePath
instance IsOption NpmOption where
  defaultValue = NpmOption ""
  parseValue = Just . NpmOption
  optionName = Tagged "npm"
  optionHelp = Tagged "path to npm"

newtype NodeOption = NodeOption FilePath
instance IsOption NodeOption where
  defaultValue = NodeOption ""
  parseValue = Just . NodeOption
  optionName = Tagged "node"
  optionHelp = Tagged "path to node"

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

newtype CodegenOption = CodegenOption FilePath
instance IsOption CodegenOption where
  defaultValue = CodegenOption ""
  parseValue = Just . CodegenOption
  optionName = Tagged "codegen"
  optionHelp = Tagged "path to codegen output"

withTools :: (IO Tools -> TestTree) -> TestTree
withTools tests = do
  askOption $ \(DamlOption damlPath) -> do
  askOption $ \(DamlLedgerOption damlLedgerPath) -> do
  askOption $ \(DamlTypesOption damlTypesPath) -> do
  askOption $ \(DamlReactOption damlReactPath) -> do
  askOption $ \(MessagingPatchOption messagingPatch) -> do
  askOption $ \(NpmOption npmPath) -> do
  askOption $ \(NodeOption nodePath) -> do
  askOption $ \(PatchOption patchPath) -> do
  askOption $ \(TestDepsOption testDepsPath) -> do
  askOption $ \(TestTsOption testTsPath) -> do
  askOption $ \(CodegenOption codegenPath) -> do
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
          , npmPath
          , nodePath
          , patchPath
          , testDepsPath
          , testTsPath
          , codegenPath
          }
  tests tools

main :: IO ()
main = withTempDir $ \npmCache -> do
  setEnv "npm_config_cache" npmCache True
  setEnv "TASTY_NUM_THREADS" "1" True
  -- We disable user management since older ledgers do not support it.
  -- We might eventually want to enable it when running against > 1.18 but for now
  -- this also acts as a nice test for running without user management.
  setEnv "REACT_APP_SUPPORTS_USERMANAGEMENT" "true" True
  let options =
        [ Option @DamlOption Proxy
        , Option @DamlLedgerOption Proxy
        , Option @DamlTypesOption Proxy
        , Option @DamlReactOption Proxy
        , Option @MessagingPatchOption Proxy
        , Option @NpmOption Proxy
        , Option @NodeOption Proxy
        , Option @PatchOption Proxy
        , Option @TestDepsOption Proxy
        , Option @TestTsOption Proxy
        , Option @CodegenOption Proxy
        ]
  let ingredients = defaultIngredients ++ [includingOptions options]
  defaultMainWithIngredients ingredients $
    withTools $ \getTools -> do
    testGroup "Create Daml App tests"
        [ test getTools
        ]
  where
    test getTools = testCaseSteps "Getting Started Guide" $ \step -> withTempDir $ \tmpDir' -> do
        -- npm gets confused when the temporary directory is not under 'private' on mac.
        let tmpDir
              | isMac = "/private" <> tmpDir'
              | otherwise = tmpDir'
        Tools{..} <- getTools
        setEnv "CI" "yes" True
        step "Create app from template"
        withCurrentDirectory tmpDir $ do
          callProcess damlBinary ["new", "create-daml-app", "--template", "create-daml-app"]
        let cdaDir = tmpDir </> "create-daml-app"
        let uiDir = cdaDir </> "ui"
        step "Patch the application code with messaging feature"
        withCurrentDirectory cdaDir $ do
          callProcessSilent patchPath ["-p2", "-i", messagingPatch]
          forM_ ["MessageEdit", "MessageList"] $ \messageComponent ->
            assertFileExists ("ui" </> "src" </> "components" </> messageComponent <.> "tsx")
          callProcessSilent "sed" ["-i", "s|--openssl-legacy-provider||g", "ui/package.json"]
        step "Extract codegen output"
        extractTarGz codegenPath $ uiDir </> "daml.js"
        -- we patch all the 'package.json' files to point to the local versions of the TypeScript
        -- libraries.
        genFiles <- listFilesRecursive $ uiDir </> "daml.js"
        forM_ [file | file <- genFiles, takeFileName file == "package.json"] (patchTsDependencies uiDir)
        withCurrentDirectory uiDir $ do
          step "Set up libraries and workspaces"
          setupNpmEnv uiDir [(DamlTypes, damlTypesPath)
                            , (DamlLedger, damlLedgerPath)
                            , (DamlReact, damlReactPath)
                            ]
          step "Install Jest, Puppeteer and other dependencies"
          addTestDependencies "package.json" testDepsPath
          patchTsDependencies uiDir "package.json"
          -- use '--scripts-prepend-node-path' to make sure we are using the correct 'node' binary
          retry 3 (callProcessSilent npmPath ["install", "--scripts-prepend-node-path"])
          step "Run Puppeteer end-to-end tests"
          copyFile testTsPath (uiDir </> "src" </> "index.test.ts")
          -- we need 'npm-cli.js' in the path for the following test
          mbOldPath <- getEnv "PATH"
          setEnv "PATH" (takeDirectory npmPath <> (searchPathSeparator : fromMaybe "" mbOldPath)) True
          callProcess npmPath ["run", "test", "--ci", "--all", "--scripts-prepend-node-path"]

addTestDependencies :: FilePath -> FilePath -> IO ()
addTestDependencies packageJsonFile extraDepsFile = do
    packageJson <- readJsonFile packageJsonFile
    extraDeps <- readJsonFile extraDepsFile
    let newPackageJson = Aeson.lodashMerge packageJson extraDeps
    BSL.writeFile packageJsonFile (Aeson.encode newPackageJson)

readJsonFile :: FilePath -> IO Aeson.Value
readJsonFile path = do
    -- Read file strictly to avoid lock being held when we subsequently write to it.
    content <- BSL.fromStrict <$> BS.readFile path
    case Aeson.decode content of
        Nothing -> error ("Could not decode JSON object from " <> path)
        Just val -> return val

extractTarGz :: FilePath -> FilePath -> IO ()
extractTarGz targz outDir = do
    runConduitRes
        $ Conduit.sourceFile targz
        .| Zlib.ungzip
        .| Tar.untar (restoreFile (\a b -> fail (T.unpack $ a <> b)) outDir)

setupNpmEnv :: FilePath -> [(TsLibrary, FilePath)] -> IO ()
setupNpmEnv uiDir libs = do
  forM_ libs $ \(tsLib, path) -> do
    let name = tsLibraryName tsLib
    let uiLibPath = uiDir </> name
    extractTarGz path uiLibPath
    patchTsDependencies uiDir $ uiLibPath </> "package.json"

-- | Overwrite dependencies to our TypeScript libraries to point to local file dependencies in the
-- 'ui' directory in the specified package.json file.
patchTsDependencies :: FilePath -> FilePath -> IO ()
patchTsDependencies uiDir packageJsonFile = do
  packageJson0 <- readJsonFile packageJsonFile
  case packageJson0 of
    Aeson.Object packageJson ->
      case KM.lookup "dependencies" packageJson of
        Just (Aeson.Object dependencies) -> do
          let depNames = KM.keys dependencies
          -- patch dependencies to point to local files if they are present in the package.json
          let patchedDeps =
                KM.fromList
                  ([ ( "@daml.js/create-daml-app"
                     , Aeson.String $
                       T.pack $
                       "file:" <> "./daml.js/create-daml-app-0.1.0")
                   | "@daml.js/create-daml-app" `elem` depNames
                   ] ++
                   [ (depName, Aeson.String $ T.pack $ "file:" <> libRelPath)
                   | tsLib <- allTsLibraries
                   , let libName = tsLibraryName tsLib
                   , let libPath = uiDir </> libName
                   , let libRelPath =
                           makeRelative (takeDirectory packageJsonFile) libPath
                   , let depName = AK.fromText $ T.replace "-" "/" $ T.pack $ "@" <> libName
                   , depName `elem` depNames
                   ]) `KM.union`
                dependencies
          let newPackageJson =
                Aeson.Object $
                KM.insert "dependencies" (Aeson.Object patchedDeps) packageJson
          -- Make sure we have write permissions before writing
          p <- getPermissions packageJsonFile
          setPermissions packageJsonFile (setOwnerWritable True p)
          BSL.writeFile packageJsonFile (Aeson.encode newPackageJson)
        Nothing -> pure () -- Nothing to patch
        _otherwise -> error $ "malformed package.json:" <> show packageJson
    _otherwise -> error $ "malformed package.json:" <> show packageJson0

data TsLibrary
    = DamlLedger
    | DamlReact
    | DamlTypes
    deriving (Bounded, Enum)

allTsLibraries :: [TsLibrary]
allTsLibraries = [minBound .. maxBound]

tsLibraryName :: TsLibrary -> String
tsLibraryName = \case
    DamlLedger -> "daml-ledger"
    DamlReact -> "daml-react"
    DamlTypes -> "daml-types"
