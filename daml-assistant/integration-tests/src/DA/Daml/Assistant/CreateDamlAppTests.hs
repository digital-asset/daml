-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Assistant.CreateDamlAppTests (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

--import Conduit
import Control.Exception.Extra
import Control.Monad
import Data.Aeson
import qualified Data.Aeson.Key as A
import qualified Data.Aeson.KeyMap as KM
import Data.Aeson.Extra.Merge
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString as BS
--import qualified Data.Conduit.Tar.Extra as Tar.Conduit.Extra
import Data.List.Extra
import Data.Proxy (Proxy (..))
import Data.Tagged (Tagged (..))
import qualified Data.Text.Extended as T
import System.Directory.Extra
import System.Environment.Blank
import System.FilePath
import System.IO.Extra
import System.Info.Extra
import Test.Tasty
import Test.Tasty.HUnit
import Test.Tasty.Options

import DA.Bazel.Runfiles
import DA.Daml.Assistant.IntegrationTestUtils
import DA.Directory
import DA.Test.Daml2jsUtils
import DA.Test.Process (callCommandSilent)
import DA.Test.Util

newtype ProjectName = ProjectName String

instance IsOption ProjectName where
    defaultValue = ProjectName "create-daml-app"
    parseValue = Just . ProjectName
    optionName = Tagged "project-name"
    optionHelp = Tagged "name of the project"

main :: IO ()
main = withTempDir $ \npmCache -> do
    setEnv "npm_config_cache" npmCache True
    limitJvmMemory defaultJvmMemoryLimits
    npm : node : grpcurl : args <- getArgs
    javaPath <- locateRunfiles "local_jdk/bin"
    oldPath <- getSearchPath
    npmPath <- takeDirectory <$> locateRunfiles (mainWorkspace </> npm)
    -- we need node in scope for the post install step of babel
    nodePath <- takeDirectory <$> locateRunfiles (mainWorkspace </> node)
    grpcurlPath <- takeDirectory <$> locateRunfiles (mainWorkspace </> grpcurl)
    let ingredients = defaultIngredients ++ [includingOptions [Option @ProjectName Proxy]]
    withArgs args (withEnv
        [ ("PATH", Just $ intercalate [searchPathSeparator] (javaPath : npmPath : nodePath : grpcurlPath : oldPath))
        , ("TASTY_NUM_THREADS", Just "1")
        ] $ defaultMainWithIngredients ingredients tests)

tests :: TestTree
tests =
    withSdkResource $ \_ ->
    askOption $ \(ProjectName projectName) -> do
    testGroup "Create Daml App tests" [gettingStartedGuideTest projectName]
  where
    gettingStartedGuideTest projectName = testCaseSteps "Getting Started Guide" $ \step ->
      withTempDir $ \tmpDir' -> do
        -- npm gets confused when the temp dir is not under /private on mac.
        let tmpDir
               | isMac = "/private" <> tmpDir'
               | otherwise = tmpDir'
        step "Create app from template"
        withCurrentDirectory tmpDir $ do
          callCommandSilent $ "daml new " <> projectName <> " --template create-daml-app"
        let cdaDir = tmpDir </> projectName
        let uiDir = cdaDir </> "ui"
        -- We need all local libraries to be in the root dir of the node project
        setupNpmEnv uiDir
        -- First test the base application (without the user-added feature).
        withCurrentDirectory cdaDir $ do
          step "Build Daml model for base application"
          callCommandSilent "daml build --ghc-option=-Werror"
          step "Run JavaScript codegen"
          callCommandSilent $ "daml codegen js -o ui/daml.js .daml/dist/" <> projectName <> "-0.1.0.dar"
          -- We patch all package.json files to point to local files for our TypeScript libraries.
          genFiles <- listFilesRecursive "ui/daml.js"
          forM_ [file | file <- genFiles, takeFileName file == "package.json"] (patchTsDependencies uiDir)
        assertFileDoesNotExist (uiDir </> "build" </> "index.html")
        withCurrentDirectory uiDir $ do
          patchTsDependencies uiDir "package.json"
          extraDepsFile <- locateRunfiles (mainWorkspace </> "templates" </> "create-daml-app-test-resources" </> "testDeps.json")
          addTestDependencies (uiDir </> "package.json") extraDepsFile
          step "Install dependencies for UI"
          createDirectoryIfMissing True "node_modules"
          cachedDeps <- locateRunfiles (mainWorkspace </> "daml-assistant" </> "integration-tests" </> "create_daml_app_deps.tar")
          -- This crashes when updating NodeJS and I don't have time or
          -- interest to debug it.
          --runConduitRes
          --    $ sourceFileBS t grep sourceFileBS
          --    cachedDeps
          --    .| Tar.Conduit.Extra.untar (Tar.Conduit.Extra.restoreFile (\a b -> fail (T.unpack $ a <> " " <> b)) ".")
          callCommandSilent ("tar --strip-components=1 -x -f " <> cachedDeps)
          retry 3 (callCommandSilent "npm-cli.js install")
          step "Run linter"
          callCommandSilent "npm-cli.js run-script lint -- --max-warnings 0"
          step "Build the application UI"
          callCommandSilent "npm-cli.js run-script build"
        assertFileExists (uiDir </> "build" </> "index.html")

        -- Now test that the messaging feature works by applying the necessary
        -- changes and testing in the same way as above.
        step "Patch the application code with messaging feature"
        messagingPatch <- locateRunfiles (mainWorkspace </> "templates" </> "create-daml-app-test-resources" </> "messaging.patch")
        patchTool <- locateRunfiles "patch_dev_env/bin/patch"
        withCurrentDirectory cdaDir $ do
          patchContent <- T.readFileUtf8 messagingPatch
          T.writeFileUtf8 (cdaDir </> "messaging.patch") (T.replace "create-daml-app" (T.pack projectName) patchContent)
          callCommandSilent $ unwords [patchTool, "-s", "-p2", "<", cdaDir </> "messaging.patch"]
          forM_ ["MessageEdit", "MessageList"] $ \messageComponent ->
            assertFileExists ("ui" </> "src" </> "components" </> messageComponent <.> "tsx")
          step "Build the new Daml model"
          callCommandSilent "daml build --ghc-option=-Werror"
          step "Run JavaScript codegen for new Daml model"
          callCommandSilent $ "daml codegen js -o ui/daml.js .daml/dist/" <> projectName <> "-0.1.0.dar"
          genFiles <- listFilesRecursive "ui/daml.js"
          forM_ [file | file <- genFiles, takeFileName file == "package.json"] (patchTsDependencies uiDir)
        withCurrentDirectory uiDir $ do
          patchTsDependencies uiDir "package.json"
          step "Run linter again"
          callCommandSilent "npm-cli.js run-script lint -- --max-warnings 0"
          step "Build the new UI"
          callCommandSilent "npm-cli.js run-script build"

        -- Run end to end testing for the app.
        withCurrentDirectory (cdaDir </> "ui") $ do
          step "Run Puppeteer end-to-end tests"
          testFile <- locateRunfiles (mainWorkspace </> "templates" </> "create-daml-app-test-resources" </> "index.test.ts")
          testFileContent <- T.readFileUtf8 testFile
          T.writeFileUtf8
              (uiDir </> "src" </> "index.test.ts")
              (T.replace "\"npm\"" "\"npm-cli.js\"" (T.replace "create-daml-app" (T.pack projectName) testFileContent))
          -- patch daml.yaml, remove JavaScript code generation entry so that patched generated code
          -- is not overwritten
          let damlYaml = cdaDir </> "daml.yaml"
          content <- readFileUTF8' damlYaml
          writeFileUTF8 damlYaml $ unlines $ dropEnd 4 $ lines content
          callCommandSilent "CI=yes npm-cli.js run-script test -- --ci --all"

addTestDependencies :: FilePath -> FilePath -> IO ()
addTestDependencies packageJsonFile extraDepsFile = do
    packageJson <- readJsonFile packageJsonFile
    extraDeps <- readJsonFile extraDepsFile
    let newPackageJson = lodashMerge packageJson extraDeps
    BSL.writeFile packageJsonFile (encode newPackageJson)

readJsonFile :: FilePath -> IO Value
readJsonFile path = do
    -- Read file strictly to avoid lock being held when we subsequently write to it.
    content <- BSL.fromStrict <$> BS.readFile path
    case decode content of
        Nothing -> error ("Could not decode JSON object from " <> path)
        Just val -> return val

setupNpmEnv :: FilePath -> IO ()
setupNpmEnv uiDir = do
  tsLibsRoot <- locateRunfiles $ mainWorkspace </> "language-support" </> "ts"
  forM_ allTsLibraries $ \tsLib -> do
    let name = tsLibraryName tsLib
    let uiLibPath = uiDir </> name
    copyDirectory (tsLibsRoot </> name </> "npm_package") uiLibPath
    patchTsDependencies uiDir $ uiLibPath </> "package.json"

-- | Overwrite dependencies to our ts libraries to point to local file dependencies in the ui
-- director in the specified package.json file.
patchTsDependencies :: FilePath -> FilePath -> IO ()
patchTsDependencies uiDir packageJsonFile = do
  packageJson0 <- readJsonFile packageJsonFile
  case packageJson0 of
    Object packageJson ->
      case KM.lookup "dependencies" packageJson of
        Just (Object dependencies) -> do
          let depNames = KM.keys dependencies
          let patchedDeps =
                KM.fromList
                  [ (depName, String $ T.pack $ "file:" <> libRelPath)
                  | tsLib <- allTsLibraries
                  , let libName = tsLibraryName tsLib
                  , let libPath = uiDir </> libName
                  , let libRelPath =
                          makeRelative (takeDirectory packageJsonFile) libPath
                  , let depName = A.fromString $ "@" <> replace "-" "/" libName
                  , depName `elem` depNames
                  ] `KM.union`
                dependencies
          let newPackageJson =
                Object $
                KM.insert "dependencies" (Object patchedDeps) packageJson
          p <- getPermissions packageJsonFile
          setPermissions packageJsonFile (setOwnerWritable True p)
          BSL.writeFile packageJsonFile (encode newPackageJson)
        Nothing -> pure () -- Nothing to patch
        _otherwise -> error $ "malformed package.json:" <> show packageJson
    _otherwise -> error $ "malformed package.json:" <> show packageJson0
