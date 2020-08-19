-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Assistant.CreateDamlAppTests (main) where

import Control.Exception.Extra
import Control.Concurrent (forkIO)
import Control.Monad
import Data.Aeson
import Data.Aeson.Extra.Merge
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString as BS
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
import DA.Test.Daml2jsUtils
import DA.Test.Process (callCommandSilent)
import DA.Test.Util

newtype ProjectName = ProjectName String

instance IsOption ProjectName where
    defaultValue = ProjectName "create-daml-app"
    parseValue = Just . ProjectName
    optionName = Tagged "project-name"
    optionHelp = Tagged "name of the project"

npmCmd :: String -> String
npmCmd args = "npm-cli.js " <> args <> " --registry http://localhost:4873"

main :: IO ()
main = withTempDir $ \tmpDir -> do
    limitJvmMemory defaultJvmMemoryLimits
    npm: verdaccio : args <- getArgs
    javaPath <- locateRunfiles "local_jdk/bin"
    oldPath <- getSearchPath
    npmPath <- takeDirectory <$> locateRunfiles (mainWorkspace </> npm)
    verdaccioPath <- locateRunfiles(mainWorkspace </> verdaccio)
    verdaccioConfigPath <- locateRunfiles (mainWorkspace </> "templates" </> "create-daml-app-test-resources" </> "verdaccio-config.yaml")
    verdaccioConfig <- readFile verdaccioConfigPath
    writeFile (tmpDir </> "verdaccio-config.yaml") $ "storage: " <> (tmpDir </> "storage\n") <> verdaccioConfig
    _pid <- forkIO $ callCommandSilent $ (init verdaccioPath) <> " --config " <> tmpDir </> "verdaccio-config.yaml"
    let ingredients = defaultIngredients ++ [includingOptions [Option @ProjectName Proxy]]
    withArgs args (withEnv
        [ ("PATH", Just $ intercalate [searchPathSeparator] (javaPath : npmPath : oldPath))
        , ("TASTY_NUM_THREADS", Just "1")
        ] $ defaultMainWithIngredients ingredients tests)
    -- killThread pid

tests :: TestTree
tests =
    withSdkResource $ \_ ->
    askOption $ \(ProjectName projectName) -> do
    testGroup "Create DAML App tests" [gettingStartedGuideTest projectName | not isWindows]
  where
    gettingStartedGuideTest projectName = testCaseSteps "Getting Started Guide" $ \step ->
      withTempDir $ \tmpDir -> do
        step "Publish TypeScript libraries to local registry"
        tsLibsRoot <- locateRunfiles $ mainWorkspace </> "language-support" </> "ts"
        withCurrentDirectory tmpDir $ do
          forM_ [DamlTypes, DamlLedger, DamlReact] $ \tsLib -> do
            let tsLibName = tsLibraryName tsLib
            let tsLibPath = tsLibsRoot </> tsLibName </> "npm_package"
            callCommandSilent $ npmCmd $ "publish " <> tsLibPath

        step "Create app from template"
        withCurrentDirectory tmpDir $ do
          callCommandSilent $ "daml new " <> projectName <> " --template create-daml-app"
        let cdaDir = tmpDir </> projectName
        -- First test the base application (without the user-added feature).
        -- tsLibsRoot <- locateRunfiles $ mainWorkspace </> "language-support" </> "ts"
        withCurrentDirectory cdaDir $ do
          step "Build DAML model for base application"
          callCommandSilent "daml build"
          step "Run JavaScript codegen"
          callCommandSilent $ "daml codegen js -o daml.js .daml/dist/" <> projectName <> "-0.1.0.dar"
        assertFileDoesNotExist (cdaDir </> "ui" </> "build" </> "index.html")
        withCurrentDirectory (cdaDir </> "ui") $ do
          step "Install dependencies for UI"
          retry 3 (callCommandSilent $ npmCmd "install")
          step "Run linter"
          callCommandSilent $ npmCmd "lint --max-warnings 0"
          step "Build the application UI"
          callCommandSilent $ npmCmd "run-script build"
        assertFileExists (cdaDir </> "ui" </> "build" </> "index.html")

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
          step "Build the new DAML model"
          callCommandSilent "daml build"
          step "Set up TypeScript libraries and Yarn workspaces for codegen again"
          -- setupNpmEnv tmpDir (Workspaces [projectName <> "/daml.js"]) [DamlTypes, DamlLedger]
          step "Run JavaScript codegen for new DAML model"
          callCommandSilent $ "daml codegen js -o daml.js .daml/dist/" <> projectName <> "-0.1.0.dar"
        withCurrentDirectory (cdaDir </> "ui") $ do
          step "Set up libraries and workspaces again for UI build"
          -- setupNpmEnv tmpDir (Workspaces [projectName <> "/ui"]) allTsLibraries
          step "Install UI dependencies again, forcing rebuild of generated code"
          callCommandSilent $ npmCmd "install --force --frozen-lockfile"
          step "Run linter again"
          callCommandSilent $ npmCmd "lint --max-warnings 0"
          step "Build the new UI"
          callCommandSilent $ npmCmd " run-script build"

        -- Run end to end testing for the app.
        withCurrentDirectory (cdaDir </> "ui") $ do
          step "Install Jest, Puppeteer and other dependencies"
          extraDepsFile <- locateRunfiles (mainWorkspace </> "templates" </> "create-daml-app-test-resources" </> "testDeps.json")
          addTestDependencies (cdaDir </> "ui" </> "package.json") extraDepsFile
          retry 3 (callCommandSilent $ npmCmd "install")
          step "Run Puppeteer end-to-end tests"
          testFile <- locateRunfiles (mainWorkspace </> "templates" </> "create-daml-app-test-resources" </> "index.test.ts")
          testFileContent <- T.readFileUtf8 testFile
          T.writeFileUtf8
              (cdaDir </> "ui" </> "src" </> "index.test.ts")
              (T.replace "create-daml-app" (T.pack projectName) testFileContent)
          callCommandSilent $ "CI=yes " <> npmCmd "run test --ci --all"

addTestDependencies :: FilePath -> FilePath -> IO ()
addTestDependencies packageJsonFile extraDepsFile = do
    packageJson <- readJsonFile packageJsonFile
    extraDeps <- readJsonFile extraDepsFile
    let newPackageJson = lodashMerge packageJson extraDeps
    BSL.writeFile packageJsonFile (encode newPackageJson)
  where
    readJsonFile :: FilePath -> IO Value
    readJsonFile path = do
        -- Read file strictly to avoid lock being held when we subsequently write to it.
        content <- BSL.fromStrict <$> BS.readFile path
        case decode content of
            Nothing -> error ("Could not decode JSON object from " <> path)
            Just val -> return val

