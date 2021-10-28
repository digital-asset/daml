-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import Control.Applicative
import DA.Test.Process
import Data.Either.Extra
import Data.Function ((&))
import Data.Maybe (fromMaybe)
import Data.Proxy (Proxy (..))
import Data.SemVer (Version)
import qualified Data.SemVer as SemVer
import Data.Tagged (Tagged (..))
import System.Directory.Extra (withCurrentDirectory)
import System.Environment (lookupEnv)
import System.Environment.Blank (setEnv)
import System.FilePath ((</>), takeBaseName)
import System.IO.Extra (withTempDir,writeFileUTF8)
import Test.Tasty (TestTree,askOption,defaultMainWithIngredients,defaultIngredients,includingOptions,testGroup,withResource)
import Test.Tasty.Options (IsOption(..), OptionDescription(..), mkOptionCLParser)
import Test.Tasty.HUnit
import qualified Bazel.Runfiles
import qualified Data.Aeson as Aeson
import qualified Data.List as List
import qualified Data.Map as Map
import qualified Data.Text as T
import qualified Web.JWT as JWT

import Sandbox

data Tools = Tools
  { daml :: FilePath
  , sandboxConfig :: SandboxConfig
  }

newtype DamlOption = DamlOption FilePath
instance IsOption DamlOption where
  defaultValue = DamlOption "daml"
  parseValue = Just . DamlOption
  optionName = Tagged "daml"
  optionHelp = Tagged "runfiles path to the daml executable"

newtype SandboxOption = SandboxOption FilePath
instance IsOption SandboxOption where
  defaultValue = SandboxOption "sandbox"
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

newtype CertificatesOption = CertificatesOption FilePath
instance IsOption CertificatesOption where
  defaultValue = CertificatesOption "certificates"
  parseValue = Just . CertificatesOption
  optionName = Tagged "certs"
  optionHelp = Tagged "runfiles path to the certificates directory"

withTools :: (IO Tools -> TestTree) -> TestTree
withTools tests = do
  askOption $ \(DamlOption damlPath) -> do
  askOption $ \(SandboxOption sandboxPath) -> do
  askOption $ \(SandboxArgsOption sandboxArgs) -> do
  askOption $ \(CertificatesOption certificatesPath) -> do
  let createRunfiles :: IO (FilePath -> FilePath)
      createRunfiles = do
        runfiles <- Bazel.Runfiles.create
        mainWorkspace <- fromMaybe "compatibility" <$> lookupEnv "TEST_WORKSPACE"
        pure (\path -> Bazel.Runfiles.rlocation runfiles $ mainWorkspace </> path)
  withResource createRunfiles (\_ -> pure ()) $ \locateRunfiles -> do
  let tools = do
        daml <- locateRunfiles <*> pure damlPath
        sandboxBinary <- locateRunfiles <*> pure sandboxPath
        sandboxCertificates <- locateRunfiles <*> pure certificatesPath
        let sandboxConfig = defaultSandboxConf
              { sandboxBinary
              , sandboxArgs
              , sandboxCertificates
              }
        pure Tools
          { daml
          , sandboxConfig
          }
  tests tools

-- | This is the version of daml-helper.
newtype SdkVersion = SdkVersion Version
  deriving Eq

instance Ord SdkVersion where
    SdkVersion x <= SdkVersion y
      | y == SemVer.initial = True -- 0.0.0 is >= than anything else
      | x == SemVer.initial = False -- 0.0.0 not <= than anything other than 0.0.0
      | otherwise = x <= y -- regular semver comparison

instance IsOption SdkVersion where
  defaultValue = SdkVersion SemVer.initial
  -- Tasty seems to force the value somewhere so we cannot just set this
  -- to `error`. However, this will always be set.
  parseValue = either (const Nothing) (Just . SdkVersion) . SemVer.fromText . T.pack
  optionName = Tagged "sdk-version"
  optionHelp = Tagged "The SDK version number"

main :: IO ()
main = do
  -- We manipulate global state via the working directory
  -- so running tests in parallel will cause trouble.
  setEnv "TASTY_NUM_THREADS" "1" True
  let options =
        [ Option @DamlOption Proxy
        , Option @SandboxOption Proxy
        , Option @SandboxArgsOption Proxy
        , Option @CertificatesOption Proxy
        , Option @SdkVersion Proxy
        ]
  let ingredients = defaultIngredients ++ [includingOptions options]
  defaultMainWithIngredients ingredients $
    withTools $ \getTools -> do
    askOption $ \sdkVersion -> do
    testGroup "Deployment"
      [ authenticatedUploadTest sdkVersion getTools
      , unauthenticatedTests sdkVersion getTools
      ]

-- | Test `daml ledger list-parties --access-token-file`
authenticatedUploadTest :: SdkVersion -> IO Tools -> TestTree
authenticatedUploadTest sdkVersion getTools = do
  withSandbox getSandboxConfig $ \getSandboxPort ->  testGroup "authentication" $
    [ testCase "Bearer prefix" $ do
          Tools{..} <- getTools
          port <- getSandboxPort
          withTempDir $ \deployDir -> do
            withCurrentDirectory deployDir $ do
              let tokenFile = deployDir </> "secretToken.jwt"
              -- The trailing newline is not required but we want to test that it is supported.
              writeFileUTF8 tokenFile ("Bearer " <> makeSignedJwt sharedSecret <> "\n")
              callProcessSilent daml
                [ "ledger", "list-parties"
                , "--access-token-file", tokenFile
                , "--host", "localhost", "--port", show port
                ]
    ] <>
    [ testCase "no Bearer prefix" $ do
          Tools{..} <- getTools
          port <- getSandboxPort
          withTempDir $ \deployDir -> do
            withCurrentDirectory deployDir $ do
              let tokenFile = deployDir </> "secretToken.jwt"
              -- The trailing newline is not required but we want to test that it is supported.
              writeFileUTF8 tokenFile (makeSignedJwt sharedSecret <> "\n")
              callProcessSilent daml
                [ "ledger", "list-parties"
                , "--access-token-file", tokenFile
                , "--host", "localhost", "--port", show port
                ]
    | supportsNoBearerPrefix sdkVersion
    ]
  where
    sharedSecret = "TheSharedSecret"
    getSandboxConfig = do
        cfg <- sandboxConfig <$> getTools
        pure cfg { mbSharedSecret = Just sharedSecret }

makeSignedJwt :: String -> String
makeSignedJwt sharedSecret = do
  let urc = JWT.ClaimsMap $ Map.fromList [ ("admin", Aeson.Bool True)]
  let cs = mempty { JWT.unregisteredClaims = urc }
  let key = JWT.hmacSecret $ T.pack sharedSecret
  let text = JWT.encodeSigned key mempty cs
  T.unpack text

unauthenticatedTests :: SdkVersion -> IO Tools -> TestTree
unauthenticatedTests sdkVersion getTools = do
    withSandbox (sandboxConfig <$> getTools) $ \getSandboxPort ->
        testGroup "unauthenticated"
            [ fetchTest sdkVersion getTools getSandboxPort
            ]

-- | Test `daml ledger fetch-dar`
fetchTest :: SdkVersion -> IO Tools -> IO Int -> TestTree
fetchTest sdkVersion getTools getSandboxPort = do
    testCaseSteps "fetchTest" $ \step -> do
    Tools{..} <- getTools
    port <- getSandboxPort
    withTempDir $ \fetchDir -> do
      withCurrentDirectory fetchDir $ do
        writeMinimalProject sdkVersion
        let origDar = ".daml/dist/proj1-0.0.1.dar"
        step "build/upload"
        callProcessSilent daml ["damlc", "build"]
        callProcessSilent daml $
          [ "ledger", "upload-dar"
          , "--host", "localhost" , "--port" , show port
          , origDar
          ] <> ["--timeout=120" | supportsTimeout sdkVersion]
        pid <- getMainPidOfDar daml origDar
        step "fetch/validate"
        let fetchedDar = "fetched.dar"
        callProcessSilent daml $
          [ "ledger", "fetch-dar"
          , "--host", "localhost" , "--port", show port
          , "--main-package-id", pid
          , "-o", fetchedDar
          ]  <> ["--timeout=120" | supportsTimeout sdkVersion]
        callProcessSilent daml ["damlc", "validate-dar", fetchedDar]

-- | Discover the main package-identifier of a dar.
--
-- Parses the output of damlc inspect-dar. Unfortunately, this output is not
-- currently optimized for machine readability. This function expects the
-- following format.
--
-- @
--   ...
--
--   DAR archive contains the following packages:
--
--   ...
--   proj1-0.0.1-... "<package-id>"
--   ...
-- @
getMainPidOfDar :: FilePath -> FilePath -> IO String
getMainPidOfDar daml fp = do
  darContents <- callProcessForStdout daml ["damlc", "inspect-dar", fp]
  let packageName = takeBaseName fp
  let mbPackageLine =
        darContents
        & lines
        & dropWhile (not . List.isInfixOf "DAR archive contains the following packages")
        & drop 1
        & List.find (List.isPrefixOf packageName)
  let mbPackageId = do
        line <- mbPackageLine
        [_, quoted] <- pure $ words line
        let stripQuotes = takeWhile (/= '"') . dropWhile (== '"')
        pure $ stripQuotes quoted
  case mbPackageId of
    Nothing -> fail $ "Couldn't determine package ID for " ++ fp
    Just pkgId -> pure pkgId

-- | Write `daml.yaml` and `Main.daml` files in the current directory.
writeMinimalProject :: SdkVersion -> IO ()
writeMinimalProject (SdkVersion sdkVersion) = do
  writeFileUTF8 "daml.yaml" $ unlines
      [ "sdk-version: " <> SemVer.toString sdkVersion
      , "name: proj1"
      , "version: 0.0.1"
      , "source: ."
      , "dependencies:"
      , "  - daml-prim"
      , "  - daml-stdlib"
      ]
  writeFileUTF8 "Main.daml" $ unlines
    [ "module Main where"
    , "template T with p : Party where signatory p"
    ]

supportsNoBearerPrefix :: SdkVersion -> Bool
supportsNoBearerPrefix ver =
    ver >= SdkVersion (fromRight' $ SemVer.fromText "1.1.1")

supportsTimeout :: SdkVersion -> Bool
supportsTimeout ver = ver > SdkVersion (fromRight' $ SemVer.fromText "1.4.0-snapshot.20200715.4733.0.d6e58626")
