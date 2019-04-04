-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE TypeOperators     #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TemplateHaskell     #-}

module DA.Sdk.Cli.Repository.Bintray.Server
    ( runMockServerBasedTest
    -- exported for testing
    , mkDAExternalFilePath
    )  where

import qualified DA.Sdk.Cli.Repository.Types                  as Ty
import           DA.Sdk.Cli.Repository.Bintray.API            ( BintrayManagementAPIWithAuth
                                                              , BintrayDownloadAPIWithAuth
                                                              , BintrayManagementAPINoAuth
                                                              , BintrayDownloadAPINoAuth
                                                              )
import qualified DA.Sdk.Cli.Repository.Bintray.Server.Storage as Storage
import           DA.Sdk.Prelude                               hiding (split)
import           DA.Sdk.Cli.Repository.Bintray.Server.Types
import qualified Data.ByteString                              as BS
import qualified Data.ByteString.Lazy                         as BSL
import           Data.Text                                    (Text, pack, unpack, split)
import           Data.Text.Encoding                           (encodeUtf8)
import           Network.Wai.Handler.Warp                     (run)
import           Servant
import           Control.Monad.Logger                         (logDebug)
import qualified System.IO.Temp                               as Tmp
import qualified Filesystem.Path.CurrentOS                    as FS

runMockServerBasedTest :: [String] -> IO ()
runMockServerBasedTest artifacts =
    Tmp.withSystemTempDirectory "bintray-mock" $ go artifacts . confFromBaseDir . stringToPath
  where
    -- Note that we need multiple versions of da.run (DA Cli installer)
    go [daCli, daCli1, daCli2, daCli3, daCli4] c = do
      Storage.createTables c
      let externalFilePaths = [ mkDACli "0-aaa" Nothing (stringToPath daCli)
                              , mkDACli "0-aaa" (Just "osx") (stringToPath daCli)
                              , mkDACli "0-aaa" (Just "linux") (stringToPath daCli)
                              ]
      Storage.persistConfig c externalFilePaths
      Storage.initPackageMetadata c externalFilePaths
      run 8882 $ bintrayServer c (stringToPath <$> [daCli, daCli1, daCli2, daCli3, daCli4])
    go otherArgs _ = ioError $ userError ("Incorrect number of arguments: " <>
                                        (show $ length otherArgs) <> " needed: 15")

-- Note that SDK Assistant's (DA Cli) path is passed around to be able to make new
-- mappings while the server is running. This mechanism can be observed from outside
-- like new SDK Assistant versions were made available.
-- We maintain a list of DA Cli paths where different versions in increasing order
-- should be present, like: 0-aaa, 1-aaa, 2-aaa
bintrayServer :: ServerConf -> [FilePath] -> Application
bintrayServer c daCliPathList = serveWithContext bintrayMockAPI basicAuthServerContext server
  where
    server = hoistServerWithContext bintrayMockAPI basicAuthServerProxy (runServerM c) (bintrayManagementServer c daCliPathList)
    basicAuthServerProxy = Proxy :: Proxy '[BasicAuthCheck BasicAuthData]

bintrayMockAPI :: Proxy BintrayMockAPI
bintrayMockAPI = Proxy

type MakeNewSdkAssistantAvailableAPI =
    "makeNewSdkAssistantAvailable"
 :> Get '[JSON] NoContent

type SetupPackageAPI =
    "setupPackage"
 :> Capture "package" Ty.PackageName
 :> Capture "version" Ty.GenericVersion
 :> CaptureAll "filepath" Text
 :> ReqBody '[PlainText] Text
 :> Post '[PlainText] NoContent

type PingAPI =
    "ping"
 :> Get '[JSON] Text

type MockOperationAPI =
        MakeNewSdkAssistantAvailableAPI
   :<|> SetupPackageAPI
   :<|> PingAPI

type BintrayMockAPI = BintrayManagementAPINoAuth :<|> BintrayManagementAPIWithAuth
                 :<|>  BintrayDownloadAPINoAuth :<|> BintrayDownloadAPIWithAuth :<|> MockOperationAPI

bintrayManagementServer :: ServerConf -> [FilePath] -> ServerT BintrayMockAPI ServerM
bintrayManagementServer c daCliPathList =
        (handleGetReleaseAttributes
    :<|> handleGetPackageAttributes
    :<|> handleUpdateReleaseAttributes
    :<|> handleUpdatePackageAttributes
    :<|> handleAttributeSearch
    :<|> handleGetPackage
    :<|> handleCreatePackage
    :<|> handleUploadRelease
    :<|> handleGetRepositories
    :<|> handleGetVersionFiles
    :<|> handleGetLatestVersion)
    :<|> (const handleGetReleaseAttributes
    :<|> const handleGetPackageAttributes
    :<|> const handleUpdateReleaseAttributes
    :<|> const handleUpdatePackageAttributes
    :<|> const handleAttributeSearch
    :<|> const handleGetPackage
    :<|> const handleCreatePackage
    :<|> const handleUploadRelease
    :<|> const handleGetRepositories
    :<|> const handleGetVersionFiles
    :<|> const handleGetLatestVersion)
    :<|> handleDownload
    :<|> const handleDownload
    :<|> (handleMakeNewSdkAssistantAvailable c daCliPathList
    :<|> handleSetupPackage c
    :<|> handlePing)

handleGetReleaseAttributes ::
       Ty.Subject
    -> Ty.Repository
    -> Ty.PackageName
    -> Ty.GenericVersion
    -> ServerM [Ty.Attribute]
handleGetReleaseAttributes subject repository packageName version =
    Storage.getAttributes subject repository packageName (Just version)

handleGetPackageAttributes ::
       Ty.Subject
    -> Ty.Repository
    -> Ty.PackageName
    -> ServerM [Ty.Attribute]
handleGetPackageAttributes subject repository packageName =
    Storage.getAttributes subject repository packageName Nothing

handleUpdateReleaseAttributes ::
       Ty.Subject
    -> Ty.Repository
    -> Ty.PackageName
    -> Ty.GenericVersion
    -> [Ty.Attribute]
    -> ServerM [Ty.Attribute]
handleUpdateReleaseAttributes subject repository packageName version attributes = do
    Storage.updateAttributes subject repository packageName (Just version) attributes
    return [] -- we return a dummy result for now

handleCreatePackage ::
       Ty.Subject
    -> Ty.Repository
    -> Ty.PackageDescriptor
    -> ServerM NoContent
handleCreatePackage subject repository packageDescr = do
    Storage.createPackage subject repository packageDescr
    return NoContent

handleGetPackage ::
       Ty.Subject
    -> Ty.Repository
    -> Ty.PackageName
    -> Maybe Ty.BitFlag
    -> ServerM Ty.BintrayPackage
handleGetPackage subject repository packageName _ =
    Storage.getPackage subject repository packageName >>= \case
        Nothing      -> throwError err404
        Just package -> return package

handleUploadRelease ::
       Ty.Subject
    -> Ty.Repository
    -> Ty.PackageName
    -> Ty.GenericVersion
    -> [Text]
    -> Maybe Ty.BitFlag
    -> BS.ByteString
    -> ServerM NoContent
handleUploadRelease subject repository packageName version filePath _ content = do
    Storage.uploadContent subject repository packageName version filePath content
    return NoContent

handleSetupPackage ::
       ServerConf
    -> Ty.PackageName
    -> Ty.GenericVersion
    -> [Text]
    -> Text
    -> ServerM NoContent
handleSetupPackage c packageName version filePath localPath = do
    $logDebug ("handleSetupPackage: Setting up: " <> pack (show packageName) <> "(" <> pack (show version) <> ")" <> " at " <> localPath)
    let filePath' = pack $ FS.encodeString $ FS.concat $ map FS.fromText filePath
        newPkgPath = [ mkDAExternalFilePath packageName version filePath' (textToPath localPath) ]
    liftIO $ Storage.persistConfig c newPkgPath
    liftIO $ Storage.initPackageMetadata c newPkgPath
    return NoContent

handlePing :: ServerM Text
handlePing = return "pong"

handleUpdatePackageAttributes ::
       Ty.Subject
    -> Ty.Repository
    -> Ty.PackageName
    -> [Ty.Attribute]
    -> ServerM NoContent
handleUpdatePackageAttributes subject repository packageName attributes = do
    Storage.updateAttributes subject repository packageName Storage.noVersion attributes
    return NoContent

handleAttributeSearch ::
       Ty.Subject
    -> Ty.Repository
    -> [Ty.SearchAttribute]
    -> Maybe Ty.BitFlag
    -> ServerM [Ty.PackageInfo]
handleAttributeSearch subject repository searchAttributes _ =
    Storage.attributeSearch subject repository searchAttributes

handleGetVersionFiles ::
       Ty.Subject
    -> Ty.Repository
    -> Ty.PackageName
    -> Ty.GenericVersion
    -> Maybe Ty.BitFlag
    -> ServerM [Ty.PkgFileInfo]
handleGetVersionFiles subject repository packageName version _ =
    Storage.getVersionFiles subject repository packageName version

handleGetRepositories ::
       Ty.Subject
    -> ServerM [Ty.Repository]
handleGetRepositories subject =
    Storage.getRepositories subject

handleGetLatestVersion ::
       Ty.Subject
    -> Ty.Repository
    -> Ty.PackageName
    -> ServerM Ty.LatestVersion
handleGetLatestVersion subject repository packageName =
    Storage.getLatestVersion subject repository packageName

handleDownload ::
       Ty.Subject
    -> Ty.Repository
    -> [Text]
    -> ServerM BS.ByteString
handleDownload subject repository fpath = do
    Storage.downloadContent subject repository fpath

handleMakeNewSdkAssistantAvailable :: ServerConf -> [FilePath] -> ServerM NoContent
handleMakeNewSdkAssistantAvailable c daCliPathList = do
    $logDebug "handleMakeNewSdkAssistantAvailable: Making a new version available"
    latest <- Storage.getLatestVersion daSubject daRepo (Ty.PackageName "da-cli")
    let (Ty.LatestVersion (Ty.GenericVersion latestTxt)) = latest
    case getNextVersion latestTxt of
      Just newVsnNum -> do
        let newVsn = (pack $ show newVsnNum) <> "-aaa"
        $logDebug ("handleMakeNewSdkAssistantAvailable: the following should be available: " <> newVsn)
        daCliPath <- maybe (throwError err404 { errBody = BSL.fromStrict $ encodeUtf8 ("Cli version " <>
                                                                        newVsn <> " is not available.") })
                           return
                           $ daCliPathList `atMay` newVsnNum
        let new    = mkDACli newVsn Nothing daCliPath
            newOsX = mkDACli newVsn (Just "osx") daCliPath
            newLx  = mkDACli newVsn (Just "linux") daCliPath
        liftIO $ Storage.persistConfig c [new, newOsX, newLx]
        liftIO $ Storage.initPackageMetadata c [new, newOsX, newLx]
        return NoContent
      Nothing ->
        throwError $ err404 { errBody = "Cannot find out next version, latest version: " <>
                                        (BSL.fromStrict $ encodeUtf8 $ pack $ show latest) }
  where
    getNextVersion v = do
        let splitted = split (== '-') v
        anIntStr <- headMay splitted
        anInt    <- (readMay $ unpack anIntStr) :: Maybe Int
        return (anInt + 1)

-- | Fake authorization, we always authorized users regardless of username or password
dummyAuthCheck :: BasicAuthCheck BasicAuthData
dummyAuthCheck = BasicAuthCheck (return . Authorized)

-- | We need to supply our handlers with the right Context. In this case,
-- Basic Authentication requires a Context Entry with the 'BasicAuthCheck' value.
-- This context is then supplied to 'server' and threaded
-- to the BasicAuth HasServer handlers.
basicAuthServerContext :: Context (BasicAuthCheck BasicAuthData ': '[])
basicAuthServerContext = dummyAuthCheck :. EmptyContext

-- ############################################################################

daSubject :: Ty.Subject
daSubject = Ty.Subject "digitalassetsdk"

daRepo :: Ty.Repository
daRepo = Ty.Repository "DigitalAssetSDK"

mkDAExternalFilePath :: Ty.PackageName
                     -> Ty.GenericVersion
                     -> Text
                     -> FilePath
                     -> ExternalFilePath
mkDAExternalFilePath packageName version serverFilePath localFilePath =
    ExternalFilePath { efpSubject        = daSubject
                     , efpRepo           = daRepo
                     , efpPackageName    = packageName
                     , efpVersion        = version
                     , efpServerFilePath = serverFilePath
                     , efpLocalFilePath  = localFilePath
                     }

mkDACli :: Text -> Maybe Text -> FilePath -> ExternalFilePath
mkDACli vsn os daCli = mkDAExternalFilePath (Ty.PackageName "da-cli") (Ty.GenericVersion vsn)
                     ("com/digitalasset/da-cli/" <> vsn <> "/da-cli-" <> vsn <>
                     maybe "" (\osSpec -> "-" <> osSpec) os <>
                      ".run") daCli
