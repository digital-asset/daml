-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TemplateHaskell #-}

module Upload (
    uploadToMavenCentral,
    mavenConfigFromEnv,
) where

import qualified Control.Concurrent.Async.Lifted.Safe as Async
import qualified Control.Exception.Safe as E
import           Control.Monad
import           Control.Monad.Logger
import           Control.Monad.IO.Class
import           "cryptohash" Crypto.Hash (Digest, MD5(..), SHA1(..), digestToHexByteString, hash)
import           Control.Retry
import           Data.Aeson
import           Data.Foldable
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Base64 as Base64
import qualified Data.ByteString.Char8 as C8
import qualified Data.List as List
import qualified Data.SemVer as SemVer
import           Data.Text (Text)
import qualified Data.Text as T
import           Data.Text.Encoding (encodeUtf8)
import           Network.Connection (TLSSettings(..))
import           Network.HTTP.Client
import           Network.HTTP.Client.TLS (mkManagerSettings, tlsManagerSettings)
import           Network.HTTP.Simple (setRequestBasicAuth, setRequestBodyFile, setRequestBodyLBS, setRequestHeader, setRequestMethod, setRequestPath)
import           Network.HTTP.Types.Status
import           Path
import           System.Environment
import           System.IO.Temp

import Types
import Util

--
-- Upload the artifacts to Maven Central
--
-- The artifacts are first uploaded to a staging repository on the Sonatype Open Source Repository Hosting platform
-- where the repository contents is verified to conform to the Maven Central standards before being released to
-- the public repository.
--
-- Digitalasset has been assigned the 'com.daml' and 'com.digitalasset' namespaces (group IDs for Maven repos and artifacts
-- need to be uploaded to the staging repository corresponding with their group ID. The staging repository for each group ID
-- is handled separately, hence there are several 'duplicated' REST calls.
--
-- Further information:
--
--  Staging requirements: https://central.sonatype.org/pages/requirements.html
--  Staging REST API: https://oss.sonatype.org/nexus-staging-plugin/default/docs/index.html
--
uploadToMavenCentral :: (MonadCI m) => MavenUploadConfig -> Path Abs Dir -> [(MavenCoords, Path Rel File)] -> m ()
uploadToMavenCentral MavenUploadConfig{..} releaseDir artifacts = do

    -- Note: TLS verification settings switchable by MavenUpload settings
    let managerSettings = if getAllowUnsecureTls mucAllowUnsecureTls then noVerifyTlsManagerSettings else tlsManagerSettings

    -- Create HTTP Connection manager with 2min response timeout as the OSSRH can be slow...
    manager <- liftIO $ newManager managerSettings { managerResponseTimeout = responseTimeoutMicro (120 * 1000 * 1000) }

    parsedUrlRequest <- parseUrlThrow $ T.unpack mucUrl -- Note: Will throw exception on non-2XX responses
    let baseRequest = setRequestMethod "PUT" $ setRequestBasicAuth (encodeUtf8 mucUser) (encodeUtf8 mucPassword) parsedUrlRequest

    decodedSigningKey <- decodeSigningKey mucSigningKey
    -- Security Note: Using the withSystemTempDirectory function to always cleanup the private key data from the filesystems.
    withSystemTempDirectory "gnupg" $ \gnupgTempDir -> do

        -- Write the secret key used for signing into a temporary file and use 'gpg' command line tool to import into
        -- GPG's internal file tree.
        secretKeyImportFile <- liftIO $ emptyTempFile gnupgTempDir "gpg-private-key.asc"
        _ <- liftIO $ BS.writeFile secretKeyImportFile decodedSigningKey

        loggedProcess_ "gpg" [ "--homedir", T.pack gnupgTempDir, "--no-tty", "--quiet", "--import", T.pack secretKeyImportFile ]

        --
        -- Prepare the remote staging repositories
        --
        (comDamlStagingRepoId, comDigitalAssetRepoId) <- prepareStagingRepo baseRequest manager

        --
        -- Upload the artifacts; each with:
        -- 1. PGP signature
        -- 2. SHA1 checksum
        -- 3. MD5 checksum

        for_ artifacts $ \(coords@MavenCoords{..}, file) -> do
            let absFile = releaseDir </> file -- (T.intercalate "/" (groupId <> [artifactId]))

            sigTempFile <- liftIO $ emptySystemTempFile $ T.unpack $ artifactId <> maybe "" ("-" <>) classifier <> "-" <> artifactType <> ".asc"
            -- The "--batch" and "--yes" flags are used to prevent gpg waiting on stdin.
            loggedProcess_ "gpg" [ "--homedir", T.pack gnupgTempDir, "-ab", "-o", T.pack sigTempFile, "--batch", "--yes", T.pack (fromAbsFile absFile) ]

            let artUploadPath = uploadPath coords comDamlStagingRepoId comDigitalAssetRepoId
            (md5Hash, sha1Hash) <- chksumFileContents absFile

            $logInfo ("(Uploading " <> artUploadPath <> " from " <> tshow absFile <> ")")

            let request
                 = setRequestHeader "Content-Type" [ encodeUtf8 $ getContentType artifactType ]
                 $ setRequestPath (encodeUtf8 artUploadPath)
                 $ setRequestBodyFile (fromAbsFile absFile) baseRequest

            let pgpSigRequest
                 = setRequestHeader "Content-Type" [ "text/plain" ]
                 $ setRequestPath (encodeUtf8 $ artUploadPath <> ".asc")
                 $ setRequestBodyFile sigTempFile baseRequest

            let sha1CksumRequest
                 = setRequestHeader "Content-Type" [ "text/plain" ]
                 $ setRequestPath (encodeUtf8 $ artUploadPath <> ".sha1")
                 $ setRequestBodyLBS sha1Hash baseRequest

            let md5CksumRequest
                 = setRequestHeader "Content-Type" [ "text/plain" ]
                 $ setRequestPath (encodeUtf8 $ artUploadPath <> ".md5")
                 $ setRequestBodyLBS md5Hash baseRequest

            (_, _, _, _) <- Async.runConcurrently $ (,,,)
                 <$> Async.Concurrently (recovering uploadRetryPolicy [ httpResponseHandler ] (\_ -> liftIO $ httpNoBody request manager))
                 <*> Async.Concurrently (recovering uploadRetryPolicy [ httpResponseHandler ] (\_ -> liftIO $ httpNoBody pgpSigRequest manager))
                 <*> Async.Concurrently (recovering uploadRetryPolicy [ httpResponseHandler ] (\_ -> liftIO $ httpNoBody sha1CksumRequest manager))
                 <*> Async.Concurrently (recovering uploadRetryPolicy [ httpResponseHandler ] (\_ -> liftIO $ httpNoBody md5CksumRequest manager))

            pure ()

        $logInfo "Finished uploading artifacts"

        -- Now 'finish' the staging and release to Maven Central
        publishStagingRepo baseRequest manager comDamlStagingRepoId comDigitalAssetRepoId

prepareStagingRepo :: (MonadCI m) => Request -> Manager -> m (Text, Text)
prepareStagingRepo baseRequest manager = do

    --
    -- Note in Profile IDs
    --
    -- Currently the profile IDs are hardcoded. The IDs are fixed to the "namespaces" ('com.daml' and 'com.digitialasset')
    -- attached to the Digitalasset accounts on the Sonatype OSSRH.
    --

    --
    -- Open the staging repository profile for uploads.
    --
    -- Opening the staging repositories explicitly instead of implicitly (by simply uploading the artifacts)
    -- allows for better managability (i.e. independant of the current state of the remote repositories which
    -- could be still open due to failures).
    --

    let startComDamlStagingRepoRequest
         = setRequestMethod "POST"
         $ setRequestPath  "/service/local/staging/profiles/b6148ff96bfaaa/start" -- Profile key could be requested
         $ setRequestHeader "content-type" [ "application/json" ]
         $ setRequestHeader "accept" [ "application/json" ]
         $ setRequestBodyLBS (BSL.fromStrict (encodeUtf8 "{\"data\":{\"description\":\"\"}}")) baseRequest

    let startComDigitalassetStagingRepoRequest
         = setRequestMethod "POST"
         $ setRequestPath  "/service/local/staging/profiles/b614bfdbd6b51f/start" -- Profile key could be requested
         $ setRequestHeader "content-type" [ "application/json" ]
         $ setRequestHeader "accept" [ "application/json" ]
         $ setRequestBodyLBS (BSL.fromStrict (encodeUtf8 "{\"data\":{\"description\":\"\"}}")) baseRequest

    (startComDamlStagingReosResponse, startComDigitalassetStagingRepoResponse) <- Async.runConcurrently $ (,)
         <$> Async.Concurrently (recovering uploadRetryPolicy [ httpResponseHandler ] (\_ -> liftIO $ httpLbs startComDamlStagingRepoRequest manager))
         <*> Async.Concurrently (recovering uploadRetryPolicy [ httpResponseHandler ] (\_ -> liftIO $ httpLbs startComDigitalassetStagingRepoRequest manager))
    comDamlStagingRepoInfo <- decodeStagingPromoteResponse startComDamlStagingReosResponse
    comDigitalassetStagingRepoInfo <- decodeStagingPromoteResponse startComDigitalassetStagingRepoResponse

    return (stagedRepositoryId $ _data comDamlStagingRepoInfo, stagedRepositoryId $ _data comDigitalassetStagingRepoInfo)

publishStagingRepo :: (MonadCI m) => Request -> Manager -> Text -> Text -> m ()
publishStagingRepo baseRequest manager comDamlRepoId comDigitalassetRepoId = do

    --
    -- "Close" the staging profiles which initiates the running of the rules that check the uploaded artifacts
    -- for compliance with the Maven Central requirements.
    -- If all the rules pass then the status of the staging repository and profile will become "closed", if anything fails
    -- then the status will remain set to "open".
    --

    let finishComDamlStagingRepoRequest
         = setRequestMethod "POST"
         $ setRequestPath  "/service/local/staging/profiles/b6148ff96bfaaa/finish" -- Profile key could be requested
         $ setRequestHeader "content-type" [ "application/json" ]
         $ setRequestBodyLBS (textToLazyByteString $ "{\"data\":{\"stagedRepositoryId\":\"" <> comDamlRepoId <> "\",\"description\":\"\"}}") baseRequest

    let finishComDigitalassetStagingRepoRequest
         = setRequestMethod "POST"
         $ setRequestPath  "/service/local/staging/profiles/b614bfdbd6b51f/finish" -- Profile key could be requested
         $ setRequestHeader "content-type" [ "application/json" ]
         $ setRequestBodyLBS (textToLazyByteString $ "{\"data\":{\"stagedRepositoryId\":\"" <> comDigitalassetRepoId <> "\",\"description\":\"\"}}") baseRequest

    (_, _) <- Async.runConcurrently $ (,)
         <$> Async.Concurrently (recovering uploadRetryPolicy [ httpResponseHandler ] (\_ -> liftIO $ httpNoBody finishComDamlStagingRepoRequest manager))
         <*> Async.Concurrently (recovering uploadRetryPolicy [ httpResponseHandler ] (\_ -> liftIO $ httpNoBody finishComDigitalassetStagingRepoRequest manager))

    let comDamlStatusReposRequest
         = setRequestMethod "GET"
         $ setRequestPath (encodeUtf8 ("/service/local/staging/repository/" <> comDamlRepoId))
         $ setRequestHeader "accept" [ "application/json" ] baseRequest

    let comDigitalassetStatusReposRequest
         = setRequestMethod "GET"
         $ setRequestPath (encodeUtf8 ("/service/local/staging/repository/" <> comDigitalassetRepoId))
         $ setRequestHeader "accept" [ "application/json" ] baseRequest

    --
    -- Poll until the staging repositories are closed or the staging repositories cease to be "transitioning" to a new state
    --
    (comDamlNotClosed, comDigitalassetNotClosed) <- Async.runConcurrently $ (,)
        <$> Async.Concurrently (recovering checkStatusRetryPolicy [ httpResponseHandler, checkRepoStatusHandler ] (\_ -> handleStatusRequest comDamlStatusReposRequest manager))
        <*> Async.Concurrently (recovering checkStatusRetryPolicy [ httpResponseHandler, checkRepoStatusHandler ] (\_ -> handleStatusRequest comDigitalassetStatusReposRequest manager))

    --
    -- Drop" (delete) both staging repositories if one or more fails the checks (and are not in the "closed" state)
    --
    when (comDamlNotClosed || comDigitalassetNotClosed) $ do
        when comDamlNotClosed $ do logStagingRepositoryActivity baseRequest manager comDamlRepoId
        when comDigitalassetNotClosed $ do logStagingRepositoryActivity baseRequest manager comDigitalassetRepoId
        dropStagingRepositories baseRequest manager [ comDamlRepoId, comDigitalassetRepoId ]
        throwIO $ RepoFailedToClose $ [ comDamlRepoId | comDamlNotClosed ] <> [ comDigitalassetRepoId | comDigitalassetNotClosed ]

    --
    -- Now the final step of releasing the staged artifacts into the wild...
    --
    let releaseStagingReposRequest
         = setRequestMethod "POST"
         $ setRequestPath  "/service/local/staging/bulk/promote"
         $ setRequestHeader "content-type" [ "application/json" ]
         $ setRequestHeader "accept" [ "application/json" ]
         $ setRequestBodyLBS (textToLazyByteString $ "{\"data\":{\"stagedRepositoryIds\":[\"" <> comDamlRepoId <> "\",\"" <> comDigitalassetRepoId <> "\"],\"description\":\"\",\"autoDropAfterRelease\":true}}") baseRequest

    _ <- recovering uploadRetryPolicy [ httpResponseHandler ] (\_ -> liftIO $ httpNoBody releaseStagingReposRequest manager)

    $logWarn "Published to Maven Central"

    pure ()

-- Print out a log of the repository activity which includes details of which verification rule failed.
-- The output is not prettified as it should only be print in rare(ish) error cases.
logStagingRepositoryActivity :: (MonadCI m) => Request -> Manager -> Text -> m ()
logStagingRepositoryActivity baseRequest manager repoId = do

    let repoActivityRequest
         = setRequestMethod "GET"
         $ setRequestPath (encodeUtf8 ("/service/local/staging/repository/" <> repoId <> "/activity"))
         $ setRequestHeader "accept" [ "application/json" ] baseRequest

    activityResponse <- recovering uploadRetryPolicy [ httpResponseHandler ] (\_ -> liftIO $ httpLbs repoActivityRequest manager)
    repoActivity <- decodeRepoActivityResponse activityResponse

    $logWarn ("Failed to process staging repository \"" <> repoId <> "\".  \n" <> (T.intercalate "\n    " $ map tshow repoActivity))

    return ()

dropStagingRepositories :: (MonadCI m) => Request -> Manager -> [Text] -> m  ()
dropStagingRepositories baseRequest manager repoIdList = do
    --
    -- Note: This is a "Bulk Drop" request used by the Nexus UI and not a Staging REST API.
    --
    let dropReposJson = "{\"data\":{\"description\":\"\",\"stagedRepositoryIds\":" <> tshow repoIdList <> "}}"
    let dropReposRequest
         = setRequestMethod "POST"
         $ setRequestPath "/service/local/staging/bulk/drop"
         $ setRequestHeader "content-type" [ "application/json" ]
         $ setRequestHeader "accept" [ "application/json" ]
         $ setRequestBodyLBS (BSL.fromStrict (encodeUtf8 dropReposJson)) baseRequest

    _ <- recovering uploadRetryPolicy [ httpResponseHandler ] (\_ -> liftIO $ httpNoBody dropReposRequest manager)

    return ()

decodeSigningKey :: (MonadCI m) => String -> m BS.ByteString
decodeSigningKey signingKey =  case Base64.decode $ C8.pack signingKey of
    Left err -> throwIO $ CannotDecodeSigningKey err
    Right decodedData -> return decodedData

-- Note: Upload path is NOT documented in the REST API Guide.
uploadPath :: MavenCoords -> Text -> Text -> Text
uploadPath MavenCoords{..} comDamlStagingRepoId comDigitalassetRepoId = do
    let stagingRepoId = if ["com", "daml"] `List.isPrefixOf` groupId then comDamlStagingRepoId else comDigitalassetRepoId
    let v = SemVer.toText version
    T.intercalate "/" ("/service/local/staging/deployByRepositoryId" : [stagingRepoId] <> groupId <> [artifactId, v, artifactId]) <> "-" <> v <> maybe "" ("-" <>) classifier <> "." <> artifactType

getContentType :: ArtifactType -> Text
getContentType t =
  case t of
    "jar" -> "application/java-archive"
    "pom" -> "application/xml"
    _     -> "application/octet-stream"

noVerifyTlsSettings :: TLSSettings
noVerifyTlsSettings = TLSSettingsSimple
    { settingDisableCertificateValidation = True
    , settingDisableSession = True
    , settingUseServerName = False
    }

noVerifyTlsManagerSettings :: ManagerSettings
noVerifyTlsManagerSettings = mkManagerSettings noVerifyTlsSettings Nothing

chksumFileContents :: (MonadIO m) => Path Abs File -> m (BSL.ByteString, BSL.ByteString)
chksumFileContents file = do
    contents <- liftIO $ BS.readFile $ fromAbsFile file
    return (BSL.fromStrict (digestToHexByteString (hash contents :: Digest MD5)), BSL.fromStrict (digestToHexByteString (hash contents :: Digest SHA1)))

mavenConfigFromEnv :: (MonadIO m, E.MonadThrow m) => m MavenUploadConfig
mavenConfigFromEnv = do
    url <- liftIO $ getEnv "MAVEN_URL"
    user <- liftIO $ getEnv "MAVEN_USERNAME"
    password <- liftIO $ getEnv "MAVEN_PASSWORD"
    mbAllowUnsecureTls <- liftIO $ lookupEnv "MAVEN_UNSECURE_TLS"
    signingKey <- liftIO $ getEnv "GPG_KEY"
    pure MavenUploadConfig
        { mucUrl = T.pack url
        , mucUser = T.pack user
        , mucPassword = T.pack password
        , mucAllowUnsecureTls = MavenAllowUnsecureTls $ mbAllowUnsecureTls == Just "True"
        , mucSigningKey = signingKey
        }

textToLazyByteString :: Text -> BSL.ByteString
textToLazyByteString text = BSL.fromStrict $ encodeUtf8 text

--
-- HTTP Response Handlers
--

httpResponseHandler :: (MonadIO m, MonadLogger m) => RetryStatus -> E.Handler m Bool
httpResponseHandler status = logRetries shouldRetry logRetry status

checkRepoStatusHandler :: (MonadIO m, MonadLogger m) => RetryStatus -> E.Handler m Bool
checkRepoStatusHandler status = logRetries shouldStatusRetry logStatusRetry status

shouldRetry :: (MonadIO m) => HttpException -> m Bool
shouldRetry e = case e of
    HttpExceptionRequest _ ConnectionTimeout -> return True
    -- Don't retry POST requests if the response timeouts as the request might of been processed
    HttpExceptionRequest request ResponseTimeout -> return (method request == "POST")
    HttpExceptionRequest _ (StatusCodeException rsp _) ->
        case statusCode (responseStatus rsp) of
            408 {- requestTimeout -} -> return True
            502 {- badGateway -} -> return True
            503 {- serviceUnavailable -} -> return True
            _ -> return False
    _ -> return False

shouldStatusRetry :: (MonadIO m) => RepoNotClosed -> m Bool
shouldStatusRetry _ = return True

-- | For use with 'logRetries'.
logRetry :: (MonadIO m, MonadLogger m, E.Exception e) => Bool -> e -> RetryStatus -> m ()
logRetry shouldRetry err status = do
    $logWarn (tshow err <> ". " <> " " <> tshow status <> " - " <> nextMsg)
    return ()
  where
    nextMsg = if shouldRetry then "Retrying." else "Aborting after " <> (tshow $ rsCumulativeDelay status) <> "µs total delay."

logStatusRetry :: (MonadIO m, MonadLogger m, E.Exception e) => Bool -> e -> RetryStatus -> m ()
logStatusRetry shouldRetry _ status =
    if shouldRetry
    then
        $logDebug ("Staging repository is still processing close request. Checked after " <> tshow (rsCumulativeDelay status) <> "µs")
    else
        $logDebug ("Aborting staging repository check after " <> (tshow $ rsCumulativeDelay status) <> "µs.")

uploadRetryPolicy :: RetryPolicy
uploadRetryPolicy = limitRetriesByCumulativeDelay (60 * 1000 * 1000) (exponentialBackoff (200 * 100))

-- The status of the staging repository can take a number of minutes to change it's
-- status to closed.
checkStatusRetryPolicy :: RetryPolicy
checkStatusRetryPolicy = limitRetriesByCumulativeDelay (10 * 60 * 1000 * 1000) (constantDelay (15 * 1000 * 1000))

handleStatusRequest :: (MonadIO m) => Request -> Manager -> m Bool
handleStatusRequest request manager = do
    statusResponse <- liftIO $ httpLbs request manager
    repoStatus <- liftIO $ decodeRepoStatus $ responseBody statusResponse
    if transitioning repoStatus
    then
        throwIO RepoNotClosed
    else
        return $ status repoStatus == "open"

--
-- Data Transfer Objects for the Nexus Staging REST API.
-- Note that fields from the REST response that are not used do not need
-- to be defined as Aeson will simply ignore them.
-- See https://oss.sonatype.org/nexus-staging-plugin/default/docs/index.html
--
data RepoStatusResponse = RepoStatusResponse
    { repositoryId :: Text
    , status :: Text
    , transitioning :: Bool
    } deriving Show

data StagingPromote = StagingPromote { stagedRepositoryId :: Text }
data StagingPromoteResponse = StagingPromoteResponse { _data :: StagingPromote }

data NameValue = NameValue
    { name :: Text
    , value :: Text
    }
instance Show NameValue where
   show NameValue{..} = T.unpack $ "     " <> name <> ":  " <> value

data RepoActivityEvent = RepoActivityEvent
    { name :: Text
    , properties :: [NameValue]
    }
instance Show RepoActivityEvent where
    show RepoActivityEvent{..} = do
            T.unpack $ name <> intercalatedValues
        where
            intercalatedValues = T.intercalate "\n    " ([""] <> map tshow properties <> [""])

data RepoActivityDetails = RepoActivityDetails
    { name :: Text
    , events :: [RepoActivityEvent]
    }
instance Show RepoActivityDetails where
    show RepoActivityDetails{..} = do
            T.unpack $ name <> intercalatedValues
        where
            intercalatedValues = T.intercalate "\n  " ([""] <> map tshow events <> [""])

-- 'Manual' parsing of required fields as the API uses the Haskell reserved keyword 'type'
instance FromJSON RepoStatusResponse where
    parseJSON (Object o) = RepoStatusResponse <$> o .: "repositoryId" <*> o .: "type" <*> o .: "transitioning"
    parseJSON _ = fail "Expected an Object"

instance FromJSON StagingPromote where
   parseJSON = withObject "StagingPromote" $ \o -> StagingPromote
        <$> o .: "stagedRepositoryId"

-- 'Manual' parsing of required fields as the API uses the Haskell reserved keyword 'data'
instance FromJSON StagingPromoteResponse where
    parseJSON (Object o) = StagingPromoteResponse <$> o .: "data"
    parseJSON _ = fail "Expected an Object"

instance FromJSON RepoActivityDetails where
   parseJSON = withObject "RepoActivityDetails" $ \o -> RepoActivityDetails
        <$> o .: "name"
        <*> o .: "events"

instance FromJSON RepoActivityEvent where
   parseJSON = withObject "RepoActivityEvent" $ \o -> RepoActivityEvent
        <$> o .: "name"
        <*> o .: "properties"

instance FromJSON NameValue where
   parseJSON = withObject "NameValue" $ \o -> NameValue
        <$> o .: "name"
        <*> o .: "value"

decodeRepoStatus :: (MonadIO m) => BSL.ByteString -> m RepoStatusResponse
decodeRepoStatus jsonString = case (eitherDecode jsonString :: Either String RepoStatusResponse) of
    Left err -> throwIO $ ParseJsonException err
    Right r -> return r

decodeStagingPromoteResponse :: (MonadIO m) => Response BSL.ByteString -> m StagingPromoteResponse
decodeStagingPromoteResponse response = case (eitherDecode $ responseBody response :: Either String StagingPromoteResponse) of
    Left err -> throwIO $ ParseJsonException err
    Right r -> return r

decodeRepoActivityResponse :: (MonadIO m) => Response BSL.ByteString -> m [RepoActivityDetails]
decodeRepoActivityResponse response = case (eitherDecode $ responseBody response :: Either String [RepoActivityDetails]) of
    Left err -> throwIO $ ParseJsonException err
    Right r -> return r

--
-- Error definitions
--
data UploadFailure
    = ParseJsonException String
    | CannotDecodeSigningKey String
    | RepoFailedToClose [Text]

instance E.Exception UploadFailure
instance Show UploadFailure where
   show (ParseJsonException msg) = "Cannot parse JSON data: " <> msg
   show (CannotDecodeSigningKey msg) = "Cannot Base64 decode signing key: " <> msg
   show (RepoFailedToClose repoIds) = "The staging repositories " <> show repoIds <> " failed to close"

data RepoNotClosed
    = RepoNotClosed
    deriving Show

instance E.Exception RepoNotClosed
