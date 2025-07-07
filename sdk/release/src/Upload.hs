-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TemplateHaskell #-}

module Upload (
    uploadToMavenCentral,
    mavenConfigFromEnv,
) where

import qualified "zip-archive" Codec.Archive.Zip as ZipArchive
import qualified Control.Exception.Safe as E
import           Control.Monad.Logger
import           Control.Monad.IO.Class
import           "cryptohash" Crypto.Hash (Digest, MD5(..), SHA1(..), digestToHexByteString, hash)
import           Control.Retry
import           Data.Foldable
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Base64 as Base64
import qualified Data.ByteString.Char8 as C8
import qualified Data.Text as T
import           Data.Text.Encoding (encodeUtf8)
import           Data.Text.Lazy.Encoding (decodeUtf8)
import           Data.Time.Clock.POSIX (getPOSIXTime)
import           Network.Connection (TLSSettings(..))
import           Network.HTTP.Client
import           Network.HTTP.Client.TLS (mkManagerSettings, tlsManagerSettings)
import           Network.HTTP.Client.MultipartFormData
import           Network.HTTP.Simple (setRequestBasicAuth, setRequestHeader, setRequestMethod, setRequestPath, setRequestQueryString)
import           Network.HTTP.Types.Status
import           Path
import           System.Environment
import           System.IO.Temp

import Types
import Util

--
-- Upload the artifacts to Maven Central
--
-- The artifacts are first zipped to a bundle file.
-- The bundle is uploaded to the Publisher Portal API where its content is verified to conform to the Maven Central
-- standards before being published to the public repository.
--
-- Digitalasset has been assigned the 'com.daml' namespace.
--
-- Further information:
--
--  Maven Central requirements: https://central.sonatype.org/publish/requirements/
--  Publisher API: https://central.sonatype.org/publish/publish-portal-api/
--
uploadToMavenCentral :: (MonadCI m) => MavenUploadConfig -> Path Abs Dir -> [(MavenCoords, Path Rel File)] -> m ()
uploadToMavenCentral MavenUploadConfig{..} releaseDir artifacts = do
     -- Note: TLS verification settings switchable by MavenUpload settings
    let managerSettings = if getAllowUnsecureTls mucAllowUnsecureTls then noVerifyTlsManagerSettings else tlsManagerSettings

    -- Create HTTP Connection manager with 10 min response timeout as the Portal API can be slow...
    manager <- liftIO $ newManager managerSettings { managerResponseTimeout = responseTimeoutMicro (10 * 60 * 1000 * 1000) }

    parsedUrlRequest <- parseUrlThrow $ T.unpack mucUrl -- Note: Will throw exception on non-2XX responses
    let baseRequest = setRequestBasicAuth (encodeUtf8 mucUser) (encodeUtf8 mucPassword) 
            $ setRequestHeader "User-Agent" ["http-conduit"] parsedUrlRequest

    decodedSigningKey <- decodeSigningKey mucSigningKey
    -- Security Note: Using the withSystemTempDirectory function to always cleanup the private key data from the filesystems.
    bundle <- withSystemTempDirectory "gnupg" $ \gpgTempDir -> do
        -- Write the secret key used for signing into a temporary file and use 'gpg' command line tool to import into
        -- GPG's internal file tree.
        secretKeyFile <- liftIO $ emptyTempFile gpgTempDir "gpg-private-key.asc"
        _ <- liftIO $ BS.writeFile secretKeyFile decodedSigningKey

        loggedProcess_ "gpg" [ "--homedir", T.pack gpgTempDir, "--no-tty", "--quiet", "--import", T.pack secretKeyFile ]

        currentTime <- liftIO $ round <$> getPOSIXTime
        foldlM (addArtifactToBundle gpgTempDir releaseDir currentTime) ZipArchive.emptyArchive artifacts

    liftIO $ BSL.writeFile (fromAbsDir releaseDir <> "bundle.zip") $ ZipArchive.fromArchive bundle
    uploadRequest <- formDataBody [partFile "bundle" $ fromAbsDir releaseDir <> "bundle.zip"]
            $ setRequestMethod "POST"
            $ setRequestPath "/api/v1/publisher/upload"
            $ setRequestQueryString [("publishingType", Just "AUTOMATIC")] baseRequest

    uploadResponse <- recovering uploadRetryPolicy [ httpResponseHandler ] (\_ -> liftIO $ httpLbs uploadRequest manager)

    let deploymentId = BSL.toStrict $ responseBody uploadResponse 
    
    let statusRequest = setRequestMethod "POST"
            $ setRequestPath "/api/v1/publisher/status"
            $ setRequestHeader "accept" [ "application/json" ]
            $ setRequestQueryString [("id", Just deploymentId)] baseRequest

    --
    -- Poll until the bundle status is PUBLISHED
    -- Throws if the bundle becomes FAILED
    --
    _ <- recovering checkStatusRetryPolicy [ httpResponseHandler, checkRepoStatusHandler ] (\_ -> handleStatusRequest statusRequest manager)

    return ()

decodeSigningKey :: (MonadCI m) => String -> m BS.ByteString
decodeSigningKey signingKey =  case Base64.decode $ C8.pack signingKey of
    Left err -> throwIO $ CannotDecodeSigningKey err
    Right decodedData -> return decodedData

addArtifactToBundle :: (MonadCI m) => FilePath -> Path Abs Dir -> Integer -> ZipArchive.Archive -> (MavenCoords, Path Rel File) -> m ZipArchive.Archive
addArtifactToBundle gpgTempDir releaseDir time bundle (_, file) = do
    let absFile = fromAbsFile $ releaseDir </> file
        sigFile = absFile <> ".asc"

    -- The "--batch" and "--yes" flags are used to prevent gpg waiting on stdin.
    loggedProcess_ "gpg" [ "--homedir", T.pack gpgTempDir, "-ab", "-o", T.pack sigFile, "--batch", "--yes", T.pack absFile ]

    content <- liftIO $ BS.readFile absFile
    sig <- liftIO $ BSL.readFile sigFile

    let md5 = digestToHexByteString (hash content :: Digest MD5)
        sha1 = digestToHexByteString (hash content :: Digest SHA1)

    let mainEntry = ZipArchive.toEntry (fromRelFile file) time (BSL.fromStrict content)
        sigEntry = ZipArchive.toEntry (fromRelFile file <> ".asc") time sig
        md5Entry = ZipArchive.toEntry (fromRelFile file <> ".md5") time (BSL.fromStrict md5)
        sha1Entry = ZipArchive.toEntry (fromRelFile file <> ".sha1") time (BSL.fromStrict sha1)

    return $ ZipArchive.addEntryToArchive mainEntry
           $ ZipArchive.addEntryToArchive sigEntry
           $ ZipArchive.addEntryToArchive md5Entry
           $ ZipArchive.addEntryToArchive sha1Entry bundle

noVerifyTlsManagerSettings :: ManagerSettings
noVerifyTlsManagerSettings = mkManagerSettings
    TLSSettingsSimple
    { settingDisableCertificateValidation = True
    , settingDisableSession = True
    , settingUseServerName = False
    }
    Nothing

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

shouldStatusRetry :: (MonadIO m) => DeploymentInProgress -> m Bool
shouldStatusRetry _ = return True

-- | For use with 'logRetries'.
logRetry :: (MonadIO m, MonadLogger m, E.Exception e) => Bool -> e -> RetryStatus -> m ()
logRetry shouldRetry err status = do
    $logWarn (tshow err <> ". " <> " " <> tshow status <> " - " <> nextMsg)
    return ()
  where
    nextMsg = if shouldRetry then "Retrying." else "Aborting after " <> (tshow $ rsCumulativeDelay status) <> "µs total delay."

logStatusRetry :: (MonadIO m, MonadLogger m) => Bool -> DeploymentInProgress -> RetryStatus -> m ()
logStatusRetry shouldRetry _ status =
    if shouldRetry
    then
        $logDebug ("Deployment is still in progress. Checked after " <> tshow (rsCumulativeDelay status) <> "µs")
    else
        $logDebug ("Aborting deployment check after " <> (tshow $ rsCumulativeDelay status) <> "µs.")

-- Retry for 5 minutes total, doubling delay starting with 20ms.
uploadRetryPolicy :: RetryPolicy
uploadRetryPolicy = limitRetriesByCumulativeDelay (5 * 60 * 1000 * 1000) (exponentialBackoff (20 * 1000))

-- The deployment usually takes a few minutes to succeed with status PUBLISHED.
-- However occasionally sonatype gets really slow so we use an absurdly long retry of 2h.
checkStatusRetryPolicy :: RetryPolicy
checkStatusRetryPolicy = limitRetriesByCumulativeDelay (2 * 60 * 60 * 1000 * 1000) (constantDelay (15 * 1000 * 1000))

handleStatusRequest :: (MonadIO m) => Request -> Manager -> m ()
handleStatusRequest request manager = do
    statusResponse <- liftIO $ httpLbs request manager
    case decodeUtf8 $ responseBody statusResponse of
        "FAILED" -> throwIO DeploymentFailed
        "PUBLISHED" -> pure ()
        _ -> throwIO DeploymentInProgress

--
-- Error definitions
--
data CannotDecodeSigningKey = CannotDecodeSigningKey String

instance E.Exception CannotDecodeSigningKey
instance Show CannotDecodeSigningKey where
   show (CannotDecodeSigningKey msg) = "Cannot Base64 decode signing key: " <> msg

data DeploymentInProgress = DeploymentInProgress deriving Show

instance E.Exception DeploymentInProgress

data DeploymentFailed = DeploymentFailed deriving Show

instance E.Exception DeploymentFailed
