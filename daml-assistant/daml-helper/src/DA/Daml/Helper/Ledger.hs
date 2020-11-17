-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE RankNTypes #-}

module DA.Daml.Helper.Ledger (
    LedgerFlags(..),
    defaultLedgerFlags,
    LedgerApi(..),
    L.ClientSSLConfig(..),
    L.ClientSSLKeyCertPair(..),
    L.TimeoutSeconds,
    JsonFlag(..),
    runDeploy,
    runLedgerListParties,
    runLedgerAllocateParties,
    runLedgerUploadDar,
    runLedgerFetchDar,
    runLedgerNavigator,
    ) where

import Control.Exception (SomeException(..), catch)
import Control.Lens (toListOf)
import Control.Monad.Extra hiding (fromMaybeM)
import Data.Aeson ((.=))
import qualified Data.Aeson as A
import Data.Aeson.Text
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy as BSL
import Data.List.Extra
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe
import Data.String (IsString, fromString)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.IO as TL
import GHC.Generics
import Network.GRPC.Unsafe.ChannelArgs (Arg(..))
import Network.HTTP.Simple
import Network.HTTP.Types (statusCode)
import Numeric.Natural
import System.Environment
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Process.Typed

import DA.Daml.Compiler.Dar (createArchive, createDarFile)
import DA.Daml.Helper.Util
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Ast.Optics as LF (packageRefs)
import qualified DA.Daml.LF.Proto3.Archive as LFArchive
import DA.Daml.Package.Config (PackageSdkVersion(..))
import DA.Daml.Project.Util (fromMaybeM)
import qualified DA.Ledger as L
import DA.Ledger (Party(..), PartyDetails(..))
import qualified SdkVersion

data LedgerApi
  = Grpc
  | HttpJson
  deriving (Show, Eq)

data LedgerFlags = LedgerFlags
  { fApi :: LedgerApi
  , fSslConfigM :: Maybe L.ClientSSLConfig
  , fTimeout :: L.TimeoutSeconds
  -----------------------------------------
  -- The following values get defaults from the project config by
  -- running `getDefaultArgs`
  , fHostM :: Maybe String
  , fPortM :: Maybe Int
  , fTokFileM :: Maybe FilePath
  , fMaxReceiveLengthM :: Maybe Natural
  }

defaultLedgerFlags :: LedgerApi -> LedgerFlags
defaultLedgerFlags api = LedgerFlags
  { fApi = api
  , fSslConfigM = Nothing
  , fTimeout = 10
  , fHostM = Nothing
  , fPortM = Nothing
  , fTokFileM = Nothing
  , fMaxReceiveLengthM = Nothing
  }

data LedgerArgs = LedgerArgs
  { api :: LedgerApi
  , sslConfigM :: Maybe L.ClientSSLConfig
  , timeout :: L.TimeoutSeconds
  -----------------------------------------
  , host :: String
  , port :: Int
  , tokM :: Maybe L.Token
  , grpcArgs :: [Arg]
  }

showHostAndPort :: LedgerArgs -> String
showHostAndPort LedgerArgs{host,port} = host <> ":" <> show port

--
-- Get default values from project config
-----------------------------------------
getDefaultArgs :: LedgerFlags -> IO LedgerArgs
getDefaultArgs LedgerFlags { fApi
                           , fSslConfigM
                           , fTimeout
                           , fHostM
                           , fPortM
                           , fTokFileM
                           , fMaxReceiveLengthM
                           } = do
  host <- fromMaybeM getProjectLedgerHost fHostM
  port <- fromMaybeM getProjectLedgerPort fPortM
  tokM <- getTokFromFile fTokFileM
  return $
    LedgerArgs
      { api = fApi
      , port = port
      , host = host
      , tokM = tokM
      , timeout = fTimeout
      , sslConfigM = fSslConfigM
      , grpcArgs = MaxReceiveMessageLength <$> maybeToList fMaxReceiveLengthM
      }

getTokFromFile :: Maybe FilePath -> IO (Maybe L.Token)
getTokFromFile tokFileM = do
  case tokFileM of
    Nothing -> return Nothing
    Just tokFile -> do
      -- This postprocessing step which allows trailing newlines
      -- matches the behavior of the Scala token reader in
      -- com.daml.auth.TokenHolder.
      tok <- intercalate "\n" . lines <$> readFileUTF8 tokFile
      return (Just (L.Token tok))

--
-- Ledger command implementations
---------------------------------

-- | Allocate project parties and upload project DAR file to ledger.
runDeploy :: LedgerFlags -> IO ()
runDeploy flags = do
    args <- getDefaultArgs flags
    putStrLn $ "Deploying to " <> showHostAndPort args
    runLedgerAllocateParties flags []
    runLedgerUploadDar flags Nothing
    putStrLn "Deploy succeeded."

-- | Allocate parties on ledger. If list of parties is empty,
-- defaults to the project parties.
runLedgerAllocateParties :: LedgerFlags -> [String] -> IO ()
runLedgerAllocateParties flags partiesArg = do
    args <- getDefaultArgs flags
    parties <-
      if notNull partiesArg
        then pure partiesArg
        else getProjectParties
    putStrLn $ "Checking party allocation at " <> showHostAndPort args
    mapM_ (allocatePartyIfRequired args) parties
    where
      allocatePartyIfRequired args name = do
        partyM <- lookupParty args name
        party <-
          flip fromMaybeM partyM $ do
            putStrLn $
              "Allocating party for '" <> name <> "' at " <> showHostAndPort args
            allocateParty args name
        putStrLn $
          "Allocated " <> show party <> " for '" <> name <> "' at " <>
          showHostAndPort args

-- | Upload a DAR file to the ledger. (Defaults to project DAR)
runLedgerUploadDar :: LedgerFlags -> Maybe FilePath -> IO ()
runLedgerUploadDar flags darPathM = do
  args <- getDefaultArgs flags
  darPath <-
    flip fromMaybeM darPathM $ do
      doBuild
      getDarPath
  putStrLn $ "Uploading " <> darPath <> " to " <> showHostAndPort args
  bytes <- BS.readFile darPath
  result <-
    uploadDarFile args bytes `catch` \(e :: SomeException) -> do
      putStrLn $
        unlines
          [ "An exception was thrown during the upload-dar command"
          , "- " <> show e
          , "One reason for this to occur is if the size of DAR file being uploaded exceeds the gRPC maximum message size. The default value for this is 4Mb, but it may be increased when the ledger is (re)started. Please check with your ledger operator."
          ]
      exitFailure
  case result of
    Left err -> do
      putStrLn $ "upload-dar did not succeed: " <> show err
      exitFailure
    Right () -> putStrLn "DAR upload succeeded."

uploadDarFile :: LedgerArgs -> BS.ByteString -> IO (Either String ())
uploadDarFile args bytes =
  case api args of
    Grpc -> runWithLedgerArgs args $ do L.uploadDarFile bytes
    HttpJson -> do
      (i :: Int) <- httpJsonRequest args "POST" "/v1/packages" $ setRequestBodyLBS (BSL.fromStrict bytes)
      return $
        if i == 1
          then Right ()
          else Left "An error occured. Please check the returned status."

newtype JsonFlag = JsonFlag { unJsonFlag :: Bool }

-- | Fetch list of parties from ledger.
runLedgerListParties :: LedgerFlags -> JsonFlag -> IO ()
runLedgerListParties flags (JsonFlag json) = do
    args <- getDefaultArgs flags
    unless json . putStrLn $ "Listing parties at " <> showHostAndPort args
    xs <- listParties args
    if json then do
        TL.putStrLn . encodeToLazyText . A.toJSON $
            [ A.object
                [ "party" .= TL.toStrict (unParty party)
                , "display_name" .= TL.toStrict displayName
                , "is_local" .= isLocal
                ]
            | PartyDetails {..} <- xs
            ]
    else if null xs then
        putStrLn "no parties are known"
    else
        mapM_ print xs

-- | Fetch the packages reachable from a main package-id, and reconstruct a DAR file.
runLedgerFetchDar :: LedgerFlags -> String -> FilePath -> IO ()
runLedgerFetchDar flags pidString saveAs = do
    args <- getDefaultArgs flags
    let pid = LF.PackageId $ T.pack pidString
    putStrLn $
      "Fetching " <> show (LF.unPackageId pid) <> " from " <> showHostAndPort args <>
      " into " <>
      saveAs
    n <- fetchDar args pid saveAs
    putStrLn $ "DAR fetch succeeded; contains " <> show n <> " packages."

-- | Reconstruct a DAR file by downloading packages from a ledger. Returns how many packages fetched.
fetchDar :: LedgerArgs -> LF.PackageId -> FilePath -> IO Int
fetchDar args rootPid saveAs = do
  pkgs <- downloadAllReachablePackages args rootPid
  let rootPkg = pkgs Map.! rootPid
  let (dalf,pkgId) = LFArchive.encodeArchiveAndHash rootPkg
  let dalfDependencies :: [(T.Text,BS.ByteString,LF.PackageId)] =
        [ (txt,bs,pkgId)
        | (pid,pkg) <- Map.toList (Map.delete rootPid pkgs)
        , let txt = recoverPackageName pkg ("dep",pid)
        , let (bsl,pkgId) = LFArchive.encodeArchiveAndHash pkg
        , let bs = BSL.toStrict bsl
        ]
  let (pName,pVersion) = do
        let LF.Package {packageMetadata} = rootPkg
        case packageMetadata of
          Nothing -> (LF.PackageName $ T.pack "reconstructed",Nothing)
          Just LF.PackageMetadata{packageName,packageVersion} -> (packageName,Just packageVersion)
  let pSdkVersion = PackageSdkVersion SdkVersion.sdkVersion
  let srcRoot = error "unexpected use of srcRoot when there are no sources"
  let za = createArchive pName pVersion pSdkVersion pkgId dalf dalfDependencies srcRoot [] [] []
  createDarFile saveAs za
  return $ Map.size pkgs

recoverPackageName :: LF.Package -> (String,LF.PackageId) -> T.Text
recoverPackageName pkg (tag,pid)= do
  let LF.Package {packageMetadata} = pkg
  case packageMetadata of
    Just LF.PackageMetadata{packageName} -> LF.unPackageName packageName
    -- fallback, manufacture a name from the pid
    Nothing -> T.pack (tag <> "-" <> T.unpack (LF.unPackageId pid))

-- | Download all Packages reachable from a PackageId; fail if any don't exist or can't be decoded.
downloadAllReachablePackages :: LedgerArgs -> LF.PackageId -> IO (Map LF.PackageId LF.Package)
downloadAllReachablePackages args pid = loop Map.empty [pid]
  where
    loop :: Map LF.PackageId LF.Package -> [LF.PackageId] -> IO (Map LF.PackageId LF.Package)
    loop acc = \case
      [] -> return acc
      pid:morePids ->
        if pid `Map.member` acc
        then loop acc morePids
        else do
          pkg <- downloadPackage args pid
          loop (Map.insert pid pkg acc) (packageRefs pkg ++ morePids)

    packageRefs pkg = nubSort [ pid | LF.PRImport pid <- toListOf LF.packageRefs pkg ]

-- | Download the Package identified by a PackageId; fail if it doesn't exist or can't be decoded.
downloadPackage :: LedgerArgs -> LF.PackageId -> IO LF.Package
downloadPackage args pid = do
  bs <-
    case api args of
      Grpc -> do
        runWithLedgerArgs args $ do
          lid <- L.getLedgerIdentity
          mbPkg <- L.getPackage lid $ convPid pid
          case mbPkg of
            Nothing -> fail $ "Unable to download package with identity: " <> show pid
            Just (L.Package bs) -> pure bs
      HttpJson ->
        httpBsRequest
          args
          "GET"
          (Path $ unPath "/v1/packages/" <> (T.encodeUtf8 $ LF.unPackageId pid))
          id
  let mode = LFArchive.DecodeAsMain
  case LFArchive.decodePackage mode pid bs of
    Left err -> fail $ show err
    Right pkg -> return pkg
  where
    convPid :: LF.PackageId -> L.PackageId
    convPid (LF.PackageId text) = L.PackageId $ TL.fromStrict text

listParties :: LedgerArgs -> IO [PartyDetails]
listParties args =
  case api args of
    Grpc -> runWithLedgerArgs args L.listKnownParties
    HttpJson -> httpJsonRequest args "GET" "/v1/parties" id

lookupParty :: LedgerArgs -> String -> IO (Maybe Party)
lookupParty args name = do
    xs <- listParties args
    let text = TL.pack name
    let pred PartyDetails{displayName,party} = if text == displayName then Just party else Nothing
    return $ firstJust pred xs

allocateParty :: LedgerArgs -> String -> IO Party
allocateParty args name = do
  let text = TL.pack name
  PartyDetails {party} <-
    case api args of
      Grpc ->
        runWithLedgerArgs args $
        L.allocateParty $ L.AllocatePartyRequest {partyIdHint = text, displayName = text}
      HttpJson ->
        httpJsonRequest args "POST" "/v1/parties/allocate" $
        setRequestBodyJSON $ AllocatePartyRequest {identifierHint = text, displayName = text}
  return party

-- | Run navigator against configured ledger. We supply Navigator with
-- the list of parties from the ledger, but in the future Navigator
-- should fetch the list of parties itself.
runLedgerNavigator :: LedgerFlags -> [String] -> IO ()
runLedgerNavigator flags remainingArguments = do
    args <- getDefaultArgs flags
    logbackArg <- getLogbackArg (damlSdkJarFolder </> "navigator-logback.xml")
    putStrLn $ "Opening navigator at " <> showHostAndPort args
    partyDetails <- listParties args

    withTempDir $ \confDir -> do
        let navigatorConfPath = confDir </> "ui-backend.conf"
            navigatorArgs = concat
                [ ["server"]
                , [host args, show (port args)]
                , remainingArguments
                ]

        writeFileUTF8 navigatorConfPath (T.unpack $ navigatorConfig partyDetails)
        unsetEnv "DAML_PROJECT" -- necessary to prevent config contamination
        withJar damlSdkJar [logbackArg] ("navigator" : navigatorArgs ++ ["-c", confDir </> "ui-backend.conf"]) $ \ph -> do
            exitCode <- waitExitCode ph
            exitWith exitCode

  where
    navigatorConfig :: [PartyDetails] -> T.Text
    navigatorConfig partyDetails = T.unlines . concat $
        [ ["users", "  {"]
        , [ T.concat
            [ "    "
            , T.pack . show $
                if TL.null displayName
                    then unParty party
                    else displayName
            , " { party = "
            , T.pack (show (unParty party))
            , " }"
            ]
          | PartyDetails{..} <- partyDetails
          ]
        , ["  }"]
        ]

--
-- Interface with the Haskell bindings
--------------------------------------

runWithLedgerArgs :: LedgerArgs -> L.LedgerService a -> IO a
runWithLedgerArgs LedgerArgs{host,port,tokM,timeout, sslConfigM, grpcArgs} ls = do
    let ls' = case tokM of Nothing -> ls; Just tok -> L.setToken tok ls
    let ledgerClientConfig =
            L.configOfHostAndPort
                (L.Host $ fromString host)
                (L.Port port)
                grpcArgs
                sslConfigM
    L.runLedgerService ls' timeout ledgerClientConfig

--
-- Interface with the HTTP JSON API
-----------------------------------

newtype Method = Method
  { unMethod :: BS.ByteString
  } deriving IsString

newtype Path = Path
  { unPath :: BS.ByteString
  } deriving IsString

data HttpJsonResponseBody a = HttpJsonResponseBody
  { status :: Int
  , result :: a
  } deriving (Generic)
instance A.FromJSON a => A.FromJSON (HttpJsonResponseBody a)

data AllocatePartyRequest = AllocatePartyRequest
  { identifierHint :: TL.Text
  , displayName :: TL.Text
  } deriving (Generic)
instance A.ToJSON AllocatePartyRequest

-- | Run a request against the HTTP JSON API.
httpJsonRequest :: A.FromJSON a => LedgerArgs -> Method -> Path -> (Request -> Request) -> IO a
httpJsonRequest args method path modify = do
  req <- makeRequest args method path modify
  resp <- runHttp httpJSON req
  pure $ result resp

httpBsRequest :: LedgerArgs -> Method -> Path -> (Request -> Request) -> IO BS.ByteString
httpBsRequest args method path modify = do
  req <- makeRequest args method path modify
  runHttp httpBS req

makeRequest :: LedgerArgs -> Method -> Path -> (Request -> Request) -> IO Request
makeRequest LedgerArgs {sslConfigM, tokM, port, host} method path modify = do
  when (isJust sslConfigM) $
    fail "The HTTP JSON API doesn't support TLS requests, but a TLS flag was set."
  pure $
    setRequestPort port $
    setRequestHost (BSC.pack host) $
    setRequestMethod (unMethod method) $
    setRequestPath (unPath path) $
    modify $
    setRequestHeader
      "authorization"
      [BSC.pack $ sanitizeToken tok | Just (L.Token tok) <- [tokM]]
      defaultRequest

runHttp :: (Request -> IO (Response a)) -> Request -> IO a
runHttp run req = do
  resp <- run req
  let status = getResponseStatus resp
  unless (statusCode status == 200) $ fail $ "Request failed with error code " <> show status
  pure $ getResponseBody resp

-- This matches how the com.daml.ledger.api.auth.client.LedgerCallCredentials
-- behaves.
sanitizeToken :: String -> String
sanitizeToken tok
  | "Bearer " `isPrefixOf` tok = tok
  | otherwise = "Bearer " <> tok
