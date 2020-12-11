-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import Data.Function ((&))
import Data.Semigroup ((<>))
import System.FilePath.Posix ((</>))

import qualified Control.Concurrent.Async
import qualified Control.Concurrent.QSem
import Control.Exception.Safe
import qualified Control.Monad as Control
import qualified Control.Monad.Extra
import qualified Control.Monad.Loops
import qualified Data.Aeson as JSON
import qualified Data.ByteString
import qualified Data.ByteString.UTF8 as BS
import qualified Data.ByteString.Lazy.UTF8 as LBS
import qualified Data.CaseInsensitive as CI
import qualified Data.Foldable
import qualified Data.HashMap.Strict as H
import qualified Data.List
import qualified Data.List.Split as Split
import Data.Maybe (fromMaybe)
import qualified Data.Ord
import qualified Data.SemVer
import qualified Data.Set as Set
import qualified Data.Text as Text
import qualified Network.HTTP.Client as HTTP
import qualified Network.HTTP.Client.TLS as TLS
import qualified Network.HTTP.Types.Status as Status
import qualified Network.URI
import qualified Options.Applicative as Opt
import Safe (headMay)
import qualified System.Directory as Directory
import qualified System.Exit as Exit
import qualified System.IO.Extra as IO
import qualified System.Process as System
import qualified Text.Regex.TDFA as Regex

shell_exit_code :: String -> IO (Exit.ExitCode, String, String)
shell_exit_code cmd = do
    System.readCreateProcessWithExitCode (System.shell cmd) ""

die :: String -> Int -> String -> String -> IO a
die cmd exit out err =
    fail $ unlines ["Subprocess:",
                         cmd,
                        "failed with exit code " <> show exit <> "; output:",
                        "---",
                        out,
                        "---",
                        "err:",
                        "---",
                        err,
                        "---"]

shell :: String -> IO String
shell cmd = System.readCreateProcess (System.shell cmd) ""

shell_ :: String -> IO ()
shell_ cmd = do
    Control.void $ shell cmd

robustly_download_nix_packages :: IO ()
robustly_download_nix_packages = do
    h (10 :: Integer)
    where
        cmd = "nix-build nix -A tools -A ci-cached"
        h n = do
            (exit, out, err) <- shell_exit_code cmd
            case (exit, n) of
              (Exit.ExitSuccess, _) -> return ()
              (Exit.ExitFailure exit, 0) -> die cmd exit out err
              _ | "unexpected end-of-file" `Data.List.isInfixOf` err -> h (n - 1)
              (Exit.ExitFailure exit, _) -> die cmd exit out err

add_github_contact_header :: HTTP.Request -> HTTP.Request
add_github_contact_header req =
    req { HTTP.requestHeaders = ("User-Agent", "DAML cron (team-daml-app-runtime@digitalasset.com)") : HTTP.requestHeaders req }

http_get :: JSON.FromJSON a => String -> IO (a, H.HashMap String String)
http_get url = do
    manager <- HTTP.newManager TLS.tlsManagerSettings
    request <- add_github_contact_header <$> HTTP.parseRequest url
    response <- HTTP.httpLbs request manager
    let body = JSON.decode $ HTTP.responseBody response
    let status = Status.statusCode $ HTTP.responseStatus response
    case (status, body) of
      (200, Just body) -> return (body, response & HTTP.responseHeaders & map (\(n, v) -> (n & CI.foldedCase & BS.toString, BS.toString v)) & H.fromList)
      _ -> Exit.die $ unlines ["GET \"" <> url <> "\" returned status code " <> show status <> ".",
                               show $ HTTP.responseBody response]

s3Path :: DocOptions -> FilePath -> String
s3Path DocOptions{s3Subdir} file =
    "s3://docs-daml-com" </> fromMaybe "" s3Subdir </> file

build_and_push :: DocOptions -> FilePath -> [Version] -> IO ()
build_and_push opts@DocOptions{build} temp versions = do
    restore_sha $ do
        Data.Foldable.for_ versions (\version -> do
            putStrLn $ "Building " <> show version <> "..."
            build temp version
            putStrLn $ "Pushing " <> show version <> " to S3 (as subfolder)..."
            push version
            putStrLn "Done.")
    where
        restore_sha io =
            bracket (init <$> shell "git symbolic-ref --short HEAD 2>/dev/null || git rev-parse HEAD")
                                      (\cur_sha -> shell_ $ "git checkout " <> cur_sha)
                                      (const io)
        push version =
            shell_ $ "aws s3 cp " <>
              (temp </> show version) <> " " <>
              s3Path opts (show version) <>
              " --recursive --acl public-read"

fetch_if_missing :: DocOptions -> FilePath -> Version -> IO ()
fetch_if_missing opts temp v = do
    missing <- not <$> Directory.doesDirectoryExist (temp </> show v)
    if missing then do
        putStrLn $ "Downloading " <> show v <> "..."
        shell_ $ "aws s3 cp " <> s3Path opts (show v) <> " " <> temp </> show v <> " --recursive"
        putStrLn "Done."
    else do
        putStrLn $ show v <> " already present."

update_s3 :: DocOptions -> FilePath -> Versions -> IO ()
update_s3 opts temp vs = do
    putStrLn "Updating versions.json & hidden.json..."
    create_versions_json (dropdown vs) (temp </> "versions.json")
    let hidden = Data.List.sortOn Data.Ord.Down $ Set.toList $ all_versions vs `Set.difference` (Set.fromList $ dropdown vs)
    create_versions_json hidden (temp </> "hidden.json")
    shell_ $ "aws s3 cp " <> temp </> "versions.json " <> s3Path opts "versions.json" <> " --acl public-read"
    shell_ $ "aws s3 cp " <> temp </> "hidden.json " <> s3Path opts "hidden.json" <> " --acl public-read"
    -- FIXME: remove after running once (and updating the reading bit in this file)
    shell_ $ "aws s3 cp " <> temp </> "hidden.json " <> s3Path opts "snapshots.json" <> " --acl public-read"
    putStrLn "Done."
    where
        create_versions_json versions file = do
            -- Not going through Aeson because it represents JSON objects as
            -- unordered maps, and here order matters.
            let versions_json = versions
                                & map show
                                & map (\s -> "\"" <> s <> "\": \"" <> s <> "\"")
                                & Data.List.intercalate ", "
                                & \s -> "{" <> s <> "}"
            writeFile file versions_json

update_top_level :: DocOptions -> FilePath -> Version -> Maybe Version -> IO ()
update_top_level opts temp new mayOld = do
    new_files <- Set.fromList <$> Directory.listDirectory (temp </> show new)
    old_files <- case mayOld of
        Nothing -> pure Set.empty
        Just old -> Set.fromList <$> Directory.listDirectory (temp </> show old)
    let to_delete = Set.toList $ old_files `Set.difference` new_files
    Control.when (not $ null to_delete) $ do
        putStrLn $ "Deleting top-level files: " <> show to_delete
        Data.Foldable.for_ to_delete (\f -> do
            shell_ $ "aws s3 rm " <> s3Path opts f <> " --recursive")
        putStrLn "Done."
    putStrLn $ "Pushing " <> show new <> " to top-level..."
    let path = s3Path opts "" <> "/"
    shell_ $ "aws s3 cp " <> temp </> show new </> " " <> path <> " --recursive --acl public-read"
    putStrLn "Done."

reset_cloudfront :: IO ()
reset_cloudfront = do
    putStrLn "Refreshing CloudFront cache..."
    shell_ $ "aws cloudfront create-invalidation"
             <> " --distribution-id E1U753I56ERH55"
             <> " --paths '/*'"

fetch_gh_paginated :: String -> IO [GitHubRelease]
fetch_gh_paginated url = do
    (resp_0, headers) <- http_get url
    case parse_next =<< H.lookup "link" headers of
      Nothing -> return resp_0
      Just next -> do
          rest <- fetch_gh_paginated next
          return $ resp_0 ++ rest
    where parse_next header = lookup "next" $ map parse_link $ split header
          split h = Split.splitOn ", " h
          link_regex = "<(.*)>; rel=\"(.*)\"" :: String
          parse_link l =
              let typed_regex :: (String, String, String, [String])
                  typed_regex = l Regex.=~ link_regex
              in
              case typed_regex of
                (_, _, _, [url, rel]) -> (rel, url)
                _ -> fail $ "Assumption violated: link header entry did not match regex.\nEntry: " <> l

data Asset = Asset { uri :: Network.URI.URI }
instance JSON.FromJSON Asset where
    parseJSON = JSON.withObject "Asset" $ \v -> Asset
        <$> (do
            Just url <- Network.URI.parseURI <$> v JSON..: "browser_download_url"
            return url)

data GitHubRelease = GitHubRelease { prerelease :: Bool, tag :: Version, assets :: [Asset] }
instance JSON.FromJSON GitHubRelease where
    parseJSON = JSON.withObject "GitHubRelease" $ \v -> GitHubRelease
        <$> (v JSON..: "prerelease")
        <*> (version . Text.tail <$> v JSON..: "tag_name")
        <*> (v JSON..: "assets")

data Version = Version Data.SemVer.Version
    deriving (Ord, Eq)
instance Show Version where
    show (Version v) = Data.SemVer.toString v

version :: Text.Text -> Version
version t = Version $ (\case Left s -> (error s); Right v -> v) $ Data.SemVer.fromText t

data Versions = Versions { top :: Maybe Version, all_versions :: Set.Set Version, dropdown :: [Version] }
    deriving Eq

versions :: [GitHubRelease] -> Versions
versions vs =
    let all_versions = Set.fromList $ map tag vs
        dropdown = vs
                   & filter (not . prerelease)
                   & map tag
                   & filter (>= version "1.0.0")
                   & Data.List.sortOn Data.Ord.Down
        top = headMay dropdown
    in Versions {..}

fetch_gh_versions :: (Version -> Bool) -> IO Versions
fetch_gh_versions pred = do
    response <- fetch_gh_paginated "https://api.github.com/repos/digital-asset/daml/releases"
    return $ versions $ filter (\v -> pred (tag v)) response

fetch_s3_versions :: DocOptions -> IO Versions
fetch_s3_versions opts = do
    -- On the first run, this will fail so treat it like an empty file.
    -- We could technically remove the catch later.
    dropdown <- fetch "versions.json" False `catchIO`
      (\_ -> pure [])
    -- TODO: read hidden.json after this has run once
    hidden <- fetch "snapshots.json" True `catchIO`
      (\_ -> pure [])
    return $ versions $ dropdown <> hidden
    where fetch file prerelease = do
              temp <- shell "mktemp"
              shell_ $ "aws s3 cp " <> s3Path opts file <> " " <> temp
              s3_raw <- shell $ "cat " <> temp
              let type_annotated_value :: Maybe JSON.Object
                  type_annotated_value = JSON.decode $ LBS.fromString s3_raw
              case type_annotated_value of
                  Just s3_json -> return $ map (\s -> GitHubRelease prerelease (version s) []) $ H.keys s3_json
                  Nothing -> Exit.die "Failed to get versions from s3"

data DocOptions = DocOptions
  { s3Subdir :: Maybe FilePath
  , includedVersion :: Version -> Bool
    -- Exclusive minimum version bound for which we build docs
  , build :: FilePath -> Version -> IO ()
  }

sdkDocOpts :: DocOptions
sdkDocOpts = DocOptions
  { s3Subdir = Nothing
    -- versions prior to 0.13.10 cannot be built anymore and are not present in
    -- the repo.
  , includedVersion = \v -> v >= version "0.13.10"
  , build = \temp version -> do
        shell_ $ "git checkout v" <> show version
        robustly_download_nix_packages
        shell_ $ "DAML_SDK_RELEASE_VERSION=" <> show version <> " bazel build //docs:docs"
        shell_ $ "mkdir -p  " <> temp </> show version
        shell_ $ "tar xzf bazel-bin/docs/html.tar.gz --strip-components=1 -C" <> temp </> show version
  }

damlOnSqlDocOpts :: DocOptions
damlOnSqlDocOpts = DocOptions
  { s3Subdir = Just "daml-driver-for-postgresql"
  , includedVersion = \v -> v > version "1.8.0-snapshot.20201201.5776.0.4b91f2a6"
  , build = \temp version -> do
        shell_ $ "git checkout v" <> show version
        robustly_download_nix_packages
        shell_ $ "DAML_SDK_RELEASE_VERSION=" <> show version <> " bazel build //ledger/daml-on-sql:docs"
        shell_ $ "mkdir -p  " <> temp </> show version
        shell_ $ "tar xzf bazel-bin/ledger/daml-on-sql/html.tar.gz --strip-components=1 -C" <> temp </> show version
  }

docs :: DocOptions -> IO ()
docs opts@DocOptions{includedVersion} = do
    putStrLn "Checking for new version..."
    gh_versions <- fetch_gh_versions includedVersion
    s3_versions <- fetch_s3_versions opts
    if s3_versions == gh_versions
    then do
        putStrLn "Versions match, nothing to do."
    else do
        -- We may have added versions. We need to build and push them.
        let added = Set.toList $ all_versions gh_versions `Set.difference` all_versions s3_versions
        IO.withTempDir $ \temp_dir -> do
            putStrLn $ "Versions to build: " <> show added
            build_and_push opts temp_dir added
            -- If there is no version on GH, we don’t have to do anything.
            Control.Monad.Extra.whenJust (top gh_versions) $ \gh_top ->
              Control.when (Just gh_top /= top s3_versions) $ do
                putStrLn $ "Updating top-level version from " <> (show $ top s3_versions) <> " to " <> (show $ top gh_versions)
                fetch_if_missing opts temp_dir gh_top
                Control.Monad.Extra.whenJust (top s3_versions) (fetch_if_missing opts temp_dir)
                update_top_level opts temp_dir gh_top (top s3_versions)
            putStrLn "Updating versions.json & hidden.json"
            update_s3 opts temp_dir gh_versions
        reset_cloudfront

download_assets :: FilePath -> GitHubRelease -> IO ()
download_assets tmp release = do
    manager <- HTTP.newManager TLS.tlsManagerSettings
    tokens <- Control.Concurrent.QSem.newQSem 20
    Control.Concurrent.Async.forConcurrently_ (map uri $ assets release) (\url ->
        bracket_
          (Control.Concurrent.QSem.waitQSem tokens)
          (Control.Concurrent.QSem.signalQSem tokens)
          (do
              req <- add_github_contact_header <$> HTTP.parseRequest (show url)
              HTTP.withResponse req manager (\resp -> do
                  let body = HTTP.responseBody resp
                  IO.withBinaryFile (tmp </> (last $ Network.URI.pathSegments url)) IO.AppendMode (\handle -> do
                      while (readFrom body) (writeTo handle)))))
  where while = Control.Monad.Loops.whileJust_
        readFrom body = ifNotEmpty <$> HTTP.brRead body
        ifNotEmpty bs = if Data.ByteString.null bs then Nothing else Just bs
        writeTo = Data.ByteString.hPut

verify_signatures :: FilePath -> FilePath -> String -> IO String
verify_signatures bash_lib tmp version_tag = do
    shell $ unlines ["bash -c '",
        "set -euo pipefail",
        "eval \"$(dev-env/bin/dade assist)\"",
        "source \"" <> bash_lib <> "\"",
        "shopt -s extglob", -- enable !() pattern: things that _don't_ match
        "cd \"" <> tmp <> "\"",
        "for f in !(*.asc); do",
            "p=" <> version_tag <> "/github/$f",
            "if ! test -f $f.asc; then",
                "echo $p: no signature file",
            "else",
                "LOG=$(mktemp)",
                "if gpg_verify $f.asc >$LOG 2>&1; then",
                    "echo $p: signature matches",
                "else",
                    "echo $p: signature does not match",
                    "echo Full gpg output:",
                    "cat $LOG",
                    "exit 2",
                "fi",
            "fi",
       "done",
       "'"]

does_backup_exist :: String -> FilePath -> FilePath -> IO Bool
does_backup_exist gcp_credentials bash_lib path = do
    out <- shell $ unlines ["bash -c '",
        "set -euo pipefail",
        "eval \"$(dev-env/bin/dade assist)\"",
        "source \"" <> bash_lib <> "\"",
        "GCRED=$(cat <<END",
        gcp_credentials,
        "END",
        ")",
        "if gcs \"$GCRED\" ls \"" <> path <> "\" >/dev/null; then",
            "echo True",
        "else",
            "echo False",
        "fi",
        "'"]
    return $ read out

gcs_cp :: String -> FilePath -> FilePath  -> FilePath -> IO ()
gcs_cp gcp_credentials bash_lib local_path remote_path = do
    shell_ $ unlines ["bash -c '",
        "set -euo pipefail",
        "eval \"$(dev-env/bin/dade assist)\"",
        "source \"" <> bash_lib <> "\"",
        "GCRED=$(cat <<END",
        gcp_credentials,
        "END",
        ")",
        "gcs \"$GCRED\" cp \"" <> local_path <> "\" \"" <> remote_path <> "\"",
        "'"]

check_files_match :: String -> String -> IO Bool
check_files_match f1 f2 = do
    (exitCode, stdout, stderr) <- System.readProcessWithExitCode "diff" [f1, f2] ""
    case exitCode of
      Exit.ExitSuccess -> return True
      Exit.ExitFailure 1 -> return False
      Exit.ExitFailure _ -> fail $ "Diff failed.\n" ++ "STDOUT:\n" ++ stdout ++ "\nSTDERR:\n" ++ stderr

check_releases :: Maybe String -> String -> Maybe Int -> IO ()
check_releases gcp_credentials bash_lib max_releases = do
    releases' <- fetch_gh_paginated "https://api.github.com/repos/digital-asset/daml/releases"
    let releases = case max_releases of
                     Nothing -> releases'
                     Just n -> take n releases'
    Data.Foldable.for_ releases (\release -> do
        let v = show $ tag release
        putStrLn $ "Checking release " <> v <> " ..."
        IO.withTempDir $ \temp_dir -> do
            download_assets temp_dir release
            verify_signatures bash_lib temp_dir v >>= putStrLn
            Control.Monad.Extra.whenJust gcp_credentials $ \gcred ->
                Directory.listDirectory temp_dir >>= Data.Foldable.traverse_ (\f -> do
                  let local_github = temp_dir </> f
                  let local_gcp = temp_dir </> f <> ".gcp"
                  let remote_gcp = "gs://daml-data/releases/" <> v <> "/github/" <> f
                  exists <- does_backup_exist gcred bash_lib remote_gcp
                  if exists then do
                      gcs_cp gcred bash_lib remote_gcp local_gcp
                      check_files_match local_github local_gcp >>= \case
                          True -> putStrLn $ f <> " matches GCS backup."
                          False -> Exit.die $ f <> " does not match GCS backup."
                  else do
                      Exit.die $ remote_gcp <> " does not exist. Aborting."))

data CliArgs = Docs
             | Check { bash_lib :: String,
                       gcp_credentials :: Maybe String,
                       max_releases :: Maybe Int }

parser :: Opt.ParserInfo CliArgs
parser = info "This program is meant to be run by CI cron. You probably don't have sufficient access rights to run it locally."
              (Opt.hsubparser (Opt.command "docs" docs
                            <> Opt.command "check" check))
  where info t p = Opt.info (p Opt.<**> Opt.helper) (Opt.progDesc t)
        docs = info "Build & push latest docs, if needed."
                    (pure Docs)
        check = info "Check existing releases."
                     (Check <$> Opt.strOption (Opt.long "bash-lib"
                                         <> Opt.metavar "PATH"
                                         <> Opt.help "Path to Bash library file.")
                            <*> (Opt.optional $
                                  Opt.strOption (Opt.long "gcp-creds"
                                         <> Opt.metavar "CRED_STRING"
                                         <> Opt.help "GCP credentials as a string."))
                            <*> (Opt.optional $
                                  Opt.option Opt.auto (Opt.long "max-releases"
                                         <> Opt.metavar "INT"
                                         <> Opt.help "Max number of releases to check.")))

main :: IO ()
main = do
    Control.forM_ [IO.stdout, IO.stderr] $
        \h -> IO.hSetBuffering h IO.LineBuffering
    opts <- Opt.execParser parser
    case opts of
      Docs -> do
          docs sdkDocOpts
          docs damlOnSqlDocOpts
      Check { bash_lib, gcp_credentials, max_releases } -> check_releases gcp_credentials bash_lib max_releases
