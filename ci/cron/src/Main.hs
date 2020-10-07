-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import Data.Function ((&))
import Data.Semigroup ((<>))
import System.FilePath.Posix ((</>))

import qualified Control.Exception
import qualified Control.Monad as Control
import qualified Data.Aeson as JSON
import qualified Data.ByteString.UTF8 as BS
import qualified Data.ByteString.Lazy.UTF8 as LBS
import qualified Data.CaseInsensitive as CI
import qualified Data.Foldable
import qualified Data.HashMap.Strict as H
import qualified Data.List
import qualified Data.List.Split as Split
import qualified Data.Ord
import qualified Data.SemVer
import qualified Data.Set as Set
import qualified Data.Text as Text
import qualified Options.Applicative as Opt
import qualified Network.HTTP.Client as HTTP
import qualified Network.HTTP.Client.TLS as TLS
import qualified Network.HTTP.Types.Status as Status
import qualified Network.URI
import qualified System.Directory as Directory
import qualified System.Exit as Exit
import qualified System.IO.Extra as IO
import qualified System.Process as System
import qualified Text.Regex.TDFA as Regex

shell_exit_code :: String -> IO (Exit.ExitCode, String, String)
shell_exit_code cmd = do
    System.readCreateProcessWithExitCode (System.shell cmd) ""

die :: String -> Exit.ExitCode -> String -> String -> IO a
die cmd (Exit.ExitFailure exit) out err =
    Exit.die $ unlines ["Subprocess:",
                         cmd,
                        "failed with exit code " <> show exit <> "; output:",
                        "---",
                        out,
                        "---",
                        "err:",
                        "---",
                        err,
                        "---"]
die _ _ _ _ = Exit.die "Type system too weak."


shell :: String -> IO String
shell cmd = do
    (exit, out, err) <- shell_exit_code cmd
    if exit == Exit.ExitSuccess
    then return out
    else die cmd exit out err

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
              (_, 0) -> die cmd exit out err
              _ | "unexpected end-of-file" `Data.List.isInfixOf` err -> h (n - 1)
              _ -> die cmd exit out err

http_get :: JSON.FromJSON a => String -> IO (a, H.HashMap String String)
http_get url = do
    manager <- HTTP.newManager TLS.tlsManagerSettings
    request' <- HTTP.parseRequest url
    -- Be polite
    let request = request' { HTTP.requestHeaders = [("User-Agent", "DAML cron (team-daml-language@digitalasset.com)")] }
    response <- HTTP.httpLbs request manager
    let body = JSON.decode $ HTTP.responseBody response
    let status = Status.statusCode $ HTTP.responseStatus response
    case (status, body) of
      (200, Just body) -> return (body, response & HTTP.responseHeaders & map (\(n, v) -> (n & CI.foldedCase & BS.toString, BS.toString v)) & H.fromList)
      _ -> Exit.die $ unlines ["GET \"" <> url <> "\" returned status code " <> show status <> ".",
                               show $ HTTP.responseBody response]

build_and_push :: FilePath -> [Version] -> IO ()
build_and_push temp versions = do
    restore_sha $ do
        Data.Foldable.for_ versions (\version -> do
            putStrLn $ "Building " <> show version <> "..."
            build version
            putStrLn $ "Pushing " <> show version <> " to S3 (as subfolder)..."
            push version
            putStrLn "Done.")
    where
        restore_sha io =
            Control.Exception.bracket (init <$> shell "git symbolic-ref --short HEAD 2>/dev/null || git rev-parse HEAD")
                                      (\cur_sha -> shell_ $ "git checkout " <> cur_sha)
                                      (const io)
        build version = do
            shell_ $ "git checkout v" <> show version
            robustly_download_nix_packages
            shell_ $ "DAML_SDK_RELEASE_VERSION=" <> show version <> " bazel build //docs:docs"
            shell_ $ "mkdir -p  " <> temp </> show version
            shell_ $ "tar xzf bazel-bin/docs/html.tar.gz --strip-components=1 -C" <> temp </> show version
        push version =
            shell_ $ "aws s3 cp " <> (temp </> show version) </> " " <> "s3://docs-daml-com" </> show version </> " --recursive --acl public-read"

fetch_if_missing :: FilePath -> Version -> IO ()
fetch_if_missing temp v = do
    missing <- not <$> Directory.doesDirectoryExist (temp </> show v)
    if missing then do
        putStrLn $ "Downloading " <> show v <> "..."
        shell_ $ "aws s3 cp s3://docs-daml-com" </> show v <> " " <> temp </> show v <> " --recursive"
        putStrLn "Done."
    else do
        putStrLn $ show v <> " already present."

update_s3 :: FilePath -> Versions -> IO ()
update_s3 temp vs = do
    putStrLn "Updating versions.json & hidden.json..."
    create_versions_json (dropdown vs) (temp </> "versions.json")
    let hidden = Data.List.sortOn Data.Ord.Down $ Set.toList $ all_versions vs `Set.difference` (Set.fromList $ dropdown vs)
    create_versions_json hidden (temp </> "hidden.json")
    shell_ $ "aws s3 cp " <> temp </> "versions.json s3://docs-daml-com/versions.json --acl public-read"
    shell_ $ "aws s3 cp " <> temp </> "hidden.json s3://docs-daml-com/hidden.json --acl public-read"
    -- FIXME: remove after running once (and updating the reading bit in this file)
    shell_ $ "aws s3 cp " <> temp </> "hidden.json s3://docs-daml-com/snapshots.json --acl public-read"
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

update_top_level :: FilePath -> Version -> Version -> IO ()
update_top_level temp new old = do
    new_files <- Set.fromList <$> Directory.listDirectory (temp </> show new)
    old_files <- Set.fromList <$> Directory.listDirectory (temp </> show old)
    let to_delete = Set.toList $ old_files `Set.difference` new_files
    Control.when (not $ null to_delete) $ do
        putStrLn $ "Deleting top-level files: " <> show to_delete
        Data.Foldable.for_ to_delete (\f -> do
            shell_ $ "aws s3 rm s3://docs-daml-com" </> f <> " --recursive")
        putStrLn "Done."
    putStrLn $ "Pushing " <> show new <> " to top-level..."
    shell_ $ "aws s3 cp " <> temp </> show new </> " s3://docs-daml-com/ --recursive --acl public-read"
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

data Versions = Versions { top :: Version, all_versions :: Set.Set Version, dropdown :: [Version] }
    deriving Eq

versions :: [GitHubRelease] -> Versions
versions vs =
    let all_versions = Set.fromList $ map tag vs
        dropdown = vs
                   & filter (not . prerelease)
                   & map tag
                   & filter (>= version "1.0.0")
                   & Data.List.sortOn Data.Ord.Down
        top = head dropdown
    in Versions {..}

fetch_gh_versions :: IO Versions
fetch_gh_versions = do
    response <- fetch_gh_paginated "https://api.github.com/repos/digital-asset/daml/releases"
    -- versions prior to 0.13.10 cannot be built anymore and are not present in
    -- the repo.
    return $ versions $ filter (\v -> tag v >= version "0.13.10") response

fetch_s3_versions :: IO Versions
fetch_s3_versions = do
    dropdown <- fetch "versions.json" False
    -- TODO: read hidden.json after this has run once
    hidden <- fetch "snapshots.json" True
    return $ versions $ dropdown <> hidden
    where fetch file prerelease = do
              temp <- shell "mktemp"
              shell_ $ "aws s3 cp s3://docs-daml-com/" <> file <> " " <> temp
              s3_raw <- shell $ "cat " <> temp
              let type_annotated_value :: Maybe JSON.Object
                  type_annotated_value = JSON.decode $ LBS.fromString s3_raw
              case type_annotated_value of
                  Just s3_json -> return $ map (\s -> GitHubRelease prerelease (version s) []) $ H.keys s3_json
                  Nothing -> Exit.die "Failed to get versions from s3"

docs :: IO ()
docs = do
    Control.forM_ [IO.stdout, IO.stderr] $
        \h -> IO.hSetBuffering h IO.LineBuffering
    putStrLn "Checking for new version..."
    gh_versions <- fetch_gh_versions
    s3_versions <- fetch_s3_versions
    if s3_versions == gh_versions
    then do
        putStrLn "Versions match, nothing to do."
    else do
        -- We may have added versions. We need to build and push them.
        let added = Set.toList $ all_versions gh_versions `Set.difference` all_versions s3_versions
        IO.withTempDir $ \temp_dir -> do
            putStrLn $ "Versions to build: " <> show added
            build_and_push temp_dir added
            Control.when (top gh_versions /= top s3_versions) $ do
                putStrLn $ "Updating top-level version from " <> (show $ top s3_versions) <> " to " <> (show $ top gh_versions)
                fetch_if_missing temp_dir (top gh_versions)
                fetch_if_missing temp_dir (top s3_versions)
                update_top_level temp_dir (top gh_versions) (top s3_versions)
            putStrLn "Updating versions.json & hidden.json"
            update_s3 temp_dir gh_versions
        reset_cloudfront

check_releases :: String -> IO ()
check_releases bash_lib = do
    releases <- fetch_gh_paginated "https://api.github.com/repos/digital-asset/daml/releases"
    Data.Foldable.for_ releases (\release -> do
        let v = show $ tag release
        putStrLn $ "Checking release " <> v <> " ..."
        out <- shell $ unlines ["bash -c '",
              "set -euo pipefail",
              "eval \"$(dev-env/bin/dade assist)\"",
              "source \"" <> bash_lib <> "\"",

              "LOG=$(mktemp)",
              "DIR=$(mktemp -d)",
              "trap \"rm -rf \\\"$DIR\\\"\" EXIT",
              "cd \"$DIR\"",

              "shopt -s extglob", -- enable !() pattern: things that _don't_ match

              "PIDS=\"\"",
              "for ass in " <> unwords (map (show . uri) $ assets release) <> "; do",
                  "{",
                      "wget --quiet \"$ass\" &",
                  "} >$LOG 2>&1",
                  "PIDS=\"$PIDS $!\"",
              "done",
              "for pid in $PIDS; do",
                  "wait $pid >$LOG 2>&1",
              "done",
              "for f in !(*.asc); do",
                  "p=github/" <> v <> "/$f",
                  "if ! test -f $f.asc; then",
                      "echo $p: no signature file",
                  "else",
                      "if gpg_verify $f.asc >$LOG 2>&1; then",
                          "echo $p: signature matches",
                      "else",
                          "echo $p: signature does not match",
                          "exit 2",
                      "fi",
                  "fi",
              "done",
          "'"]
        putStrLn out)

data CliArgs = Docs | Check { bash_lib :: String }

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
                                         <> Opt.help "Path to Bash library file."))

main :: IO ()
main = do
    opts <- Opt.execParser parser
    case opts of
      Docs -> docs
      Check { bash_lib } -> check_releases bash_lib
