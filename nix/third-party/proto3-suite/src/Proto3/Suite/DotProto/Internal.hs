-- | This module provides misc internal helpers and utilities

{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE ViewPatterns      #-}

module Proto3.Suite.DotProto.Internal where

import qualified Control.Foldl             as FL
import           Control.Lens              (over)
import           Control.Lens.Cons         (_head)
import           Data.Char                 (toUpper)
import           Data.Maybe                (fromMaybe)
import qualified Data.Text                 as T
import qualified Filesystem.Path.CurrentOS as FP
import           Filesystem.Path.CurrentOS ((</>))
import qualified NeatInterpolation         as Neat
import           Prelude                   hiding (FilePath)
import           Proto3.Suite.DotProto
import           Text.Parsec               (ParseError)
import           Turtle                    (ExitCode (..), FilePath, MonadIO,
                                            Text)
import qualified Turtle
import           Turtle.Format             ((%))
import qualified Turtle.Format             as F

-- $setup
-- >>> :set -XOverloadedStrings

dieLines :: MonadIO m => Text -> m a
dieLines (Turtle.textToLines -> msg) = do
  mapM_ Turtle.err msg
  Turtle.exit (ExitFailure 1)

-- | toModulePath takes an include-relative path to a .proto file and produces a
-- "module path" which is used during code generation.
--
-- Note that, with the exception of the '.proto' portion of the input filepath,
-- this function interprets '.' in the filename components as if they were
-- additional slashes (assuming that the '.' is not the first character, which
-- is merely ignored). So e.g. "google/protobuf/timestamp.proto" and
-- "google.protobuf.timestamp.proto" map to the same module path.
--
-- >>> toModulePath "/absolute/path/fails.proto"
-- Left "expected include-relative path"
--
-- >>> toModulePath "relative/path/to/file_without_proto_suffix_fails"
-- Left "expected .proto suffix"
--
-- >>> toModulePath "relative/path/to/file_without_proto_suffix_fails.txt"
-- Left "expected .proto suffix"
--
-- >>> toModulePath "../foo.proto"
-- Left "expected include-relative path, but the path started with ../"
--
-- >>> toModulePath "foo..proto"
-- Left "path contained unexpected .. after canonicalization, please use form x.y.z.proto"
--
-- >>> toModulePath "foo/bar/baz..proto"
-- Left "path contained unexpected .. after canonicalization, please use form x.y.z.proto"
--
-- >>> toModulePath "foo.bar../baz.proto"
-- Left "path contained unexpected .. after canonicalization, please use form x.y.z.proto"
--
-- >>> toModulePath "google/protobuf/timestamp.proto"
-- Right (Path ["Google","Protobuf","Timestamp"])
--
-- >>> toModulePath "a/b/c/google.protobuf.timestamp.proto"
-- Right (Path ["A","B","C","Google","Protobuf","Timestamp"])
--
-- >>> toModulePath "foo/FiLeName_underscore.and.then.some.dots.proto"
-- Right (Path ["Foo","FiLeName_underscore","And","Then","Some","Dots"])
--
-- >>> toModulePath "foo/bar/././baz/../boggle.proto"
-- Right (Path ["Foo","Bar","Boggle"])
--
-- >>> toModulePath "./foo.proto"
-- Right (Path ["Foo"])
--
-- NB: We ignore preceding single '.' characters
-- >>> toModulePath ".foo.proto"
-- Right (Path ["Foo"])
toModulePath :: FilePath -> Either String Path
toModulePath fp0@(fromMaybe fp0 . FP.stripPrefix "./" -> fp)
  | Turtle.absolute fp
    = Left "expected include-relative path"
  | Turtle.extension fp /= Just "proto"
    = Left "expected .proto suffix"
  | otherwise
    = case FP.stripPrefix "../" fp of
        Just{}  -> Left "expected include-relative path, but the path started with ../"
        Nothing
          | T.isInfixOf ".." (Turtle.format F.fp . FP.collapse $ fp)
            -> Left "path contained unexpected .. after canonicalization, please use form x.y.z.proto"
          | otherwise
            -> Right
             . Path
             . dropWhile null -- Remove a potential preceding empty component which
                              -- arose from a preceding '.' in the input path, which we
                              -- want to ignore. E.g. ".foo.proto" => ["","Foo"].
             . fmap (T.unpack . over _head toUpper)
             . concatMap (T.splitOn ".")
             . T.splitOn "/"
             . Turtle.format F.fp
             . FP.collapse
             . Turtle.dropExtension
             $ fp

fatalBadModulePath :: MonadIO m => FilePath -> String -> m a
fatalBadModulePath (Turtle.format F.fp -> fp) (T.pack -> rsn) =
  dieLines [Neat.text|
    Error: failed when computing the "module path" for "${fp}": ${rsn}

    Please ensure that the provided path to a .proto file is specified as
    relative to some --includeDir path and that it has the .proto suffix.
  |]

-- | @importProto searchPaths toplevel inc@ attempts to import include-relative
-- @inc@ after locating it somewhere in the @searchPaths@; @toplevel@ is simply
-- the path of toplevel .proto being processed so we can report it in an error
-- message. This function terminates the program if it cannot find the file to
-- import or if it cannot construct a valid module path from it.
importProto :: MonadIO m
            => [FilePath] -> FilePath -> FilePath -> m (Either ParseError DotProto)
importProto paths (Turtle.format F.fp -> toplevelProtoText) protoFP =
  findProto paths protoFP >>= \case
    Found mp fp     -> parseProtoFile mp fp
    BadModulePath e -> fatalBadModulePath protoFP e
    NotFound        -> dieLines [Neat.text|
      Error: while processing include statements in "${toplevelProtoText}", failed
      to find the imported file "${protoFPText}", after looking in the following
      locations (controlled via the --includeDir switch(es)):

      $pathsText
    |]
  where
    pathsText   = T.unlines (Turtle.format ("  "%F.fp) . (</> protoFP) <$> paths)
    protoFPText = Turtle.format F.fp protoFP

data FindProtoResult
  = Found Path FilePath
  | NotFound
  | BadModulePath String
  deriving (Eq, Show)

-- | Attempts to locate the first (if any) filename that exists on the given
-- search paths, and constructs the "module path" from the given
-- include-relative filename (2nd parameter). Terminates the program with an
-- error if the given pathname is not relative.
findProto :: MonadIO m => [FilePath] -> FilePath -> m FindProtoResult
findProto searchPaths protoFP
  | Turtle.absolute protoFP = dieLines [Neat.text|
      Error: Absolute paths to .proto files, whether on the command line or
      in include directives, are not currently permitted; rather, all .proto
      filenames must be relative to the current directory, or relative to some
      search path specified via --includeDir.

      This is because we currently use the include-relative name to decide
      the structure of the Haskell module tree that we emit during code
      generation.
      |]
  | otherwise = case toModulePath protoFP of
      Left e -> pure (BadModulePath e)
      Right mp -> do
        mfp <- flip Turtle.fold FL.head $ do
          sp <- Turtle.select searchPaths
          let fp = sp </> protoFP
          True <- Turtle.testfile fp
          pure fp
        case mfp of
          Nothing -> pure NotFound
          Just fp -> pure (Found mp fp)
