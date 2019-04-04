{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}
{-# LANGUAGE TypeOperators       #-}

import           Options.Generic
import           Prelude                        hiding (FilePath)
import           Proto3.Suite.DotProto.Generate
import           Turtle                         (FilePath)

data Args w = Args
  { out        :: w ::: FilePath   <?> "Output directory path where generated Haskell modules will be written (directory is created if it does not exist; note that files in the output directory may be overwritten!)"
  , includeDir :: w ::: [FilePath] <?> "Path to search for included .proto files (can be repeated, and paths will be searched in order; the current directory is used if this option is not provided)"
  , proto      :: w ::: FilePath   <?> "Path to input .proto file"
  , extraInstanceFile :: w ::: [FilePath] <?> "Additional file to provide instances that would otherwise be generated. Can be used multiple times. Types for which instance overrides are given must be fully qualified."
  } deriving Generic
instance ParseRecord (Args Wrapped)
deriving instance Show (Args Unwrapped)

main :: IO ()
main = do
  Args{..} :: Args Unwrapped <- unwrapRecord "Compiles a .proto file to a Haskell module"
  compileDotProtoFileOrDie extraInstanceFile out includeDir proto
