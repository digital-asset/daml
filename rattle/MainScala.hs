{-# LANGUAGE ViewPatterns, RecordWildCards, LambdaCase #-}

module MainScala(main) where

import Rattle
import Metadata
import Util
import Data.Char
import Data.Maybe
import System.IO.Extra
import System.Info.Extra
import System.Process.Extra
import System.FilePattern.Directory
import System.FilePath
import System.Directory
import System.Environment
import System.IO.Unsafe
import Control.Monad.Extra
import Data.List.Extra

bash :: String -> IO ()
bash s = cmd_ "bash" ["-v", "-c", s]

main :: IO ()
main = rattle $ do
    putStrLn "  ---- Starting SCALA rattle build ----"
    bash "(cd daml-lf/archive; protoc da/*.proto --java_out=src/main/scala)"
    bash "(cd daml-lf/transaction/src/main/protobuf;  protoc com/digitalasset/daml/lf/*.proto --java_out=../scala)"

    bash "cp rattle/build.sbt daml-lf"
    bash "(cd daml-lf; sbt compile)"
    --bash "(cd daml-lf; sbt test)"
