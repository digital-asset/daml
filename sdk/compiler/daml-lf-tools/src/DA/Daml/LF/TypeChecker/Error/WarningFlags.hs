-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.TypeChecker.Error.WarningFlags (
        module DA.Daml.LF.TypeChecker.Error.WarningFlags
    ) where

import qualified Data.List as L
import Data.Functor.Contravariant

data DamlWarningFlagStatus
  = AsError -- -Werror=<name>
  | AsWarning -- -W<name> or -Wwarn=<name> or -Wno-error=<name>
  | Hidden -- -Wno-<name>

data DamlWarningFlagSpec err
  = DamlWarningFlagSpec
    { dwfsName :: String
    , dwfsHidden :: Bool
    , dwfsFilter :: err -> Bool
    }

specToFlag :: DamlWarningFlagSpec err -> DamlWarningFlagStatus -> DamlWarningFlag err
specToFlag spec status = RawDamlWarningFlag { rfStatus = status, rfName = dwfsName spec, rfFilter = dwfsFilter spec }

data DamlWarningFlag err
  = RawDamlWarningFlag
    { rfName :: String
    , rfStatus :: DamlWarningFlagStatus
    , rfFilter :: err -> Bool
    }

mkDamlWarningFlagParser :: (err -> DamlWarningFlagStatus) -> [DamlWarningFlagSpec err] -> DamlWarningFlagParser err
mkDamlWarningFlagParser dwfpDefault specs =
  DamlWarningFlagParser
    { dwfpDefault = dwfpDefault
    , dwfpFlagParsers = map specToMapEntry specs
    , dwfpSuggestFlag = \err -> dwfsName <$> L.find (specCanMatchErr err) specs
    }
  where
    specToMapEntry :: DamlWarningFlagSpec err -> (String, DamlWarningFlagStatus -> DamlWarningFlag err)
    specToMapEntry spec = (dwfsName spec, specToFlag spec)

    specCanMatchErr :: err -> DamlWarningFlagSpec err -> Bool
    specCanMatchErr err spec = not (dwfsHidden spec) && dwfsFilter spec err

data DamlWarningFlagParser err = DamlWarningFlagParser
  { dwfpDefault :: err -> DamlWarningFlagStatus
  , dwfpFlagParsers :: [(String, DamlWarningFlagStatus -> DamlWarningFlag err)]
  , dwfpSuggestFlag :: err -> Maybe String
  }

data DamlWarningFlags err = DamlWarningFlags
  { dwfDefault :: err -> DamlWarningFlagStatus
  , dwfFlags :: [DamlWarningFlag err]
  }

instance Contravariant DamlWarningFlag where
  contramap f flag = flag { rfFilter = rfFilter flag . f }

instance Contravariant DamlWarningFlagParser where
  contramap f DamlWarningFlagParser {..} =
    DamlWarningFlagParser
      { dwfpDefault = dwfpDefault . f
      , dwfpFlagParsers = (fmap . fmap . fmap) (contramap f) dwfpFlagParsers
      , dwfpSuggestFlag = dwfpSuggestFlag . f
      }

instance Contravariant DamlWarningFlags where
  contramap f DamlWarningFlags {..} =
    DamlWarningFlags
      { dwfDefault = dwfDefault . f
      , dwfFlags = fmap (contramap f) dwfFlags
      }

combineParsers :: DamlWarningFlagParser err1 -> DamlWarningFlagParser err2 -> DamlWarningFlagParser (Either err1 err2)
combineParsers left right =
  DamlWarningFlagParser
    { dwfpDefault = either (dwfpDefault left) (dwfpDefault right)
    , dwfpFlagParsers =
        (fmap . fmap . fmap) toLeft (dwfpFlagParsers left) ++
        (fmap . fmap . fmap) toRight (dwfpFlagParsers right)
    , dwfpSuggestFlag = either (dwfpSuggestFlag left) (dwfpSuggestFlag right)
    }

toLeft :: DamlWarningFlag err -> DamlWarningFlag (Either err x)
toLeft = mapFlagFilter (\x -> either x (const False))

toRight :: DamlWarningFlag err -> DamlWarningFlag (Either x err)
toRight = mapFlagFilter (\x -> either (const False) x)

mapFlagFilter :: ((subErr -> Bool) -> superErr -> Bool) -> DamlWarningFlag subErr -> DamlWarningFlag superErr
mapFlagFilter f flag = flag { rfFilter = f (rfFilter flag) }

parseRawDamlWarningFlag
  :: DamlWarningFlagParser err
  -> String -> Either String (DamlWarningFlag err)
parseRawDamlWarningFlag parser@DamlWarningFlagParser { dwfpFlagParsers } = \case
  ('e':'r':'r':'o':'r':'=':name) -> parseNameE name AsError
  ('n':'o':'-':'e':'r':'r':'o':'r':'=':name) -> parseNameE name AsWarning
  ('n':'o':'-':name) -> parseNameE name Hidden
  ('w':'a':'r':'n':'=':name) -> parseNameE name AsWarning
  name -> parseNameE name AsWarning
  where
  parseNameE name status = case lookup name dwfpFlagParsers of
    Nothing -> Left $ "Warning flag is not valid - warning flags must be of the form `-Werror=<name>`, `-Wno-<name>`, or `-W<name>`. Available names are: " <> namesAsList parser
    Just flag -> Right (flag status)

namesAsList :: DamlWarningFlagParser err -> String
namesAsList DamlWarningFlagParser {dwfpFlagParsers} = L.intercalate ", " (map fst dwfpFlagParsers)

getWarningStatus :: DamlWarningFlags err -> err -> DamlWarningFlagStatus
getWarningStatus DamlWarningFlags { dwfDefault, dwfFlags } err =
  case filter (\flag -> rfFilter flag err) dwfFlags of
    [] -> dwfDefault err
    xs -> rfStatus (last xs)

mkDamlWarningFlags :: DamlWarningFlagParser err -> [DamlWarningFlag err] -> DamlWarningFlags err
mkDamlWarningFlags parser flags = DamlWarningFlags
  { dwfFlags = flags
  , dwfDefault = dwfpDefault parser
  }

addDamlWarningFlags :: [DamlWarningFlag err] -> DamlWarningFlags err -> DamlWarningFlags err
addDamlWarningFlags newFlags flags = flags { dwfFlags = dwfFlags flags ++ newFlags }
