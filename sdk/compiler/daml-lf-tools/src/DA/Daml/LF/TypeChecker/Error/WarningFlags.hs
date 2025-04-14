-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.TypeChecker.Error.WarningFlags (
        module DA.Daml.LF.TypeChecker.Error.WarningFlags
    ) where

import qualified Data.List as L
import Data.Functor.Contravariant

data WarningFlagStatus
  = AsError -- -Werror=<name>
  | AsWarning -- -W<name> or -Wwarn=<name> or -Wno-error=<name>
  | Hidden -- -Wno-<name>

data WarningFlagSpec err
  = WarningFlagSpec
    { dwfsName :: String
    , dwfsHidden :: Bool
    , dwfsFilter :: err -> Bool
    }

specToFlag :: WarningFlagSpec err -> WarningFlagStatus -> WarningFlag err
specToFlag spec status = WarningFlag { rfStatus = status, rfName = dwfsName spec, rfFilter = dwfsFilter spec }

data WarningFlag err
  = WarningFlag
    { rfName :: String
    , rfStatus :: WarningFlagStatus
    , rfFilter :: err -> Bool
    }

mkWarningFlagParser :: (err -> WarningFlagStatus) -> [WarningFlagSpec err] -> WarningFlagParser err
mkWarningFlagParser dwfpDefault specs =
  WarningFlagParser
    { dwfpDefault = dwfpDefault
    , dwfpFlagParsers = map specToMapEntry specs
    , dwfpSuggestFlag = \err -> dwfsName <$> L.find (specCanMatchErr err) specs
    }
  where
    specToMapEntry :: WarningFlagSpec err -> (String, WarningFlagStatus -> WarningFlag err)
    specToMapEntry spec = (dwfsName spec, specToFlag spec)

    specCanMatchErr :: err -> WarningFlagSpec err -> Bool
    specCanMatchErr err spec = not (dwfsHidden spec) && dwfsFilter spec err

data WarningFlagParser err = WarningFlagParser
  { dwfpDefault :: err -> WarningFlagStatus
  , dwfpFlagParsers :: [(String, WarningFlagStatus -> WarningFlag err)]
  , dwfpSuggestFlag :: err -> Maybe String
  }

data WarningFlags err = WarningFlags
  { dwfDefault :: err -> WarningFlagStatus
  , dwfFlags :: [WarningFlag err]
  }

instance Contravariant WarningFlag where
  contramap f flag = flag { rfFilter = rfFilter flag . f }

instance Contravariant WarningFlagParser where
  contramap f WarningFlagParser {..} =
    WarningFlagParser
      { dwfpDefault = dwfpDefault . f
      , dwfpFlagParsers = (fmap . fmap . fmap) (contramap f) dwfpFlagParsers
      , dwfpSuggestFlag = dwfpSuggestFlag . f
      }

instance Contravariant WarningFlags where
  contramap f WarningFlags {..} =
    WarningFlags
      { dwfDefault = dwfDefault . f
      , dwfFlags = fmap (contramap f) dwfFlags
      }

splitWarningFlags :: WarningFlags (Either err1 err2) -> (WarningFlags err1, WarningFlags err2)
splitWarningFlags flags = (contramap Left flags, contramap Right flags)

combineParsers :: WarningFlagParser err1 -> WarningFlagParser err2 -> WarningFlagParser (Either err1 err2)
combineParsers left right =
  WarningFlagParser
    { dwfpDefault = either (dwfpDefault left) (dwfpDefault right)
    , dwfpFlagParsers =
        (fmap . fmap . fmap) (mapFlagFilter (flip either (const False))) (dwfpFlagParsers left) ++
        (fmap . fmap . fmap) (mapFlagFilter (either (const False))) (dwfpFlagParsers right)
    , dwfpSuggestFlag = either (dwfpSuggestFlag left) (dwfpSuggestFlag right)
    }

mapFlagFilter :: ((subErr -> Bool) -> superErr -> Bool) -> WarningFlag subErr -> WarningFlag superErr
mapFlagFilter f flag = flag { rfFilter = f (rfFilter flag) }

parseWarningFlag
  :: WarningFlagParser err
  -> String -> Either String (WarningFlag err)
parseWarningFlag parser@WarningFlagParser { dwfpFlagParsers } = \case
  ('e':'r':'r':'o':'r':'=':name) -> parseNameE name AsError
  ('n':'o':'-':'e':'r':'r':'o':'r':'=':name) -> parseNameE name AsWarning
  ('n':'o':'-':name) -> parseNameE name Hidden
  ('w':'a':'r':'n':'=':name) -> parseNameE name AsWarning
  name -> parseNameE name AsWarning
  where
  parseNameE name status = case lookup name dwfpFlagParsers of
    Nothing -> Left $ "Warning flag is not valid - warning flags must be of the form `-Werror=<name>`, `-Wno-<name>`, or `-W<name>`. Available names are: " <> namesAsList parser
    Just flag -> Right (flag status)

namesAsList :: WarningFlagParser err -> String
namesAsList WarningFlagParser {dwfpFlagParsers} = L.intercalate ", " (map fst dwfpFlagParsers)

getWarningStatus :: WarningFlags err -> err -> WarningFlagStatus
getWarningStatus WarningFlags { dwfDefault, dwfFlags } err =
  case filter (\flag -> rfFilter flag err) dwfFlags of
    [] -> dwfDefault err
    xs -> rfStatus (last xs)

mkWarningFlags :: WarningFlagParser err -> [WarningFlag err] -> WarningFlags err
mkWarningFlags parser flags = WarningFlags
  { dwfFlags = flags
  , dwfDefault = dwfpDefault parser
  }

addWarningFlags :: [WarningFlag err] -> WarningFlags err -> WarningFlags err
addWarningFlags newFlags flags = flags { dwfFlags = dwfFlags flags ++ newFlags }
