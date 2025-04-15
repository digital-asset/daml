-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeOperators #-}

-- | This module defines the logic around flags
module DA.Daml.LF.TypeChecker.Error.WarningFlags (
        module DA.Daml.LF.TypeChecker.Error.WarningFlags
    ) where

import Data.Kind (Type)
import Options.Applicative
import qualified Text.PrettyPrint.ANSI.Leijen as PAL
import qualified Data.List as L
import Data.Functor.Contravariant

data WarningFlagStatus
  = AsError -- -Werror=<name>
  | AsWarning -- -W<name> or -Wwarn=<name> or -Wno-error=<name>
  | Hidden -- -Wno-<name>

data WarningFlagSpec err
  = WarningFlagSpec
    { wfsName :: String
    , wfsHidden :: Bool
    , wfsFilter :: err -> Bool
    }

specToFlag :: WarningFlagSpec err -> WarningFlagStatus -> WarningFlag err
specToFlag spec status = WarningFlag { wfStatus = status, wfName = wfsName spec, wfFilter = wfsFilter spec }

data WarningFlag err
  = WarningFlag
    { wfName :: String
    , wfStatus :: WarningFlagStatus
    , wfFilter :: err -> Bool
    }

modifyWfFilter :: ((subErr -> Bool) -> superErr -> Bool) -> WarningFlag subErr -> WarningFlag superErr
modifyWfFilter f flag = flag { wfFilter = f $ wfFilter flag }

mkWarningFlagParser :: (err -> WarningFlagStatus) -> [WarningFlagSpec err] -> WarningFlagParser err
mkWarningFlagParser dwfpDefault specs =
  WarningFlagParser
    { dwfpDefault = dwfpDefault
    , dwfpFlagParsers = map specToMapEntry specs
    , dwfpSuggestFlag = \err -> wfsName <$> L.find (specCanMatchErr err) specs
    }
  where
    specToMapEntry :: WarningFlagSpec err -> (String, WarningFlagStatus -> WarningFlag err)
    specToMapEntry spec = (wfsName spec, specToFlag spec)

    specCanMatchErr :: err -> WarningFlagSpec err -> Bool
    specCanMatchErr err spec = not (wfsHidden spec) && wfsFilter spec err

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
  contramap f = modifyWfFilter (. f)

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
  case filter (\flag -> wfFilter flag err) dwfFlags of
    [] -> dwfDefault err
    xs -> wfStatus (last xs)

mkWarningFlags :: WarningFlagParser err -> [WarningFlag err] -> WarningFlags err
mkWarningFlags parser flags = WarningFlags
  { dwfFlags = flags
  , dwfDefault = dwfpDefault parser
  }

addWarningFlags :: [WarningFlag err] -> WarningFlags err -> WarningFlags err
addWarningFlags newFlags flags = flags { dwfFlags = dwfFlags flags ++ newFlags }

runParser :: WarningFlagParser a -> Parser (WarningFlags a)
runParser parser =
  mkWarningFlags parser <$>
    many (Options.Applicative.option
      (eitherReader (parseWarningFlag parser))
      (short 'W' <> helpDoc (Just helpStr)))
  where
  helpStr =
    PAL.vcat
      [ "Turn an error into a warning with -W<name> or -Wwarn=<name> or -Wno-error=<name>"
      , "Turn a warning into an error with -Werror=<name>"
      , "Disable warnings and errors with -Wno-<name>"
      , "Available names are: " <> PAL.string (namesAsList parser)
      ]


data Product (xs :: [Type]) where
  ProdZ :: Product '[]
  ProdT :: a -> Product xs -> Product (a ': xs)

class ProductTupleIso xs tuple | xs -> tuple, tuple -> xs where
  toTuple :: Product xs -> tuple
  fromTuple :: tuple -> Product xs

instance ProductTupleIso '[a, b] (a, b) where
  toTuple (ProdT a (ProdT b ProdZ)) = (a, b)
  fromTuple (a, b) = ProdT a (ProdT b ProdZ)

instance ProductTupleIso '[a, b, c] (a, b, c) where
  toTuple (ProdT a (ProdT b (ProdT c ProdZ))) = (a, b, c)
  fromTuple (a, b, c) = ProdT a (ProdT b (ProdT c ProdZ))

instance ProductTupleIso '[a, b, c, d] (a, b, c, d) where
  toTuple (ProdT a (ProdT b (ProdT c (ProdT d ProdZ)))) = (a, b, c, d)
  fromTuple (a, b, c, d) = ProdT a (ProdT b (ProdT c (ProdT d ProdZ)))

data Sum (rest :: [Type]) where
  SumL :: a -> Sum (a ': xs)
  SumR :: Sum xs -> Sum (a ': xs)

eitherSum :: (l -> x) -> (Sum r -> x) -> Sum (l ': r) -> x
eitherSum lx _  (SumL l) = lx l
eitherSum _  rx (SumR r) = rx r

type WarningFlagParsers xs = WarningFlagParser (Sum xs)

voidParser :: WarningFlagParsers '[]
voidParser = mkWarningFlagParser (const (error "voidParser: should not be able to get a flag with SumR")) []

--combineParsers :: WarningFlagParser left -> WarningFlagParser (Sum rest) -> WarningFlagParser (Sum (left ': rest))
--combineParsers left rest =
--  WarningFlagParser
--    { dwfpDefault = eitherSum (dwfpDefault left) (dwfpDefault rest)
--    , dwfpFlagParsers =
--        (fmap . fmap . fmap) (modifyWfFilter (flip eitherSum (const False))) (dwfpFlagParsers left) ++
--        (fmap . fmap . fmap) (modifyWfFilter (eitherSum (const False))) (dwfpFlagParsers rest)
--    , dwfpSuggestFlag = eitherSum (dwfpSuggestFlag left) (dwfpSuggestFlag rest)
--    }

combineParsers :: (ProductTupleIso (CombineParsersType xs) r, CombineParsers xs) => r -> WarningFlagParser (Sum xs)
combineParsers = combineParsersInternal . fromTuple

class CombineParsers (xs :: [Type]) where
  type CombineParsersType xs :: [Type]
  combineParsersInternal :: Product (CombineParsersType xs) -> WarningFlagParser (Sum xs)

instance CombineParsers '[] where
  type CombineParsersType '[] = '[]
  combineParsersInternal ProdZ = voidParser

instance CombineParsers xs => CombineParsers (a ': xs) where
  type CombineParsersType (a ': xs) = WarningFlagParser a ': CombineParsersType xs
  combineParsersInternal (ProdT left restSum) =
    let rest = combineParsersInternal restSum
    in
    WarningFlagParser
      { dwfpDefault = eitherSum (dwfpDefault left) (dwfpDefault rest)
      , dwfpFlagParsers =
          (fmap . fmap . fmap) (modifyWfFilter (flip eitherSum (const False))) (dwfpFlagParsers left) ++
          (fmap . fmap . fmap) (modifyWfFilter (eitherSum (const False))) (dwfpFlagParsers rest)
      , dwfpSuggestFlag = eitherSum (dwfpSuggestFlag left) (dwfpSuggestFlag rest)
      }

splitWarningFlags :: (ProductTupleIso (SplitWarningFlagsType xs) r, SplitWarningFlags xs) => WarningFlags (Sum xs) -> r
splitWarningFlags = toTuple . splitWarningFlagsInternal

class SplitWarningFlags xs where
  type SplitWarningFlagsType xs :: [Type]
  splitWarningFlagsInternal :: WarningFlags (Sum xs) -> Product (SplitWarningFlagsType xs)

instance SplitWarningFlags '[] where
  type SplitWarningFlagsType '[] = '[]
  splitWarningFlagsInternal = const ProdZ

instance SplitWarningFlags rest => SplitWarningFlags (a ': rest) where
  type SplitWarningFlagsType (a ': rest) = WarningFlags a ': SplitWarningFlagsType rest
  splitWarningFlagsInternal a = ProdT (contramap SumL a) (splitWarningFlagsInternal (contramap SumR a))
