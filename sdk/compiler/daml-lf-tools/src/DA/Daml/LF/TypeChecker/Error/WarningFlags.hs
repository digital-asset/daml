-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | This module defines the logic around flags
--
-- A parser for a set of warning flags for warnings of type `err` is a
-- `WarningFlagParser err`. It is constructed using `mkWarningFlagParser`, which
-- takes multiple WarningFlagSpecs (one for each flag), and a total function
-- which defines a default value for each possible warning (`wfpDefault`).
--
-- The WarningFlagSpec specifies the name of a flag (`wfsName`), whether it
-- should show up in the help (`wfsHidden`), and which errs it targets
-- (`wfsFilter`). This lets a flag to target multiple possible warnings - for
-- example, the `upgraded-template-expression-changed` affects the warning level
-- for the following warnings:
--   WEUpgradedTemplateChangedPrecondition,
--   WEUpgradedTemplateChangedSignatories,
--   WEUpgradedTemplateChangedObservers,
--   WEUpgradedTemplateChangedAgreement,
--   WEUpgradedTemplateChangedKeyExpression,
--   WEUpgradedTemplateChangedKeyMaintainers
--
-- The `WarningFlagParser err` parses flags according to its specs from the
-- command line to produce a `WarningFlags err` datatype - this datatype
-- contains the same default function, and generates a WarningFlag for each flag
-- that was specified on the command line. WarningFlag datatype copies verbatim
-- the name and filter from the WarningFlagSpec that it originates from, along
-- with the warning level that was specified.
--
-- The `WarningFlags err` datatype can be queried with `getWarningStatus` to get
-- the WarningFlagStatus for any warning of type `err`. If the user specified
-- the warning level for a flag which matches the warning, that warning level
-- will be returned. If no flag was specified matching the warning, the
-- warning's default level will be returned from `wfDefault`.
--
-- A tuple of N WarningFlagParsers can be combined with combineParsers to
-- produce a parser that will parse flags which match `Sum [err1, err2, ..., errN]`.
-- The resulting `WarningFlags (Sum [err1, ..., errN])` can be split into a
-- tuple of N new WarningFlags datatypes for each error type:
-- `(WarningFlags err1, WarningFlags err2, ..., WarningFlags errN)`

module DA.Daml.LF.TypeChecker.Error.WarningFlags (
        module DA.Daml.LF.TypeChecker.Error.WarningFlags
    ) where

import Options.Applicative
import qualified Text.PrettyPrint.ANSI.Leijen as PAL
import qualified Data.List as L
import Data.Functor.Contravariant
import Data.HList

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

modifyWfsFilter :: ((subErr -> Bool) -> superErr -> Bool) -> WarningFlagSpec subErr -> WarningFlagSpec superErr
modifyWfsFilter f flag = flag { wfsFilter = f $ wfsFilter flag }

data WarningFlag err
  = WarningFlag
    { wfName :: String
    , wfStatus :: WarningFlagStatus
    , wfFilter :: err -> Bool
    }

mkWarningFlagParser :: (err -> WarningFlagStatus) -> [WarningFlagSpec err] -> WarningFlagParser err
mkWarningFlagParser wfpDefault specs =
  WarningFlagParser
    { wfpDefault = wfpDefault
    , wfpFlagParsers = specs
    , wfpSuggestFlag = \err -> wfsName <$> L.find (specCanMatchErr err) specs
    }
  where
    specCanMatchErr :: err -> WarningFlagSpec err -> Bool
    specCanMatchErr err spec = not (wfsHidden spec) && wfsFilter spec err

data WarningFlagParser err = WarningFlagParser
  { wfpDefault :: err -> WarningFlagStatus
  , wfpFlagParsers :: [WarningFlagSpec err]
  , wfpSuggestFlag :: err -> Maybe String
  }

data WarningFlags err = WarningFlags
  { wfDefault :: err -> WarningFlagStatus
  , wfFlags :: [WarningFlag err]
  }

instance Contravariant WarningFlagSpec where
  contramap f WarningFlagSpec {..} = WarningFlagSpec { wfsName, wfsHidden, wfsFilter = wfsFilter . f }

instance Contravariant WarningFlag where
  contramap f WarningFlag {..} = WarningFlag { wfName, wfStatus, wfFilter = wfFilter . f }

instance Contravariant WarningFlagParser where
  contramap f WarningFlagParser {..} =
    WarningFlagParser
      { wfpDefault = wfpDefault . f
      , wfpFlagParsers = fmap (contramap f) wfpFlagParsers
      , wfpSuggestFlag = wfpSuggestFlag . f
      }

instance Contravariant WarningFlags where
  contramap f WarningFlags {..} =
    WarningFlags
      { wfDefault = wfDefault . f
      , wfFlags = fmap (contramap f) wfFlags
      }

parseWarningFlag
  :: WarningFlagParser err
  -> String -> Either String (WarningFlag err)
parseWarningFlag parser@WarningFlagParser { wfpFlagParsers } = \case
  ('e':'r':'r':'o':'r':'=':name) -> parseNameE name AsError
  ('n':'o':'-':'e':'r':'r':'o':'r':'=':name) -> parseNameE name AsWarning
  ('n':'o':'-':name) -> parseNameE name Hidden
  ('w':'a':'r':'n':'=':name) -> parseNameE name AsWarning
  name -> parseNameE name AsWarning
  where
  parseNameE name status = case L.find (\spec -> wfsName spec == name) wfpFlagParsers of
    Nothing -> Left $ "Warning flag is not valid - warning flags must be of the form `-Werror=<name>`, `-Wno-<name>`, or `-W<name>`. Available names are: " <> namesAsList parser
    Just WarningFlagSpec{..} -> Right (WarningFlag { wfName = wfsName, wfFilter = wfsFilter, wfStatus = status })

namesAsList :: WarningFlagParser err -> String
namesAsList WarningFlagParser {wfpFlagParsers} = L.intercalate ", " (map wfsName (filter (not . wfsHidden) wfpFlagParsers))

getWarningStatus :: WarningFlags err -> err -> WarningFlagStatus
getWarningStatus WarningFlags { wfDefault, wfFlags } err =
  case filter (\flag -> wfFilter flag err) wfFlags of
    [] -> wfDefault err
    xs -> wfStatus (last xs)

mkWarningFlags :: WarningFlagParser err -> [WarningFlag err] -> WarningFlags err
mkWarningFlags parser flags = WarningFlags
  { wfFlags = flags
  , wfDefault = wfpDefault parser
  }

addWarningFlags :: [WarningFlag err] -> WarningFlags err -> WarningFlags err
addWarningFlags newFlags flags = flags { wfFlags = wfFlags flags ++ newFlags }

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

type WarningFlagParsers xs = WarningFlagParser (Sum xs)

combineParsers :: Combine WarningFlagParser xs => Product (CombineType WarningFlagParser xs) -> WarningFlagParsers xs
combineParsers = combine

instance CombineF WarningFlagParser where
  combineF (left, rest) =
    WarningFlagParser
      { wfpDefault = either (wfpDefault left) (wfpDefault rest)
      , wfpFlagParsers =
          fmap (modifyWfsFilter (flip either (const False))) (wfpFlagParsers left) ++
          fmap (modifyWfsFilter (either (const False))) (wfpFlagParsers rest)
      , wfpSuggestFlag = either (wfpSuggestFlag left) (wfpSuggestFlag rest)
      }
  combineZ = mkWarningFlagParser (const (error "voidParser: should not be able to get a flag with SumR")) []

splitWarningFlags :: Split WarningFlags xs => WarningFlags (Sum xs) -> Product (SplitType WarningFlags xs)
splitWarningFlags = split

instance SplitF WarningFlags where
  splitF a = (contramap Left a, contramap Right a)
