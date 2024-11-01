module DA.Daml.LF.TypeChecker.Error.WarningFlags where

import qualified Data.List as L

data DamlWarningFlagStatus
  = AsError -- -Werror=<name>
  | AsWarning -- -W<name> or -Wwarn=<name> or -Wno-error=<name>
  | Hidden -- -Wno-<name>

data DamlWarningFlag a
  = RawDamlWarningFlag
    { rfName :: String
    , rfStatus :: DamlWarningFlagStatus
    , rfFilter :: a -> Bool
    }

data DamlWarningFlagParser a = DamlWarningFlagParser
  { dwfpDefault :: a -> DamlWarningFlagStatus
  , dwfpFlagParsers :: [(String, DamlWarningFlagStatus -> DamlWarningFlag a)]
  }

data DamlWarningFlags a = DamlWarningFlags
  { dwfDefault :: a -> DamlWarningFlagStatus
  , dwfFlags :: [DamlWarningFlag a]
  }

combineParsers :: DamlWarningFlagParser a -> DamlWarningFlagParser b -> DamlWarningFlagParser (Either a b)
combineParsers left right =
  DamlWarningFlagParser
    { dwfpDefault = either (dwfpDefault left) (dwfpDefault right)
    , dwfpFlagParsers =
        (fmap . fmap . fmap) (mapFlagFilter (\x -> either x (const False))) (dwfpFlagParsers left) ++
        (fmap . fmap . fmap) (mapFlagFilter (\x -> either (const False) x)) (dwfpFlagParsers right)
    }
  where
  mapFlagFilter :: ((a -> Bool) -> b -> Bool) -> DamlWarningFlag a -> DamlWarningFlag b
  mapFlagFilter f flag = flag { rfFilter = f (rfFilter flag) }

parseRawDamlWarningFlag
  :: DamlWarningFlagParser a
  -> String -> Either String (DamlWarningFlag a)
parseRawDamlWarningFlag DamlWarningFlagParser { dwfpFlagParsers } = \case
  ('e':'r':'r':'o':'r':'=':name) -> parseNameE name AsError
  ('n':'o':'-':'e':'r':'r':'o':'r':'=':name) -> parseNameE name AsWarning
  ('n':'o':'-':name) -> parseNameE name Hidden
  ('w':'a':'r':'n':'=':name) -> parseNameE name AsWarning
  name -> parseNameE name AsWarning
  where
  parseNameE name status = case lookup name dwfpFlagParsers of
    Nothing -> Left $ "Warning flag is not valid - warning flags must be of the form `-Werror=<name>`, `-Wno-<name>`, or `-W<name>`. Available names are: " <> L.intercalate ", " (map fst dwfpFlagParsers)
    Just flag -> Right (flag status)

getWarningStatus :: DamlWarningFlags a -> a -> DamlWarningFlagStatus
getWarningStatus DamlWarningFlags { dwfDefault, dwfFlags } err =
  case filter (\flag -> rfFilter flag err) dwfFlags of
    [] -> dwfDefault err
    xs -> rfStatus (last xs)

noDamlWarningFlags :: DamlWarningFlagParser a -> DamlWarningFlags a
noDamlWarningFlags parser = DamlWarningFlags
  { dwfFlags = []
  , dwfDefault = dwfpDefault parser
  }
