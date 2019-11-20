-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
--

module DA.Daml.Compiler.Upgrade
    ( generateUpgradeModule
    ) where

-- | Generate non-consuming choices to upgrade all templates defined in the module.
generateUpgradeModule :: [String] -> String -> String -> String -> String
generateUpgradeModule templateNames modName qualA qualB =
    unlines $ header ++ concatMap upgradeTemplates templateNames
  where
    header = header0 ++ header2
      -- If we compile with packages from a single sdk version, the instances modules will not be
      -- there and hence we can not include header1.
    header0 =
        [ "daml 1.2"
        , "module " <> modName <> " where"
        , "import " <> modName <> qualA <> " qualified as A"
        , "import " <> modName <> qualB <> " qualified as B"
        ]
    header2 = [
        "import DA.Upgrade"
        ]

upgradeTemplates :: String -> [String]
upgradeTemplates n =
    [ "template instance " <> n <> "Upgrade = Upgrade A." <> n <> " B." <> n
    , "template instance " <> n <> "Rollback = Rollback A." <> n <> " B." <> n
    , "instance Convertible A." <> n <> " B." <> n <> " where"
    , "    convert A." <> n <> "{..} = B." <> n <> " {..}"
    , "instance Convertible B." <> n <> " A." <> n <> " where"
    , "    convert B." <> n <> "{..} = A." <> n <> " {..}"
    ]
