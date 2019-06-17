-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
--

module DA.Daml.GHC.Compiler.Upgrade
    ( generateUpgradeModule
    ) where

import Data.Maybe
import "ghc-lib" GHC
import "ghc-lib-parser" Name
import "ghc-lib-parser" RdrName

-- | Generate non-consuming choices to upgrade all templates defined in the module.
generateUpgradeModule ::
       (String, ParsedSource) -> (String, ParsedSource) -> String
generateUpgradeModule (pkgA, L _lA srcA) (pkgB, L _lB srcB) = unlines $ header ++ concatMap upgradeTemplate names
  where
    names = [n | n <- allTemplateDataNames srcA, n `elem` allTemplateDataNames srcB]
    modName = moduleNameString $ unLoc $ fromMaybe (error "missing module name") $ hsmodName srcA
    allTemplateDataNames :: HsModule GhcPs -> [String]
    allTemplateDataNames src =
        [ occNameString $ occName $ unLoc tcdLName
        | L _ (TyClD NoExt DataDecl {..}) <- hsmodDecls src
        , unLoc tcdLName `elem` templateDataNames srcA
        ]
    templateDataNames :: HsModule GhcPs -> [RdrName]
    templateDataNames src =
        [ ty2
        | L _ (InstD NoExt ClsInstD {..}) <- hsmodDecls src
        , ClsInstDecl {..} <- [cid_inst]
        , HsAppTy _ ty1 (L _ (HsTyVar _ _ (L _ ty2))) <-
              [unLoc $ hsImplicitBody cid_poly_ty]
        , HsTyVar _ _ (L _ n) <- [unLoc ty1]
        , n ==
              mkRdrQual
                  (mkModuleName "DA.Internal.Desugar")
                  (mkOccName tcClsName "Template")
        ]

    header =
        [ "daml 1.2"
        , "module " <> modName <> " where"
        , "import \"" <> pkgA <> "\" " <> modName <> " qualified as A"
        , "import \"" <> pkgB <> "\" " <> modName <> " qualified as B"
        , "import DA.Next.Set"
        , "import DA.Upgrade"
        ]
    upgradeTemplate n =
      [ "template " <> n <> "Upgrade"
      , "    with"
      , "        op : Party"
      , "    where"
      , "        signatory op"
      , "        nonconsuming choice Upgrade: ContractId B." <> n
      , "            with"
      , "                inC : ContractId A." <> n
      , "                sigs : [Party]"
      , "            controller sigs"
      , "                do"
      , "                    d <- fetch inC"
      , "                    assert $ fromList sigs == fromList (signatory d)"
      , "                    create $ conv d"
      ]
