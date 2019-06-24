-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
--

module DA.Daml.GHC.Compiler.Upgrade
    ( generateUpgradeModule
    , generateGenInstancesModule
    ) where

import DA.Daml.GHC.Compiler.Generics
import Data.Maybe
import "ghc-lib" GHC
import "ghc-lib-parser" Name
import "ghc-lib-parser" RdrName
import "ghc-lib-parser" PrelNames
import "ghc-lib-parser" Outputable (showSDocUnsafe, ppr)

-- | Generate a module containing generic instances for data types that don't have them already.
generateGenInstancesModule :: String -> (String, ParsedSource) -> String
generateGenInstancesModule qual (pkg, L _l src) =
    unlines $ header ++ map (showSDocUnsafe . ppr) genericInstances
  where
    modName = moduleNameString $ unLoc $ fromMaybe (error "missing module name") $ hsmodName src
    header =
        [ "{-# LANGUAGE EmptyCase #-}"
        , "daml 1.2"
        , "module " <> modName <> "Instances" <> qual <> " where"
        , "import \"" <> pkg <> "\" " <> modName
        , "import DA.Generics"
        ]

    genericInstances =
        [ generateGenericInstanceFor
            (nameOccName genClassName)
            tcdLName
            pkg
            (fromMaybe (error "Missing module name") $ hsmodName src)
            tcdTyVars
            tcdDataDefn
        | L _ (TyClD _x DataDecl {..}) <- hsmodDecls src
        , not $ hasGenDerivation tcdDataDefn
        ]

    hasGenDerivation :: HsDataDefn GhcPs -> Bool
    hasGenDerivation HsDataDefn {..} =
        or [ name `elem` [nameOccName n | n <- genericClassNames]
            | d <- unLoc dd_derivs
            , (HsIB _ (L _ (HsTyVar _ _ (L _ (Unqual name))))) <-
                  unLoc $ deriv_clause_tys $ unLoc d
            ]
    hasGenDerivation XHsDataDefn{} = False

-- | Generate non-consuming choices to upgrade all templates defined in the module.
generateUpgradeModule ::
       (String, ParsedSource) -> (String, ParsedSource) -> String
generateUpgradeModule (pkgA, L _lA srcA) (pkgB, L _lB srcB) =
    unlines $ header ++ concatMap upgradeTemplate names
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
        , "import " <> modName <> "InstancesA()"
        , "import " <> modName <> "InstancesB()"
        , "import DA.Next.Set"
        , "import DA.Upgrade"
        ]

upgradeTemplate :: String -> [String]
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
