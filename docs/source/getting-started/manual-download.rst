.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Manually Installing the SDK
***************************

If you require a higher level of security, you can instead install the Daml SDK by manually downloading the compressed tarball, verifying its signature, extracting it and manually running the install script.

Note that the Windows installer is already signed (within the binary itself), and that signature is checked by Windows before starting it. Nevertheless, you can still follow the steps below to check its external signature file.

To do that:

1. Go to https://github.com/digital-asset/daml/releases. Confirm your browser sees a valid certificate for the github.com domain.
2. Download the artifact (*Assets* section, after the release notes) for your platform as well as the corresponding signature file. For example, if you are on macOS and want to install the latest release (2.0.0 at the time of writing), you would download the files ``daml-sdk-2.0.0-macos.tar.gz`` and ``daml-sdk-2.0.0-macos.tar.gz.asc``. Note that for Windows you can choose between the tarball (ends in ``.tar.gz``), which follows the same instructions as the Linux and macOS ones (but assumes you have a number of typical Unix tools installed), or the installer, which ends with ``.exe``. Regardless, the steps to verify the signature are the same.
3. To verify the signature, you need to have ``gpg`` installed (see
   https://gnupg.org for more information on that) and the Digital Asset
   Security Public Key imported into your keychain. Once you have ``gpg``
   installed, you can import the key by running::

     gpg --keyserver https://keys.openpgp.org --search 4911A8DFE976ACDFA07130DBE8372C0C1C734C51

   This should come back with a key belonging to ``Digital Asset Holdings, LLC
   <security@digitalasset.com>``, created on 2019-05-16 and expiring on
   023-04-18. If any of those details are different, something is wrong. In
   that case please contact Digital Asset immediately.

   Alternatively, if keyservers do not work for you (we are having a bit of
   trouble getting them to work reliably for us), you can find the full public
   key at the bottom of this page.
4. Once the key is imported, you can ask ``gpg`` to verify that the file you have downloaded has indeed been signed by that key. Continuing with our example of 2.0.0 on macOS, you should have both files in the current directory and run::

     gpg --verify daml-sdk-2.0.0-macos.tar.gz.asc

   and that should give you a result that looks like::

     gpg: assuming signed data in 'daml-sdk-2.0.0-macos.tar.gz'
     gpg: Signature made Wed Aug 12 13:30:49 2020 CEST
     gpg:                using RSA key E8372C0C1C734C51
     gpg: Good signature from "Digital Asset Holdings, LLC <security@digitalasset.com>" [unknown]
     gpg: WARNING: This key is not certified with a trusted signature!
     gpg:          There is no indication that the signature belongs to the owner.
     Primary key fingerprint: 4911 A8DF E976 ACDF A071  30DB E837 2C0C 1C73 4C51

   Note: This warning means you have not told gnupg that you trust this key actually belongs to Digital Asset. The ``[unknown]`` tag next to the key has the same meaning: ``gpg`` relies on a web of trust, and you have not told it how far you trust this key. Nevertheless, at this point you have verified that this is indeed the key that has been used to sign the archive.

5. The next step is to extract the tarball and run the install script (unless you chose the Windows installer, in which case the next step is to double-click it)::

     tar xzf daml-sdk-2.0.0-macos.tar.gz
     cd sdk-2.0.0
     ./install.sh

6. Just like for the more automated install procedure, you may want to add ``~/.daml/bin`` to your ``$PATH``.



To import the public key directly without relying on a keyserver, you can
copy-paste the following Bash command::

    gpg --import < <(cat <<EOF
    -----BEGIN PGP PUBLIC KEY BLOCK-----

    mQENBFzdsasBCADO+ZcfZQATP6ceTh4WfXiL2Z2tetvUZGfTaEs/UfBoJPmQ53bN
    90MxudKhgB2mi8DuifYnHfLCvkxSgzfhj2IogV1S+Fa2x99Y819GausJoYfK9gwc
    8YWKEkM81F15jA5UWJTsssKNxUddr/sxJIHIFfqGRQ0e6YeAcc5bOAogBE8UrmxE
    uGfOt9/MvLpDewjDE+2lQOFi9RZuy7S8RMJLTiq2JWbO5yI50oFKeMQy/AJPmV7y
    qAyYUIeZZxvrYeBWi5JDsZ2HOSJPqV7ttD2MvkyXcJCW/Xf8FcleAoWJU09RwVww
    BhZSDz+9mipwZBHENILMuVyEygG5A+vc/YptABEBAAG0N0RpZ2l0YWwgQXNzZXQg
    SG9sZGluZ3MsIExMQyA8c2VjdXJpdHlAZGlnaXRhbGFzc2V0LmNvbT6JAVQEEwEI
    AD4CGwMFCwkIBwIGFQoJCAsCBBYCAwECHgECF4AWIQRJEajf6Xas36BxMNvoNywM
    HHNMUQUCYHxZ3AUJB2EPMAAKCRDoNywMHHNMUfJpB/9Gj7Kce6qtrXj4f54eLOf1
    RpKYUnBcBWjmrnj8eS9AYLy7C1nkpP4H8OAlDJWxslnY6MjMOYmPNgGzf4/MONxa
    PuFbRdfyblkUfujXikI2GFXwyUDEp9J0WOTC9LmZkRxf92bFxTy9rD+Lx9EeBPdi
    nfyID2TOKH0fY0pawqjjvnLyVb/WfNUogkhLRpDXFWrykCWDaWQmFgDkLU2nYkb+
    YyEfWq4cgF3Sbsa43AToRUpU16rldPwClmtDPS8Ba/SxvcU3l+9ksdcTsIko8BEy
    Bw0K5xkRenEDDwpZvTA2bHLs3iBWW6WC52wyUOLzar+ha/YRgNjb8YBlkYbLbwaN
    uQENBFzdsasBCAC5fr5pqxFm+AWPc7wiBSt7uKNdxiRJYydeoPqgmYZTvc8Um8pI
    6JHtUrNxnx4WWKtj6iSPn5pSUrJbue4NAUsBF5O9LZ0fcQKb5diZLGHKtOZttCaj
    Iryp1Rm961skmPmi3yYaHXq4GC/05Ra/bo3C+ZByv/W0JzntOxA3Pvc3c8Pw5sBm
    63xu7iRrnJBtyFGD+MuAZxbN8dwYX0OcmwuSFGxf/wa+aB8b7Ut9RP76sbDvFaXx
    Ef314k8AwxUvlv+ozdNWmEBxp1wR/Fra9i8EbC0V6EkCcModRhjbaNSPIbgkC0ka
    2cgYp1UDgf9FrKvkuir70dg75qSrPRwvFghrABEBAAGJATwEGAEIACYCGwwWIQRJ
    Eajf6Xas36BxMNvoNywMHHNMUQUCYHxZ3AUJB2EPMQAKCRDoNywMHHNMUYXRB/0b
    Ln55mfnhJUFwaL49Le5I74EoL4vCAya6aDDVx/C7PJlVfr+cXZi9gNJn9RTAjCz3
    4yQeg3AFhqvTK/bEH7RvAfqeUf8TqPjI/qDacSFDhZjdsg3GMDolXp0oubp9mN+Y
    JFowLzulJ7DXFVyICozuWeixcjtKzlePX0GW80kcPzXCNwukcMrwCf45+OzF6YMb
    yA2FyBmjjgAlHKM/oUapVoD2hmO3ptC5CAkfslxrsIUAfoStez9MrGoX1JOCu4qm
    aODLV3Mlty4HhdtO2o+Akh6ay5fnrXQ5r2kGa1ICrfoFFKs7oWpSDbsTsgQKexFC
    rLmmBKjG6RQfWJyVSUc8
    =pVlb
    -----END PGP PUBLIC KEY BLOCK-----
    EOF
    
    )
