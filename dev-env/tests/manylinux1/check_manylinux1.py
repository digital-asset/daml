# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Check that we can dlopen the manylinux1 shared libraries.

PEP-513 defines the manylinux1 platform tag, which demands that
certain shared libraries are available on the host system. This script
checks for the presence of these libraries and reports any
discrepancies found.

See: https://www.python.org/dev/peps/pep-0513/.
"""

import ctypes

so_names = [
    ('libpanelw.so.5', 'new_panel'),
    ('libncursesw.so.5', 'ungetmouse'),
    ('libgcc_s.so.1', '__deregister_frame'),
    ('libstdc++.so.6', '_ZNSt13bad_exceptionD0Ev'),
    ('libm.so.6', 'tanh'),
    ('libdl.so.2', 'dlopen'),
    ('librt.so.1', 'mq_open'),
    ('libcrypt.so.1', 'crypt'),
    ('libc.so.6', 'sleep'),
    ('libnsl.so.1', 'nis_checkpoint'),
    ('libutil.so.1', 'login_tty'),
    ('libpthread.so.0', 'sem_open'),
    ('libresolv.so.2', 'ns_datetosecs'),
    ('libX11.so.6', 'XWhitePixel'),
    ('libXext.so.6', 'XSyncAwait'),
    ('libXrender.so.1', 'XRenderSetSubpixelOrder'),
    ('libICE.so.6', 'IceWriteAuthFileEntry'),
    ('libSM.so.6', 'SmsRegisterClientReply'),
    ('libGL.so.1', 'glXSwapBuffers'),
    ('libgobject-2.0.so.0', 'g_value_transform'),
    ('libgthread-2.0.so.0', 'g_thread_init'),
    ('libglib-2.0.so.0', 'g_utf8_validate'),
]


def lib_path(lib):
    """Return the full path of a shared library."""
    # Tease out the full path of the library by accessing an undefined
    # symbol and parsing the exception.
    try:
        lib.undefined_symbol_to_trigger_exception
    except AttributeError as exc:
        path = exc.args[0].partition(': undefined symbol')[0]
        return path


def main(require_nix_store_paths=True):
    """Try loading all libraries and report errors along the way."""
    errors = 0

    print('Testing %d shared libraries, paths outside /nix/store are %s' %
          (len(so_names),
           'forbidden' if require_nix_store_paths else 'allowed'))

    for so_name, symbol in so_names:
        print()
        print('- Loading', so_name)
        try:
            lib = ctypes.cdll.LoadLibrary(so_name)
            path = lib_path(lib)
            is_nix_store_path = path.startswith('/nix/store/')
            if require_nix_store_paths and not is_nix_store_path:
                print('  *** error:', path, 'is outside the Nix store ***')
                errors += 1
            else:
                print('  path:', path)
            print('  symbol:', getattr(lib, symbol), '-', symbol)
        except (OSError, AttributeError) as exc:
            print('  *** error:', exc, '***')
            errors += 1

    print()
    if errors:
        print('!!! Found %d errors !!!' % errors)
    else:
        print('All libraries loaded successfully!')
    return errors


if __name__ == '__main__':
    import sys
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--allow-system-libs', action='store_true',
                        help='Allow loading libraries from locations outside '
                        '/nix/store.')
    args = parser.parse_args()
    sys.exit(main(require_nix_store_paths=not args.allow_system_libs))
