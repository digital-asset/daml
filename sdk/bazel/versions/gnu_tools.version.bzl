_GNU_MIRRORS = [
    "https://mirrors.kernel.org/gnu",
    "https://ftp.fau.de/gnu",
    "https://ftp.gnu.org/gnu",
]

def _gnu_urls(path):
    return ["{}/{}".format(mirror, path) for mirror in _GNU_MIRRORS]

# -- autoconf --
# https://ftp.gnu.org/gnu/autoconf/
AUTOCONF_VERSION = "2.72"
AUTOCONF_URLS = _gnu_urls("autoconf/autoconf-{}.tar.gz".format(AUTOCONF_VERSION))
AUTOCONF_SHA256 = "afb181a76e1ee72832f6581c0eddf8df032b83e2e0239ef79ebedc4467d92d6e"

# -- automake --
# https://ftp.gnu.org/gnu/automake/
AUTOMAKE_VERSION = "1.16.5"
AUTOMAKE_URLS = _gnu_urls("automake/automake-{}.tar.gz".format(AUTOMAKE_VERSION))
AUTOMAKE_SHA256 = "07bd24ad08a64bc17250ce09ec56e921d6343903943e99ccf63bbf0705e34605"

# -- m4 --
# https://ftp.gnu.org/gnu/m4/
M4_VERSION = "1.4.19"
M4_URLS = _gnu_urls("m4/m4-{}.tar.gz".format(M4_VERSION))
M4_SHA256 = "3be4a26d825ffdfda52a56fc43246456989a3630093cced3fbddf4771ee58a70"

# -- make --
# https://ftp.gnu.org/gnu/make/
MAKE_VERSION = "4.4.1"
MAKE_URLS = _gnu_urls("make/make-{}.tar.gz".format(MAKE_VERSION))
MAKE_SHA256 = "dd16fb1d67bfab79a72f5e8390735c49e3e8e70b4945a15ab1f81ddb78658fb3"

# -- gmp --
# https://gmplib.org/download/gmp/
GMP_VERSION = "6.3.0"
GMP_URLS = _gnu_urls("gmp/gmp-{}.tar.xz".format(GMP_VERSION)) + [
    "https://gmplib.org/download/gmp/gmp-{}.tar.xz".format(GMP_VERSION),
]
GMP_SHA256 = "a3c2b80201b89e68616f4ad30bc66aee4927c3ce50e33929ca819d5c43538898"

# -- zlib --
# https://zlib.net/fossils/
ZLIB_VERSION = "1.3.1"
ZLIB_URLS = [
    "https://github.com/madler/zlib/releases/download/v{v}/zlib-{v}.tar.gz".format(v = ZLIB_VERSION),
    "https://zlib.net/fossils/zlib-{}.tar.gz".format(ZLIB_VERSION),
]
ZLIB_SHA256 = "9a93b2b7dfdac77ceba5a558a580e74667dd6fede4585b91eefb60f03b72df23"

# -- ncurses --
# https://ftp.gnu.org/gnu/ncurses/
NCURSES_LINUX_VERSION = "6.4"
NCURSES_LINUX_URLS = _gnu_urls("ncurses/ncurses-{}.tar.gz".format(NCURSES_LINUX_VERSION))
NCURSES_LINUX_SHA256 = "6931283d9ac87c5073f30b6290c4c75f21632bb4fc3603ac8100812bed248159"

# -- numactl --
# https://github.com/numactl/numactl/releases
NUMACTL_VERSION = "2.0.19"
NUMACTL_SHA256 = "f2672a0381cb59196e9c246bf8bcc43d5568bc457700a697f1a1df762b9af884"

# -- bzip2 --
# https://sourceware.org/pub/bzip2/
BZIP2_VERSION = "1.0.8"
BZIP2_URLS = [
    "https://mirror.bazel.build/sourceware.org/pub/bzip2/bzip2-{}.tar.gz".format(BZIP2_VERSION),
    "https://fossies.org/linux/misc/bzip2-{}.tar.gz".format(BZIP2_VERSION),
    "https://sourceware.org/pub/bzip2/bzip2-{}.tar.gz".format(BZIP2_VERSION),
]
BZIP2_SHA256 = "ab5a03176ee106d3f0fa90e381da478ddae405918153cca248e682cd0c4a2269"
