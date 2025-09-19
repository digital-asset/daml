// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto.rlp

import java.math.BigInteger

/**
 * Recursive Length Prefix (RLP) encoding.
 *
 * The purpose of RLP is to encode arbitrarily nested arrays of binary data, and
 * RLP is the main encoding method used to serialize objects in Ethereum. The
 * only purpose of RLP is to encode structure; encoding specific atomic data
 * types (eg. strings, integers, floats) is left up to higher-order protocols; in
 * Ethereum the standard is that integers are represented in big endian binary
 * form. If one wishes to use RLP to encode a dictionary, the two suggested
 * canonical forms are to either use [[k1,v1],[k2,v2]...] with keys in
 * lexicographic order or to use the higher-level Patricia Tree encoding as
 * Ethereum does.
 *
 * The RLP encoding function takes in an item. An item is defined as follows:
 *
 * - A string (ie. byte array) is an item - A list of items is an item
 *
 * For example, an empty string is an item, as is the string containing the word
 * "cat", a list containing any number of strings, as well as more complex data
 * structures like ["cat",["puppy","cow"],"horse",[[]],"pig",[""],"sheep"]. Note
 * that in the context of the rest of this article, "string" will be used as a
 * synonym for "a certain number of bytes of binary data"; no special encodings
 * are used and no knowledge about the content of the strings is implied.
 *
 * See: https://github.com/ethereum/wiki/wiki/%5BEnglish%5D-RLP
 *
 * @author Roman Mandeleil
 * @since 01.04.2014
 */
object RLP {
  private val MAX_DEPTH: Int = 16
  // Allow for content up to size of 2^64 bytes
  private val MAX_ITEM_LENGTH: Double = Math.pow(256, 8)
  /*
   * Reason for threshold according to Vitalik Buterin:
   * - 56 bytes maximizes the benefit of both options
   * - if we went with 60 then we would have only had 4 slots for long strings
   * so RLP would not have been able to store objects above 4gb
   * - if we went with 48 then RLP would be fine for 2^128 space, but that's way too much
   * - so 56 and 2^64 space seems like the right place to put the cutoff
   * - also, that's where Bitcoin's varint does the cutof
   */
  private val SIZE_THRESHOLD: Int = 56

  /** RLP encoding rules are defined as follows: */

  /*
   * For a single byte whose value is in the [0x00, 0x7f] range, that byte is
   * its own RLP encoding.
   */

  /**
   * [0x80]
   * If a string is 0-55 bytes long, the RLP encoding consists of a single
   * byte with value 0x80 plus the length of the string followed by the
   * string. The range of the first byte is thus [0x80, 0xb7].
   */
  private val OFFSET_SHORT_ITEM: Int = 0x80

  /**
   * [0xb7]
   * If a string is more than 55 bytes long, the RLP encoding consists of a
   * single byte with value 0xb7 plus the length of the length of the string
   * in binary form, followed by the length of the string, followed by the
   * string. For example, a length-1024 string would be encoded as
   * \xb9\x04\x00 followed by the string. The range of the first byte is thus
   * [0xb8, 0xbf].
   */
  private val OFFSET_LONG_ITEM: Inbt = 0xb7

  /**
   * [0xc0]
   * If the total payload of a list (i.e. the combined length of all its
   * items) is 0-55 bytes long, the RLP encoding consists of a single byte
   * with value 0xc0 plus the length of the list followed by the concatenation
   * of the RLP encodings of the items. The range of the first byte is thus
   * [0xc0, 0xf7].
   */
  private val OFFSET_SHORT_LIST: Int = 0xc0

  /**
   * [0xf7]
   * If the total payload of a list is more than 55 bytes long, the RLP
   * encoding consists of a single byte with value 0xf7 plus the length of the
   * length of the list in binary form, followed by the length of the list,
   * followed by the concatenation of the RLP encodings of the items. The
   * range of the first byte is thus [0xf8, 0xff].
   */
  private val OFFSET_LONG_LIST: Int = 0xf7

  /* ******************************************************
   *                      DECODING                        *
   * ******************************************************/

  private def decodeOneByteItem(data: Array[Byte], index: Int): Byte = {
    // null item
    if ((data(index) & 0xFF) eq OFFSET_SHORT_ITEM) return (data(index) - OFFSET_SHORT_ITEM).asInstanceOf[Byte]
    // single byte item
    if ((data(index) & 0xFF) < OFFSET_SHORT_ITEM) return data(index)
    // single byte item
    if ((data(index) & 0xFF) eq OFFSET_SHORT_ITEM + 1) return data(index + 1)
  }

  def decodeInt(data: Array[Byte], index: Int): Int = {
    var value = 0
    // NOTE: From RLP doc:
    // Ethereum integers must be represented in big endian binary form
    // with no leading zeroes (thus making the integer value zero be
    // equivalent to the empty byte array)
    if (data(index) == 0x00) throw new RuntimeException("not a number")
    else if ((data(index) & 0xFF) < OFFSET_SHORT_ITEM) return data(index)
    else if ((data(index) & 0xFF) <= OFFSET_SHORT_ITEM + Integer.BYTES) {
      val length = (data(index) - OFFSET_SHORT_ITEM).asInstanceOf[Byte]
      var pow = (length - 1).toByte
      for (i <- 1 to length) {
        // << (8 * pow) == bit shift to 0 (*1), 8 (*256) , 16 (*65..)..
        value += (data(index + i) & 0xFF) << (8 * pow)
        pow -= 1
      }
    }
    else {
      // If there are more than 4 bytes, it is not going
      // to decode properly into an int.
      throw new RuntimeException("wrong decode attempt")
    }
    value
  }

  def decodeShort(data: Array[Byte], index: Int): Short = {
    var value = 0
    if (data(index) == 0x00) throw new RuntimeException("not a number")
    else if ((data(index) & 0xFF) < OFFSET_SHORT_ITEM) return data(index)
    else if ((data(index) & 0xFF) <= OFFSET_SHORT_ITEM + Short.BYTES) {
      val length = (data(index) - OFFSET_SHORT_ITEM).asInstanceOf[Byte]
      var pow = (length - 1).toByte
      for (i <- 1 to length) {
        // << (8 * pow) == bit shift to 0 (*1), 8 (*256) , 16 (*65..)
        value += (data(index + i) & 0xFF) << (8 * pow)
        pow -= 1
      }
    }
    else {
      // If there are more than 2 bytes, it is not going
      // to decode properly into a short.
      throw new RuntimeException("wrong decode attempt")
    }
    value
  }

  def decodeLong(data: Array[Byte], index: Int): Long = {
    var value = 0
    if (data(index) == 0x00) throw new RuntimeException("not a number")
    else if ((data(index) & 0xFF) < OFFSET_SHORT_ITEM) return data(index)
    else if ((data(index) & 0xFF) <= OFFSET_SHORT_ITEM + Long.BYTES) {
      val length = (data(index) - OFFSET_SHORT_ITEM).asInstanceOf[Byte]
      var pow = (length - 1).toByte
      for (i <- 1 to length) {
        // << (8 * pow) == bit shift to 0 (*1), 8 (*256) , 16 (*65..)..
        value += (data(index + i) & 0xFF).toLong << (8 * pow)
        pow -= 1
      }
    }
    else {
      // If there are more than 8 bytes, it is not going
      // to decode properly into a long.
      throw new RuntimeException("wrong decode attempt")
    }
    value
  }

  private def decodeStringItem(data: Array[Byte], index: Int) = {
    val valueBytes = decodeItemBytes(data, index)
    if (valueBytes.length == 0) {
      // shortcut
      ""
    }
    else new String(valueBytes)
  }

  def decodeBigInteger(data: Array[Byte], index: Int): Nothing = {
    val valueBytes = decodeItemBytes(data, index)
    if (valueBytes.length == 0) {
      // shortcut
      BigInteger.ZERO
    }
    else {
      val res = new Nothing(1, valueBytes)
      res
    }
  }

  private def decodeByteArray(data: Array[Byte], index: Int) = decodeItemBytes(data, index)

  private def nextItemLength(data: Array[Byte], index: Int): Int = {
    if (index >= data.length) return -1
    // [0xf8, 0xff]
    if ((data(index) & 0xFF) > OFFSET_LONG_LIST) {
      val lengthOfLength = (data(index) - OFFSET_LONG_LIST).asInstanceOf[Byte]
      return calcLength(lengthOfLength, data, index)
    }
    // [0xc0, 0xf7]
    if ((data(index) & 0xFF) >= OFFSET_SHORT_LIST && (data(index) & 0xFF) <= OFFSET_LONG_LIST) return ((data(index) & 0xFF) - OFFSET_SHORT_LIST).asInstanceOf[Byte]
    // [0xb8, 0xbf]
    if ((data(index) & 0xFF) > OFFSET_LONG_ITEM && (data(index) & 0xFF) < OFFSET_SHORT_LIST) {
      val lengthOfLength = (data(index) - OFFSET_LONG_ITEM).asInstanceOf[Byte]
      return calcLength(lengthOfLength, data, index)
    }
    // [0x81, 0xb7]
    if ((data(index) & 0xFF) > OFFSET_SHORT_ITEM && (data(index) & 0xFF) <= OFFSET_LONG_ITEM) return ((data(index) & 0xFF) - OFFSET_SHORT_ITEM).asInstanceOf[Byte]
    // [0x00, 0x80]
    if ((data(index) & 0xFF) <= OFFSET_SHORT_ITEM) return 1
    -1
  }

  def decodeIP4Bytes(data: Array[Byte], index: Int): Array[Byte] = {
    var offset = 1
    val result = new Array[Byte](4)
    for (i <- 0 until 4) {
      result(i) = decodeOneByteItem(data, index + offset)
      if ((data(index + offset) & 0xFF) > OFFSET_SHORT_ITEM) offset += 2
      else offset += 1
    }
    // return IP address
    result
  }

  def getFirstListElement(payload: Array[Byte], pos: Int): Int = {
    if (pos >= payload.length) return -1
    // [0xf8, 0xff]
    if ((payload(pos) & 0xFF) > OFFSET_LONG_LIST) {
      val lengthOfLength = (payload(pos) - OFFSET_LONG_LIST).asInstanceOf[Byte]
      return pos + lengthOfLength + 1
    }
    // [0xc0, 0xf7]
    if ((payload(pos) & 0xFF) >= OFFSET_SHORT_LIST && (payload(pos) & 0xFF) <= OFFSET_LONG_LIST) return pos + 1
    // [0xb8, 0xbf]
    if ((payload(pos) & 0xFF) > OFFSET_LONG_ITEM && (payload(pos) & 0xFF) < OFFSET_SHORT_LIST) {
      val lengthOfLength = (payload(pos) - OFFSET_LONG_ITEM).asInstanceOf[Byte]
      return pos + lengthOfLength + 1
    }
    -1
  }

  def getNextElementIndex(payload: Array[Byte], pos: Int): Int = {
    if (pos >= payload.length) return -1
    // [0xf8, 0xff]
    if ((payload(pos) & 0xFF) > OFFSET_LONG_LIST) {
      val lengthOfLength = (payload(pos) - OFFSET_LONG_LIST).asInstanceOf[Byte]
      val length = calcLength(lengthOfLength, payload, pos)
      return pos + lengthOfLength + length + 1
    }
    // [0xc0, 0xf7]
    if ((payload(pos) & 0xFF) >= OFFSET_SHORT_LIST && (payload(pos) & 0xFF) <= OFFSET_LONG_LIST) {
      val length = ((payload(pos) & 0xFF) - OFFSET_SHORT_LIST).asInstanceOf[Byte]
      return pos + 1 + length
    }
    // [0xb8, 0xbf]
    if ((payload(pos) & 0xFF) > OFFSET_LONG_ITEM && (payload(pos) & 0xFF) < OFFSET_SHORT_LIST) {
      val lengthOfLength = (payload(pos) - OFFSET_LONG_ITEM).asInstanceOf[Byte]
      val length = calcLength(lengthOfLength, payload, pos)
      return pos + lengthOfLength + length + 1
    }
    // [0x81, 0xb7]
    if ((payload(pos) & 0xFF) > OFFSET_SHORT_ITEM && (payload(pos) & 0xFF) <= OFFSET_LONG_ITEM) {
      val length = ((payload(pos) & 0xFF) - OFFSET_SHORT_ITEM).asInstanceOf[Byte]
      return pos + 1 + length
    }
    // []0x80]
    if ((payload(pos) & 0xFF) == OFFSET_SHORT_ITEM) return pos + 1
    // [0x00, 0x7f]
    if ((payload(pos) & 0xFF) < OFFSET_SHORT_ITEM) return pos + 1
    -1
  }

  /**
   * Parse length of long item or list.
   * RLP supports lengths with up to 8 bytes long,
   * but due to java limitation it returns either encoded length
   * or {@link Integer# MAX_VALUE} in case if encoded length is greater
   *
   * @param lengthOfLength length of length in bytes
   * @param msgData        message
   * @param pos            position to parse from
   * @return calculated length
   */
  private def calcLength(lengthOfLength: Int, msgData: Array[Byte], pos: Int): Int = {
    var pow = (lengthOfLength - 1).toByte
    var length = 0
    for (i <- 1 to lengthOfLength) {
      val bt = msgData(pos + i) & 0xFF
      val shift = 8 * pow
      // no leading zeros are acceptable
      if (bt == 0 && length == 0) throw new RuntimeException("RLP length contains leading zeros")
      // return MAX_VALUE if index of highest bit is more than 31
      if (32 - Integer.numberOfLeadingZeros(bt) + shift > 31) return Integer.MAX_VALUE
      length += bt << shift
      pow -= 1
    }
    // check that length is in payload bounds
    verifyLength(length, msgData.length - pos - lengthOfLength)
    length
  }

  def getCommandCode(data: Array[Byte]): Byte = {
    val index = getFirstListElement(data, 0)
    val command = data(index)
    if ((command & 0xFF) == OFFSET_SHORT_ITEM) 0
    else command
  }

  /**
   * Parse wire byte[] message into RLP elements
   *
   * @param msgData    - raw RLP data
   * @param depthLimit - limits depth of decoding
   * @return rlpList
   *         - outcome of recursive RLP structure
   */
  def decode2(msgData: Array[Byte], depthLimit: Int): RLPList = {
    if (depthLimit < 1) throw new RuntimeException("Depth limit should be 1 or higher")
    val rlpList = new RLPList
    fullTraverse(msgData, 0, 0, msgData.length, rlpList, depthLimit)
    rlpList
  }

  /**
   * Parse wire byte[] message into RLP elements
   *
   * @param msgData - raw RLP data
   * @return rlpList
   *         - outcome of recursive RLP structure
   */
  def decode2(msgData: Array[Byte]): RLPList = {
    val rlpList = new RLPList
    fullTraverse(msgData, 0, 0, msgData.length, rlpList, Integer.MAX_VALUE)
    rlpList
  }

  /**
   * Decodes RLP with list without going deep after 1st level list
   * (actually, 2nd as 1st level is wrap only)
   *
   * So assuming you've packed several byte[] with {@link # encodeList ( byte [ ]...)},
   * you could use this method to unpack them,
   * getting RLPList with RLPItem's holding byte[] inside
   *
   * @param msgData rlp data
   * @return list of RLPItems
   */
  def unwrapList(msgData: Array[Byte]): RLPList = decode2(msgData, 2).get(0).asInstanceOf[RLPList]

  def decode2OneItem(msgData: Array[Byte], startPos: Int): RLPList = {
    val rlpList = new RLPList
    fullTraverse(msgData, 0, startPos, startPos + 1, rlpList, Integer.MAX_VALUE)
    rlpList.get(0)
  }

  /**
   * Get exactly one message payload
   */
  def fullTraverse(msgData: Array[Byte], level: Int, startPos: Int, endPos: Int, rlpList: RLPList, depth: Int): Unit = {
    if (level > MAX_DEPTH) throw new RuntimeException(String.format("Error: Traversing over max RLP depth (%s)", MAX_DEPTH))
    try {
      if (msgData == null || msgData.length == 0) return
      var pos = startPos
      while (pos < endPos) {
        logger.debug("fullTraverse: level: " + level + " startPos: " + pos + " endPos: " + endPos)
        // It's a list with a payload more than 55 bytes
        // data[0] - 0xF7 = how many next bytes allocated
        // for the length of the list
        if ((msgData(pos) & 0xFF) > OFFSET_LONG_LIST) {
          val lengthOfLength = (msgData(pos) - OFFSET_LONG_LIST).asInstanceOf[Byte]
          val length = calcLength(lengthOfLength, msgData, pos)
          if (length < SIZE_THRESHOLD) throw new RuntimeException("Short list has been encoded as long list")
          // check that length is in payload bounds
          verifyLength(length, msgData.length - pos - lengthOfLength)
          val rlpData = new Array[Byte](lengthOfLength + length + 1)
          System.arraycopy(msgData, pos, rlpData, 0, lengthOfLength + length + 1)
          if (level + 1 < depth) {
            val newLevelList = new RLPList
            newLevelList.setRLPData(rlpData)
            fullTraverse(msgData, level + 1, pos + lengthOfLength + 1, pos + lengthOfLength + length + 1, newLevelList, depth)
            rlpList.add(newLevelList)
          }
          else rlpList.add(new RLPList(rlpData))
          pos += lengthOfLength + length + 1
          continue //todo: continue is not supported

        }
        // It's a list with a payload less than 55 bytes
        if ((msgData(pos) & 0xFF) >= OFFSET_SHORT_LIST && (msgData(pos) & 0xFF) <= OFFSET_LONG_LIST) {
          val length = ((msgData(pos) & 0xFF) - OFFSET_SHORT_LIST).asInstanceOf[Byte]
          val rlpData = new Array[Byte](length + 1)
          System.arraycopy(msgData, pos, rlpData, 0, length + 1)
          if (level + 1 < depth) {
            val newLevelList = new RLPList
            newLevelList.setRLPData(rlpData)
            if (length > 0) fullTraverse(msgData, level + 1, pos + 1, pos + length + 1, newLevelList, depth)
            rlpList.add(newLevelList)
          }
          else rlpList.add(new RLPList(rlpData))
          pos += 1 + length
          continue //todo: continue is not supported

        }
        // It's an item with a payload more than 55 bytes
        // data[0] - 0xB7 = how much next bytes allocated for
        // the length of the string
        if ((msgData(pos) & 0xFF) > OFFSET_LONG_ITEM && (msgData(pos) & 0xFF) < OFFSET_SHORT_LIST) {
          val lengthOfLength = (msgData(pos) - OFFSET_LONG_ITEM).asInstanceOf[Byte]
          val length = calcLength(lengthOfLength, msgData, pos)
          if (length < SIZE_THRESHOLD) throw new RuntimeException("Short item has been encoded as long item")
          // check that length is in payload bounds
          verifyLength(length, msgData.length - pos - lengthOfLength)
          // now we can parse an item for data[1]..data[length]
          val item = new Array[Byte](length)
          System.arraycopy(msgData, pos + lengthOfLength + 1, item, 0, length)
          val rlpItem = new RLPList(item)
          rlpList.add(rlpItem)
          pos += lengthOfLength + length + 1
          continue //todo: continue is not supported

        }
        // It's an item less than 55 bytes long,
        // data[0] - 0x80 == length of the item
        if ((msgData(pos) & 0xFF) > OFFSET_SHORT_ITEM && (msgData(pos) & 0xFF) <= OFFSET_LONG_ITEM) {
          val length = ((msgData(pos) & 0xFF) - OFFSET_SHORT_ITEM).asInstanceOf[Byte]
          val item = new Array[Byte](length)
          System.arraycopy(msgData, pos + 1, item, 0, length)
          if (length == 1 && (item(0) & 0xFF) < OFFSET_SHORT_ITEM) throw new RuntimeException("Single byte has been encoded as byte string")
          val rlpItem = new RLPList(item)
          rlpList.add(rlpItem)
          pos += 1 + length
          continue //todo: continue is not supported

        }
        // null item
        if ((msgData(pos) & 0xFF) == OFFSET_SHORT_ITEM) {
          val item = ByteUtil.EMPTY_BYTE_ARRAY
          val rlpItem = new RLPList(item)
          rlpList.add(rlpItem)
          pos += 1
          continue //todo: continue is not supported

        }
        // single byte item
        if ((msgData(pos) & 0xFF) < OFFSET_SHORT_ITEM) {
          val item = Array((msgData(pos) & 0xFF).toByte)
          val rlpItem = new RLPList(item)
          rlpList.add(rlpItem)
          pos += 1
        }
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException("RLP wrong encoding (" + Hex.toHexString(msgData, startPos, endPos - startPos) + ")", e)
      case e: OutOfMemoryError =>
        throw new RuntimeException("Invalid RLP (excessive mem allocation while parsing) (" + Hex.toHexString(msgData, startPos, endPos - startPos) + ")", e)
    }
  }

  /**
   * Compares supplied length information with maximum possible
   *
   * @param suppliedLength  Length info from header
   * @param availableLength Length of remaining object
   * @throws RuntimeException if supplied length is bigger than available
   */
  private def verifyLength(suppliedLength: Int, availableLength: Int): Unit = {
    if (suppliedLength > availableLength) throw new RuntimeException(String.format("Length parsed from RLP (%s bytes) is greater " + "than possible size of data (%s bytes)", suppliedLength, availableLength))
  }

  /**
   * Reads any RLP encoded byte-array and returns all objects as byte-array or list of byte-arrays
   *
   * @param data RLP encoded byte-array
   * @param pos  position in the array to start reading
   * @return DecodeResult encapsulates the decoded items as a single Object and the final read position
   */
  def decode(data: Array[Byte], pos: Int): DecodeResult = {
    if (data == null || data.length < 1) return null
    val prefix = data(pos) & 0xFF
    if (prefix == OFFSET_SHORT_ITEM) { // 0x80
      new DecodeResult(pos + 1, "") // means no length or 0

    }
    else if (prefix < OFFSET_SHORT_ITEM) { // [0x00, 0x7f]
      new DecodeResult(pos + 1, Array[Byte](data(pos))) // byte is its own RLP encoding

    }
    else if (prefix <= OFFSET_LONG_ITEM) { // [0x81, 0xb7]
      val len = prefix - OFFSET_SHORT_ITEM // length of the encoded bytes

      new DecodeResult(pos + 1 + len, copyOfRange(data, pos + 1, pos + 1 + len))
    }
    else if (prefix < OFFSET_SHORT_LIST) { // [0xb8, 0xbf]
      val lenlen = prefix - OFFSET_LONG_ITEM // length of length the encoded bytes

      val lenbytes = byteArrayToInt(copyOfRange(data, pos + 1, pos + 1 + lenlen)) // length of encoded bytes

      // check that length is in payload bounds
      verifyLength(lenbytes, data.length - pos - 1 - lenlen)
      new DecodeResult(pos + 1 + lenlen + lenbytes, copyOfRange(data, pos + 1 + lenlen, pos + 1 + lenlen + lenbytes))
    }
    else if (prefix <= OFFSET_LONG_LIST) { // [0xc0, 0xf7]
      val len = prefix - OFFSET_SHORT_LIST // length of the encoded list

      val prevPos = pos
      pos += 1
      decodeList(data, pos, prevPos, len)
    }
    else if (prefix <= 0xFF) { // [0xf8, 0xff]
      val lenlen = prefix - OFFSET_LONG_LIST // length of length the encoded list

      val lenlist = byteArrayToInt(copyOfRange(data, pos + 1, pos + 1 + lenlen)) // length of encoded bytes

      pos = pos + lenlen + 1 // start at position of first element in list

      val prevPos = lenlist
      decodeList(data, pos, prevPos, lenlist)
    }
    else throw new RuntimeException("Only byte values between 0x00 and 0xFF are supported, but got: " + prefix)
  }

  final class LList(private val rlp: Array[Byte]) {
    final private val offsets = new Array[Int](32)
    final private val lens = new Array[Int](32)
    private var cnt = 0

    def getEncoded: Array[Byte] = {
      val encoded = new Array[Array[Byte]](cnt)
      for (i <- 0 until cnt) {
        encoded(i) = encodeElement(getBytes(i))
      }
      encodeList(encoded)
    }

    def add(off: Int, len: Int, isList: Boolean): Unit = {
      offsets(cnt) = off
      lens(cnt) = if (isList) -(1) - len
      else len
      cnt += 1
    }

    def getBytes(idx: Int): Array[Byte] = {
      var len = lens(idx)
      len = if (len < 0) -(len) - 1
      else len
      val ret = new Array[Byte](len)
      System.arraycopy(rlp, offsets(idx), ret, 0, len)
      ret
    }

    def getList(idx: Int): LList = decodeLazyList(rlp, offsets(idx), -lens(idx) - 1)

    def isList(idx: Int): Boolean = lens(idx) < 0

    def size: Int = cnt
  }

  def decodeLazyList(data: Array[Byte]): LList = decodeLazyList(data, 0, data.length).getList(0)

  def decodeLazyList(data: Array[Byte], pos: Int, length: Int): LList = {
    if (data == null || data.length < 1) return null
    val ret = new LList(data)
    val end = pos + length
    while (pos < end) {
      val prefix = data(pos) & 0xFF
      if (prefix == OFFSET_SHORT_ITEM) { // 0x80
        ret.add(pos, 0, false) // means no length or 0

        pos += 1
      }
      else if (prefix < OFFSET_SHORT_ITEM) { // [0x00, 0x7f]
        ret.add(pos, 1, false) // means no length or 0

        pos += 1
      }
      else if (prefix <= OFFSET_LONG_ITEM) { // [0x81, 0xb7]
        val len = prefix - OFFSET_SHORT_ITEM // length of the encoded bytes

        ret.add(pos + 1, len, false)
        pos += len + 1
      }
      else if (prefix < OFFSET_SHORT_LIST) { // [0xb8, 0xbf]
        val lenlen = prefix - OFFSET_LONG_ITEM // length of length the encoded bytes

        val lenbytes = byteArrayToInt(copyOfRange(data, pos + 1, pos + 1 + lenlen)) // length of encoded bytes

        // check that length is in payload bounds
        verifyLength(lenbytes, data.length - pos - 1 - lenlen)
        ret.add(pos + 1 + lenlen, lenbytes, false)
        pos += 1 + lenlen + lenbytes
      }
      else if (prefix <= OFFSET_LONG_LIST) { // [0xc0, 0xf7]
        val len = prefix - OFFSET_SHORT_LIST // length of the encoded list

        ret.add(pos + 1, len, true)
        pos += 1 + len
      }
      else if (prefix <= 0xFF) { // [0xf8, 0xff]
        val lenlen = prefix - OFFSET_LONG_LIST // length of length the encoded list

        val lenlist = byteArrayToInt(copyOfRange(data, pos + 1, pos + 1 + lenlen)) // length of encoded bytes

        // check that length is in payload bounds
        verifyLength(lenlist, data.length - pos - 1 - lenlen)
        ret.add(pos + 1 + lenlen, lenlist, true)
        pos += 1 + lenlen + lenlist // start at position of first element in list

      }
      else throw new RuntimeException("Only byte values between 0x00 and 0xFF are supported, but got: " + prefix)
    }
    ret
  }

  private def decodeList(data: Array[Byte], pos: Int, prevPos: Int, len: Int) = {
    // check that length is in payload bounds
    verifyLength(len, data.length - pos)
    val slice = new Nothing
    var i = 0
    while (i < len) {
      // Get the next item in the data list and append it
      val result = decode(data, pos)
      slice.add(result.getDecoded)
      // Increment pos by the amount bytes in the previous read
      prevPos = result.getPos
      i += (prevPos - pos)
      pos = prevPos
    }
    new Nothing(pos, slice.toArray)
  }

  /* ******************************************************
*                      ENCODING                        *
* ******************************************************/

  /**
   * Turn Object into its RLP encoded equivalent of a byte-array
   * Support for String, Integer, BigInteger and Lists of any of these types.
   *
   * @param input as object or List of objects
   * @return byte[] RLP encoded
   */
  def encode(input: AnyRef): Array[Byte] = {
    val `val` = new Nothing(input)
    if (`val`.isList) {
      val inputArray = `val`.asList
      if (inputArray.isEmpty) return encodeLength(inputArray.size, OFFSET_SHORT_LIST)
      var output = ByteUtil.EMPTY_BYTE_ARRAY
      for (`object` <- inputArray) {
        output = concatenate(output, encode(`object`))
      }
      val prefix = encodeLength(output.length, OFFSET_SHORT_LIST)
      concatenate(prefix, output)
    }
    else {
      val inputAsBytes = toBytes(input)
      if (inputAsBytes.length == 1 && (inputAsBytes(0) & 0xff) <= 0x80) inputAsBytes
      else {
        val firstByte = encodeLength(inputAsBytes.length, OFFSET_SHORT_ITEM)
        concatenate(firstByte, inputAsBytes)
      }
    }
  }

  /**
   * Integer limitation goes up to 2^31-1 so length can never be bigger than MAX_ITEM_LENGTH
   */
  def encodeLength(length: Int, offset: Int): Array[Byte] = if (length < SIZE_THRESHOLD) {
    val firstByte = (length + offset).toByte
    Array[Byte](firstByte)
  }
  else if (length < MAX_ITEM_LENGTH) {
    var binaryLength: Array[Byte] = null
    if (length > 0xFF) binaryLength = intToBytesNoLeadZeroes(length)
    else binaryLength = Array[Byte](length.toByte)
    val firstByte = (binaryLength.length + offset + SIZE_THRESHOLD - 1).asInstanceOf[Byte]
    concatenate(Array[Byte](firstByte), binaryLength)
  }
  else throw new RuntimeException("Input too long")

  def encodeByte(singleByte: Byte): Array[Byte] = if ((singleByte & 0xFF) == 0) Array[Byte](OFFSET_SHORT_ITEM.asInstanceOf[Byte])
  else if ((singleByte & 0xFF) <= 0x7F) Array[Byte](singleByte)
  else Array[Byte]((OFFSET_SHORT_ITEM + 1).asInstanceOf[Byte], singleByte)

  def encodeShort(singleShort: Short): Array[Byte] = if ((singleShort & 0xFF) == singleShort) encodeByte(singleShort.toByte)
  else Array[Byte]((OFFSET_SHORT_ITEM + 2).asInstanceOf[Byte], (singleShort >> 8 & 0xFF).toByte, (singleShort >> 0 & 0xFF).toByte)

  def encodeInt(singleInt: Int): Array[Byte] = if ((singleInt & 0xFF) == singleInt) encodeByte(singleInt.toByte)
  else if ((singleInt & 0xFFFF) == singleInt) encodeShort(singleInt.toShort)
  else if ((singleInt & 0xFFFFFF) == singleInt) Array[Byte]((OFFSET_SHORT_ITEM + 3).asInstanceOf[Byte], (singleInt >>> 16).toByte, (singleInt >>> 8).toByte, singleInt.toByte)
  else Array[Byte]((OFFSET_SHORT_ITEM + 4).asInstanceOf[Byte], (singleInt >>> 24).toByte, (singleInt >>> 16).toByte, (singleInt >>> 8).toByte, singleInt.toByte)

  def encodeString(srcString: String): Array[Byte] = encodeElement(srcString.getBytes)

  def encodeBigInteger(srcBigInteger: Nothing): Array[Byte] = {
    if (srcBigInteger.compareTo(BigInteger.ZERO) < 0) throw new RuntimeException("negative numbers are not allowed")
    if (srcBigInteger.equals(BigInteger.ZERO)) encodeByte(0.toByte)
    else encodeElement(asUnsignedByteArray(srcBigInteger))
  }

  def encodeElement(srcData: Array[Byte]): Array[Byte] = {
    // [0x80]
    if (isNullOrZeroArray(srcData)) Array[Byte](OFFSET_SHORT_ITEM.asInstanceOf[Byte])
    else if (isSingleZero(srcData)) srcData
    else if (srcData.length == 1 && (srcData(0) & 0xFF) < 0x80) srcData
    else if (srcData.length < SIZE_THRESHOLD) {
      // length = 8X
      val length = (OFFSET_SHORT_ITEM + srcData.length).asInstanceOf[Byte]
      val data = Arrays.copyOf(srcData, srcData.length + 1)
      System.arraycopy(data, 0, data, 1, srcData.length)
      data(0) = length
      data
    }
    else {
      // length of length = BX
      // prefix = [BX, [length]]
      var tmpLength = srcData.length
      var lengthOfLength = 0
      while (tmpLength != 0) {
        lengthOfLength += 1
        tmpLength = tmpLength >> 8
      }
      // set length Of length at first byte
      val data = new Array[Byte](1 + lengthOfLength + srcData.length)
      data(0) = (OFFSET_LONG_ITEM + lengthOfLength).asInstanceOf[Byte]
      // copy length after first byte
      tmpLength = srcData.length
      for (i <- lengthOfLength until 0 by -1) {
        data(i) = (tmpLength & 0xFF).toByte
        tmpLength = tmpLength >> 8
      }
      // at last copy the number bytes after its length
      System.arraycopy(srcData, 0, data, 1 + lengthOfLength, srcData.length)
      data
    }
  }

  def calcElementPrefixSize(srcData: Array[Byte]): Int = if (isNullOrZeroArray(srcData)) 0
  else if (isSingleZero(srcData)) 0
  else if (srcData.length == 1 && (srcData(0) & 0xFF) < 0x80) 0
  else if (srcData.length < SIZE_THRESHOLD) 1
  else {
    // length of length = BX
    // prefix = [BX, [length]]
    var tmpLength = srcData.length
    var byteNum = 0
    while (tmpLength != 0) {
      byteNum += 1
      tmpLength = tmpLength >> 8
    }
    1 + byteNum
  }

  def encodeListHeader(size: Int): Array[Byte] = {
    if (size == 0) return Array[Byte](OFFSET_SHORT_LIST.asInstanceOf[Byte])
    val totalLength = size
    var header: Array[Byte] = null
    if (totalLength < SIZE_THRESHOLD) {
      header = new Array[Byte](1)
      header(0) = (OFFSET_SHORT_LIST + totalLength).asInstanceOf[Byte]
    }
    else {
      // length of length = BX
      // prefix = [BX, [length]]
      var tmpLength = totalLength
      var byteNum = 0
      while (tmpLength != 0) {
        byteNum += 1
        tmpLength = tmpLength >> 8
      }
      tmpLength = totalLength
      val lenBytes = new Array[Byte](byteNum)
      for (i <- 0 until byteNum) {
        lenBytes(byteNum - 1 - i) = ((tmpLength >> (8 * i)) & 0xFF).toByte
      }
      // first byte = F7 + bytes.length
      header = new Array[Byte](1 + lenBytes.length)
      header(0) = (OFFSET_LONG_LIST + byteNum).asInstanceOf[Byte]
      System.arraycopy(lenBytes, 0, header, 1, lenBytes.length)
    }
    header
  }

  def encodeLongElementHeader(length: Int): Array[Byte] = if (length < SIZE_THRESHOLD) if (length == 0) Array[Byte](0x80.toByte)
  else Array[Byte]((0x80 + length).toByte)
  else {
    // length of length = BX
    // prefix = [BX, [length]]
    var tmpLength = length
    var byteNum = 0
    while (tmpLength != 0) {
      byteNum += 1
      tmpLength = tmpLength >> 8
    }
    val lenBytes = new Array[Byte](byteNum)
    for (i <- 0 until byteNum) {
      lenBytes(byteNum - 1 - i) = ((length >> (8 * i)) & 0xFF).toByte
    }
    // first byte = F7 + bytes.length
    val header = new Array[Byte](1 + lenBytes.length)
    header(0) = (OFFSET_LONG_ITEM + byteNum).asInstanceOf[Byte]
    System.arraycopy(lenBytes, 0, header, 1, lenBytes.length)
    header
  }

  def encodeSet(data: Nothing): Array[Byte] = {
    var dataLength = 0
    val encodedElements = new Nothing
    for (element <- data) {
      val encodedElement = RLP.encodeElement(element.getData)
      dataLength += encodedElement.length
      encodedElements.add(encodedElement)
    }
    val listHeader = encodeListHeader(dataLength)
    val output = new Array[Byte](listHeader.length + dataLength)
    System.arraycopy(listHeader, 0, output, 0, listHeader.length)
    var cummStart = listHeader.length
    for (element <- encodedElements) {
      System.arraycopy(element, 0, output, cummStart, element.length)
      cummStart += element.length
    }
    output
  }

  /**
   * A handy shortcut for {@link # encodeElement ( byte [ ] )} + {@link # encodeList ( byte [ ]...)}
   *
   * Encodes each data element and wraps them all into a list.
   */
  def wrapList(data: Array[Byte]*): Array[Byte] = {
    val elements = new Array[Array[Byte]](data.length)
    for (i <- 0 until data.length) {
      elements(i) = encodeElement(data(i))
    }
    encodeList(elements)
  }

  def encodeList(elements: Array[Byte]*): Array[Byte] = {
    if (elements == null) return Array[Byte](OFFSET_SHORT_LIST.asInstanceOf[Byte])
    var totalLength = 0
    for (element1 <- elements) {
      totalLength += element1.length
    }
    var data: Array[Byte] = null
    var copyPos = 0
    if (totalLength < SIZE_THRESHOLD) {
      data = new Array[Byte](1 + totalLength)
      data(0) = (OFFSET_SHORT_LIST + totalLength).asInstanceOf[Byte]
      copyPos = 1
    }
    else {
      // length of length = BX
      // prefix = [BX, [length]]
      var tmpLength = totalLength
      var byteNum = 0
      while (tmpLength != 0) {
        byteNum += 1
        tmpLength = tmpLength >> 8
      }
      tmpLength = totalLength
      val lenBytes = new Array[Byte](byteNum)
      for (i <- 0 until byteNum) {
        lenBytes(byteNum - 1 - i) = ((tmpLength >> (8 * i)) & 0xFF).toByte
      }
      // first byte = F7 + bytes.length
      data = new Array[Byte](1 + lenBytes.length + totalLength)
      data(0) = (OFFSET_LONG_LIST + byteNum).asInstanceOf[Byte]
      System.arraycopy(lenBytes, 0, data, 1, lenBytes.length)
      copyPos = lenBytes.length + 1
    }
    for (element <- elements) {
      System.arraycopy(element, 0, data, copyPos, element.length)
      copyPos += element.length
    }
    data
  }

  /*
   *  Utility function to convert Objects into byte arrays
   */
  private def toBytes(input: AnyRef): Array[Byte] = {
    input match {
      case value: Array[Byte] => return value
      case inputString: String =>
        return inputString.getBytes
      case 0L =>
        return ByteUtil.EMPTY_BYTE_ARRAY
      case inputLong: Long =>
        return asUnsignedByteArray(BigInteger.valueOf(inputLong))
      case inputInt: Integer =>
        return if (inputInt eq 0) ByteUtil.EMPTY_BYTE_ARRAY
        else asUnsignedByteArray(BigInteger.valueOf(inputInt))
      case inputBigInt: BigInteger =>
        return if (inputBigInt.equals(BigInteger.ZERO)) ByteUtil.EMPTY_BYTE_ARRAY
        else asUnsignedByteArray(inputBigInt)
      case value: Value =>
        return toBytes(value.asObj)
      case _ =>
    }
    throw new RuntimeException("Unsupported type: Only accepting String, Integer and BigInteger for now")
  }

  private def decodeItemBytes(data: Array[Byte], index: Int) = {
    val length = calculateItemLength(data, index)
    // [0x80]
    if (length == 0) new Array[Byte](0)
    else if ((data(index) & 0xFF) < OFFSET_SHORT_ITEM) {
      val valueBytes = new Array[Byte](1)
      System.arraycopy(data, index, valueBytes, 0, 1)
      valueBytes
    }
    else if ((data(index) & 0xFF) <= OFFSET_LONG_ITEM) {
      val valueBytes = new Array[Byte](length)
      System.arraycopy(data, index + 1, valueBytes, 0, length)
      valueBytes
    }
    else if ((data(index) & 0xFF) > OFFSET_LONG_ITEM && (data(index) & 0xFF) < OFFSET_SHORT_LIST) {
      val lengthOfLength = (data(index) - OFFSET_LONG_ITEM).asInstanceOf[Byte]
      val valueBytes = new Array[Byte](length)
      System.arraycopy(data, index + 1 + lengthOfLength, valueBytes, 0, length)
      valueBytes
    }
    else throw new RuntimeException("wrong decode attempt")
  }

  private def calculateItemLength(data: Array[Byte], index: Int) = {
    // [0xb8, 0xbf] - 56+ bytes item
    if ((data(index) & 0xFF) > OFFSET_LONG_ITEM && (data(index) & 0xFF) < OFFSET_SHORT_LIST) {
      val lengthOfLength = (data(index) - OFFSET_LONG_ITEM).asInstanceOf[Byte]
      calcLength(lengthOfLength, data, index)
    }
    else if ((data(index) & 0xFF) > OFFSET_SHORT_ITEM && (data(index) & 0xFF) <= OFFSET_LONG_ITEM) (data(index) - OFFSET_SHORT_ITEM).asInstanceOf[Byte]
    else if ((data(index) & 0xFF) == OFFSET_SHORT_ITEM) 0.toByte
    else if ((data(index) & 0xFF) < OFFSET_SHORT_ITEM) 1.toByte
    else throw new RuntimeException("wrong decode attempt")
  }
}
