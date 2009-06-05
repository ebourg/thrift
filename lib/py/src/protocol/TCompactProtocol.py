from TProtocol import *
from struct import pack, unpack

__all__ = ['TCompactProtocol', 'TCompactProtocolFactory']

CLEAR = 0
WRITE = 1
BOOL_WRITE = 2
VALUE_WRITE = 3
CONTAINER_WRITE = 7
READ = 4
VALUE_READ = 6
CONTAINER_READ = 8
TRUE_READ = 9
FALSE_READ = 10

def make_helper(v_from, v_to, container):
  def helper(func):
    def nested(self, *args, **kwargs):
      assert self.state == v_from or self.state == container, (v_from, container, self.state)
      try:
        return func(self, *args, **kwargs)
      finally:
        if self.state == v_from:
          self.state = v_to
    return nested
  return helper
writer = make_helper(VALUE_WRITE, WRITE, CONTAINER_WRITE)
reader = make_helper(VALUE_READ, READ, CONTAINER_READ)

def makeZigZag(n, bits):
  return (n << 1) ^ (n >> (bits - 1))

def fromZigZag(n):
  return (n >> 1) ^ -(n & 1)

def writeVarint(trans, n):
  out = []
  while True:
    if n & ~0x7f == 0:
      out.append(n)
      break
    else:
      out.append((n & 0xff) | 0x80)
      n = n >> 7
  trans.write(''.join(map(chr, out)))

def readVarint(trans):
  result = 0
  shift = 0
  while True:
    x = trans.readAll(1)
    byte = ord(x)
    result |= (byte & 0x7f) << shift
    if byte >> 7 == 0:
      return result
    shift += 7

class CompactType:
  TRUE = 1
  FALSE = 2
  BYTE = 0x03
  I16 = 0x04
  I32 = 0x05
  I64 = 0x06
  DOUBLE = 0x07
  BINARY = 0x08
  LIST = 0x09
  SET = 0x0A
  MAP = 0x0B
  STRUCT = 0x0C

CTYPES = {TType.BOOL: CompactType.TRUE, # used for collection
          TType.BYTE: CompactType.BYTE,
          TType.I16: CompactType.I16,
          TType.I32: CompactType.I32,
          TType.I64: CompactType.I64,
          TType.DOUBLE: CompactType.DOUBLE,
          TType.STRING: CompactType.BINARY,
          TType.STRUCT: CompactType.STRUCT,
          TType.LIST: CompactType.LIST,
          TType.SET: CompactType.SET,
          TType.MAP: CompactType.MAP,
          }

TTYPES = {}
for k, v in CTYPES.items():
  TTYPES[v] = k
TTYPES[CompactType.FALSE] = TType.BOOL
del k
del v

class TCompactProtocol(TProtocolBase):
  "Compact implementation of the Thrift protocol driver."

  PROTOCOL_ID = 0x82
  VERSION = 1
  VERSION_MASK = 0x1f
  TYPE_MASK = 0xe0
  TYPE_SHIFT_AMOUNT = 5

  def __init__(self, trans):
    TProtocolBase.__init__(self, trans)
    self.state = CLEAR
    self.__last_fid = 0
    self.__bool_fid = None
    self.__structs = []

  def __writeVarint(self, n):
    writeVarint(self.trans, n)

  def writeMessageBegin(self, name, type, seqid):
    assert self.state == CLEAR
    self.__writeUByte(self.PROTOCOL_ID)
    self.__writeUByte(self.VERSION | (type << self.TYPE_SHIFT_AMOUNT))
    self.__writeVarint(seqid)
    self.__writeString(name)
    self.state = WRITE

  def writeMessageEnd(self):
    assert self.state == WRITE
    self.state = CLEAR

  def writeStructBegin(self, name):
    assert self.state in (CLEAR, WRITE, CONTAINER_WRITE, VALUE_WRITE), self.state
    self.__structs.append((self.state, self.__last_fid))
    self.state = WRITE
    self.__last_fid = 0

  def writeStructEnd(self):
    assert self.state == WRITE
    self.state, self.__last_fid = self.__structs.pop()
    if self.state == VALUE_WRITE:
      self.state = WRITE

  def writeCollectionEnd(self):
    assert self.state == CONTAINER_WRITE
    self.state = WRITE
  writeMapEnd = writeCollectionEnd
  writeSetEnd = writeCollectionEnd
  writeListEnd = writeCollectionEnd

  def __writeFieldHeader(self, type, fid):
    delta = fid - self.__last_fid
    if 0 < delta <= 15:
      self.__writeUByte(delta << 4 | type)
    else:
      self.__writeByte(type)
      self.__writeI16(fid)
    self.__last_fid = fid

  def writeFieldBegin(self, name, type, fid):
    assert self.state == WRITE, self.state
    if type == TType.BOOL:
      self.state = BOOL_WRITE
      self.__bool_fid = fid
    else:
      self.state = VALUE_WRITE
      self.__writeFieldHeader(CTYPES[type], fid)

  def __writeUByte(self, byte):
    self.trans.write(pack('!B', byte))

  def __writeByte(self, byte):
    self.trans.write(pack('!b', byte))

  def __writeI16(self, i16):
    self.__writeVarint(makeZigZag(i16, 16))

  def __writeSize(self, i32):
    self.__writeVarint(i32)

  def readFieldBegin(self):
    assert self.state == READ, self.state
    type = self.__readUByte()
    if type & 0x0f == TType.STOP:
      return None, 0, 0
    delta = type >> 4
    if delta == 0:
      fid = self.__readI16()
    else:
      fid = self.__last_fid + delta
    self.__last_fid = fid
    type = type & 0x0f
    if type == CompactType.TRUE:
      self.state = TRUE_READ
    elif type == CompactType.FALSE:
      self.state = FALSE_READ
    else:
      self.state = VALUE_READ
    return None, self.__getTType(type), fid

  def writeCollectionBegin(self, etype, size):
    assert self.state == VALUE_WRITE
    if size <= 14:
      self.__writeUByte(size << 4 | CTYPES[etype])
    else:
      self.__writeUByte(0xf0 | CTYPES[etype])
      self.__writeSize(size)
    self.state = CONTAINER_WRITE
  writeSetBegin = writeCollectionBegin
  writeListBegin = writeCollectionBegin

  def writeMapBegin(self, ktype, vtype, size):
    assert self.state == VALUE_WRITE
    if size == 0:
      self.__writeByte(0)
    else:
      self.__writeSize(size)
      self.__writeUByte(CTYPES[ktype] << 4 | CTYPES[vtype])
    self.state = CONTAINER_WRITE

  def writeBool(self, bool):
    if self.state == BOOL_WRITE:
      self.__writeFieldHeader(types[bool], self.__bool_fid)
    elif self.state == VALUE_WRITE:
      self.__writeByte(int(bool))
    else:
      raise AssertetionError, "Invalid state in compact protocol"
    self.state = WRITE

  writeByte = writer(__writeByte)
  writeI16 = writer(__writeI16)

  @writer
  def writeI32(self, i32):
    self.__writeVarint(makeZigZag(i32, 32))

  @writer
  def writeI64(self, i64):
    self.__writeVarint(makeZigZag(i64, 64))

  @writer
  def writeDouble(self, dub):
    self.trans.write(pack('!d', dub))

  def __writeString(self, s):
    self.__writeSize(len(s))
    self.trans.write(s)
  writeString = writer(__writeString)

  def __readUByte(self):
    result, = unpack('!B', self.trans.readAll(1))
    return result

  def __readByte(self):
    result, = unpack('!b', self.trans.readAll(1))
    return result

  def __readVarint(self):
    return readVarint(self.trans)
    result = 0
    shift = 0
    while True:
      byte = self.__readUByte()
      result |= (byte & 0xf7) << shift
      if byte >> 7 == 0:
        return result
      shift += 7

  def __readZigZag(self):
    return fromZigZag(self.__readVarint())

  def __getTType(self, byte):
    return TTYPES[byte & 0x0f]

  def __readSize(self):
    result = self.__readVarint()
    if result > 0x7fff:
      raise TException("Length is too long")
    elif result < 0:
      raise TException("Length < 0")
    return result

  def readMessageBegin(self):
    assert self.state == CLEAR
    proto_id = self.__readUByte()
    if proto_id != self.PROTOCOL_ID:
      raise TProtocolException(TProtocolException.BAD_VERSION,
          'Bad protocol id in the message: %d' % proto_id)
    ver_type = self.__readUByte()
    type = (ver_type & self.TYPE_MASK) >> self.TYPE_SHIFT_AMOUNT
    version = ver_type & self.VERSION_MASK
    if version != self.VERSION:
      raise TProtocolException(TProtocolException.BAD_VERSION,
          'Bad version: %d (expect %d)' % (version, self.VERSION))
    seqid = self.__readVarint()
    name = self.__readString()
    return name, type, seqid

  def readMessageEnd(self):
    assert self.state == READ
    assert len(self.__structs) == 0
    self.state = CLEAR

  def readStructBegin(self):
    assert self.state == CLEAR or \
          self.state == READ or \
          self.state == CONTAINER_READ or \
          self.state == VALUE_READ, self.state
    self.__structs.append((self.state, self.__last_fid))
    self.state = READ
    self.__last_fid = 0

  def readStructEnd(self):
    assert self.state == READ
    self.state, self.__last_fid = self.__structs.pop()
    if self.state == VALUE_READ:
      self.state = READ

  def readCollectionBegin(self):
    assert self.state == VALUE_READ
    self.state = CONTAINER_READ
    size_type = self.__readUByte()
    type = self.__getTType(size_type)
    if size_type >> 4 == 15:
      return type, self.__readSize()
    else:
      return type, size_type >> 4
  readSetBegin = readCollectionBegin
  readListBegin = readCollectionBegin

  def readMapBegin(self):
    assert self.state == VALUE_READ
    self.state = CONTAINER_READ
    size = self.__readSize()
    types = 0
    if size > 0:
      types = self.__readUByte()
    vtype = self.__getTType(types)
    ktype = self.__getTType(types >> 4)
    return ktype, vtype, size

  def readCollectionEnd(self):
    assert self.state == CONTAINER_READ
    self.state = READ
  readSetEnd = readCollectionEnd
  readListEnd = readCollectionEnd
  readMapEnd = readCollectionEnd

  def readBool(self):
    if self.state == TRUE_READ:
      self.state = READ
      return True
    elif self.state == FALSE_READ:
      self.state = READ
      return False
    elif self.state == CONTAINER_READ:
      self.state = CONTAINER_READ
      return bool(self.__readByte())
    else:
      raise AssertionError, "Invalid state in compact protocol: %d" % self.state

  readByte = reader(__readByte)
  __readI16 = __readZigZag
  readI16 = reader(__readZigZag)
  readI32 = reader(__readZigZag)
  readI64 = reader(__readZigZag)

  @reader
  def readDouble(self):
    buff = self.trans.readAll(8)
    val, = unpack('!d', buff)
    return val

  def __readString(self):
    len = self.__readSize()
    return self.trans.readAll(len)
  readString = reader(__readString)

  def writeFieldStop(self):
    self.__writeByte(0)

class TCompactProtocolFactory:
  def __init__(self):
    pass

  def getProtocol(self, trans):
    return TCompactProtocol(trans)