use std::u32;

/* The current RDB version. When the format changes in a way that is no longer
 * backward compatible this number gets incremented. */
pub const VERSION: u16 = 7;

/* Defines related to the dump file format. To store 32 bits lengths for short
 * keys requires a lot of space, so we check the most significant 2 bits of
 * the first byte to interpreter the length:
 *
 * 00|000000 => if the two MSB are 00 the len is the 6 bits of this byte
 * 01|000000 00000000 =>  01, the len is 14 byes, 6 bits + 8 bits of next byte
 * 10|000000 [32 bit integer] => if it's 01, a full 32 bit len will follow
 * 11|000000 this means: specially encoded object will follow. The six bits
 *           number specify the kind of object that follows.
 *           See the ENC_* defines.
 *
 * Lengths up to 63 are stored using a single byte, most DB keys, and may
 * values, will fit inside. */
pub const BITLEN6: u8 = 0;
pub const BITLEN14: u8 = 1;
pub const BITLEN32: u8 = 2;
pub const ENCVAL: u8 = 3;
pub const LENERR: u32 = u32::MAX;

/* When a length of a string object stored on disk has the first two bits
 * set, the remaining two bits specify a special encoding for the object
 * accordingly to the following defines: */
pub const ENC_INT8: u8 = 0; /* 8 bit signed integer */
pub const ENC_INT16: u8 = 1; /* 16 bit signed integer */
pub const ENC_INT32: u8 = 2; /* 32 bit signed integer */
pub const ENC_LZF: u8 = 3; /* string compressed with FASTLZ */

/* Dup object types to RDB object types. Only reason is readability (are we
 * dealing with RDB types or with in-memory object types?). */
pub const TYPE_STRING: u8 = 0;
pub const TYPE_LIST: u8 = 1;
pub const TYPE_SET: u8 = 2;
pub const TYPE_ZSET: u8 = 3;
pub const TYPE_HASH: u8 = 4;
/* NOTE: WHEN ADDING NEW RDB TYPE, UPDATE rdbIsObjectType() BELOW */

/* Object types for encoded objects. */
pub const TYPE_HASH_ZIPMAP: u8 = 9;
pub const TYPE_LIST_ZIPLIST: u8 = 10;
pub const TYPE_SET_INTSET: u8 = 11;
pub const TYPE_ZSET_ZIPLIST: u8 = 12;
pub const TYPE_HASH_ZIPLIST: u8 = 13;
pub const TYPE_LIST_QUICKLIST: u8 = 14;
/* NOTE: WHEN ADDING NEW RDB TYPE, UPDATE rdbIsObjectType() BELOW */

/* Special RDB opcodes (saved/loaded with rdbSaveType/rdbLoadType). */
pub const OPCODE_AUX: u8 = 250;
pub const OPCODE_RESIZEDB: u8 = 251;
pub const OPCODE_EXPIRETIME_MS: u8 = 252;
pub const OPCODE_EXPIRETIME: u8 = 253;
pub const OPCODE_SELECTDB: u8 = 254;
pub const OPCODE_EOF: u8 = 255;
