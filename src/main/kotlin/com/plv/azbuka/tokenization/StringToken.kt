package com.plv.azbuka.tokenization

/*
 * StringToken is the "top" type of all tokens. Any derived tokenizers can subclass StringToken to provide their own
 * token types. This is useful those who want to index and search things other than ordinary text.
 */
open class StringToken(val representation: String) {
  override fun hashCode(): Int = representation.hashCode()
  override fun equals(other: Any?): Boolean = other == representation
  override fun toString(): String = "StringToken($representation)"
}

