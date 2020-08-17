package com.plv.azbuka.tokenization

import java.io.File
import java.nio.file.Path

/*
 * Tokenizable is the base interface for tokenizers in the library. It requires subclasses to define these behaviors:
 *  - How to read a file (i.e. Streaming or keeping it in memory?)
 *  - How to convert a string into tokens
 *  - How to map a list of tokens into positions into a larger file
 */
interface Tokenizable {
    fun toTokens(docPath: Path): Map<StringToken, List<Int>>


    fun toTokens(str: String): List<StringToken>
}
