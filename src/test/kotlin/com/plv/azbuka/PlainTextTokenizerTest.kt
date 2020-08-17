package com.plv.azbuka

import com.plv.azbuka.tokenization.*
import org.junit.Test
import org.junit.Assert.*
import java.io.ByteArrayInputStream


class PlainTextTokenizerTest {
    private val TOKENIZER_TEST_STRING = "Lorem ipsum dolor sit amet"

    @Test fun testBasicTokenization() {
        val fakeStream = ByteArrayInputStream("$TOKENIZER_TEST_STRING $TOKENIZER_TEST_STRING".toByteArray())
        val tokens = PlainTextTokenizer.toTokens(fakeStream)
        assertEquals(tokens, mapOf(
            StringToken("lorem") to listOf(0, 5),
            StringToken("ipsum") to listOf(1, 6),
            StringToken("dolor") to listOf(2, 7),
            StringToken("sit") to listOf (3, 8),
            StringToken("amet") to listOf(4, 9)
        ))
    }

    @Test fun testBasicTokenization_nonASCII() {
        val charsets = listOf(
            Charsets.ISO_8859_1,
            Charsets.UTF_16,
            Charsets.UTF_16BE,
            Charsets.UTF_16LE,
            Charsets.UTF_32,
            Charsets.UTF_32BE,
            Charsets.UTF_32LE,
            Charsets.UTF_8
        )

        charsets.forEach {
            val fakeStream = ByteArrayInputStream("$TOKENIZER_TEST_STRING $TOKENIZER_TEST_STRING".toByteArray(it))
            val tokens = PlainTextTokenizer.toTokens(fakeStream)
            assertEquals(tokens, mapOf(
                StringToken("lorem") to listOf(0, 5),
                StringToken("ipsum") to listOf(1, 6),
                StringToken("dolor") to listOf(2, 7),
                StringToken("sit") to listOf (3, 8),
                StringToken("amet") to listOf(4, 9)
            ))
        }
    }
}