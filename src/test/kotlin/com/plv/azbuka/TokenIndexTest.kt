package com.plv.azbuka

import com.plv.azbuka.tokenization.PlainTextTokenizer
import com.plv.azbuka.tokenization.StringToken
import org.junit.Assert.assertEquals
import org.junit.Test
import java.nio.file.Paths

class TokenIndexTest {
  private val storedDoc = InodeStore.StoredInode(
    1,
    Document(Paths.get("/some/fake/path"), PlainTextTokenizer)
  )
  private val storedDocTokenization = mapOf(
    StringToken("Hello") to listOf(1),
    StringToken("world") to listOf(2)
  )
  private val storedDoc2 = InodeStore.StoredInode(
    2,
    Document(Paths.get("/some/fake/path"), PlainTextTokenizer)
  )
  private val storedDoc2Tokenization = mapOf(
    StringToken("Hello") to listOf(1, 2),
    StringToken("there") to listOf(3),
    StringToken("my") to listOf(4),
    StringToken("name") to listOf(5),
    StringToken("is") to listOf(6)
  )

  @Test
  fun testAddRemove() {
    val index = TokenIndex()
    val storedDoc = InodeStore.StoredInode(
      1,
      Document(Paths.get("/some/fake/path"), PlainTextTokenizer)
    )
    index.add(
      storedDoc,
      mapOf(
        StringToken("Hello") to listOf(1),
        StringToken("world") to listOf(2)
      )
    )

    assertEquals(2, index.sizeTokens)
    assertEquals(1, index.sizeDocuments)

    val removedTokens = index.remove(storedDoc)
    assertEquals(removedTokens, listOf(StringToken("Hello"), StringToken("world")))
    assertEquals(0, index.sizeTokens)
    assertEquals(0, index.sizeDocuments)
  }

  @Test
  fun testSearch() {
    val index = TokenIndex()
    index.addAll(
      mapOf(
        storedDoc to storedDocTokenization,
        storedDoc2 to storedDoc2Tokenization
      )
    )

    assertEquals(6, index.sizeTokens)
    assertEquals(2, index.sizeDocuments)

    assertEquals(setOf(storedDoc.id, storedDoc2.id), index.search(StringToken("Hello")))
    assertEquals(setOf(storedDoc2.id), index.search(StringToken("name")))
    assertEquals(setOf(storedDoc.id), index.search(StringToken("world")))
    assertEquals(setOf<DocId>(), index.search(StringToken("doesnotexist")))
  }

  @Test
  fun testSearchAndConsecutive() {
    val index = TokenIndex()
    index.addAll(
      mapOf(
        storedDoc to storedDocTokenization,
        storedDoc2 to storedDoc2Tokenization
      )
    )

    assertEquals(
      setOf(storedDoc.id, storedDoc2.id),
      index.searchAndConsecutive(listOf(StringToken("Hello")))
    )
    assertEquals(
      setOf(storedDoc2.id),
      index.searchAndConsecutive(
        listOf(StringToken("Hello"), StringToken("there"), StringToken("my"))
      )
    )
    assertEquals(
      setOf<DocId>(),
      index.searchAndConsecutive(listOf(StringToken("there"), StringToken("Hello")))
    )
    assertEquals(
      setOf(storedDoc2.id),
      index.searchAndConsecutive(listOf(StringToken("Hello"), StringToken("Hello")))
    )
    assertEquals(
      setOf<DocId>(),
      index.searchAndConsecutive(
        listOf(
          StringToken("Hello"),
          StringToken("Hello"),
          StringToken("Hello")
        )
      )
    )
  }

  @Test
  fun testSearchAnd() {
    val index = TokenIndex()
    index.addAll(mapOf(storedDoc to storedDocTokenization, storedDoc2 to storedDoc2Tokenization))

    assertEquals(
      setOf(storedDoc.id, storedDoc2.id),
      index.searchAnd(
        listOf(StringToken("Hello"))
      )
    )
    assertEquals(
      setOf(storedDoc2.id),
      index.searchAnd(
        listOf(StringToken("my"))
      )
    )
    assertEquals(
      setOf(storedDoc2.id),
      index.searchAnd(
        listOf(StringToken("my"), StringToken("name"))
      )
    )
    assertEquals(
      setOf<DocId>(),
      index.searchAnd(
        listOf(StringToken("doesnotexist"))
      )
    )
    assertEquals(
      setOf(storedDoc.id, storedDoc2.id),
      index.searchAnd(
        listOf(StringToken("Hello"))
      )
    )
    assertEquals(
      setOf<DocId>(),
      index.searchAnd(
        listOf(
          StringToken("Hello"),
          StringToken("doesnotexist")
        )
      )
    )
    assertEquals(
      setOf(storedDoc2.id),
      index.searchAnd(
        listOf(
          StringToken("Hello"),
          StringToken("there")
        )
      )
    )
  }

  @Test
  fun testSearchOr() {
    val index = TokenIndex()
    index.addAll(mapOf(storedDoc to storedDocTokenization, storedDoc2 to storedDoc2Tokenization))
    assertEquals(
      setOf(1, 2),
      index.searchOr(
        listOf(
          StringToken("Hello"),
          StringToken("name")
        )
      )
    )
    assertEquals(
      setOf(1, 2),
      index.searchOr(
        listOf(
          StringToken("name"),
          StringToken("Hello")

        )
      )
    )
    assertEquals(
      setOf(2),
      index.searchOr(
        listOf(StringToken("name"))
      )
    )
  }
}