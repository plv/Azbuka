package com.plv.azbuka

import com.plv.azbuka.com.plv.bukva.Directory
import com.plv.azbuka.com.plv.bukva.Document
import com.plv.azbuka.tokenization.StringToken
import com.plv.azbuka.tokenization.Tokenizable
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Assert.*
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.util.*

class InodeStoreTest {
  @get:Rule
  val tempDir = TemporaryFolder()

  @Test
  fun testAddRemove_basic() {
    val store = InodeStore()

    val doc = Document(tempDir.newFile().toPath())
    val stored = store.add(doc)
    assertSame(store.get<Document>(doc.path), store.get<Document>(stored.id))

    assertSame(doc, store.remove(stored))
    assertEquals(0, store.size)
  }

  @Test
  fun testAddRemove_AddDocumentThenParentDir() {
    val store = InodeStore()

    val doc = Document(tempDir.newFile("test.txt").toPath())
    val dir = Directory(tempDir.root.toPath())

    store.add(doc)
    tempDir.newFile("test2.txt")
    val storedDir = store.add(dir)
    assertEquals(3, store.size)

    store.remove(storedDir.first.reify<Directory>())

    assertEquals(0, store.size)
  }

  @Test
  fun testAddRemove_DocContainingDir() {
    val store = InodeStore()
    val doc = Document(tempDir.newFile().toPath())
    store.add(doc)
    val storedDir = store.add(Directory(tempDir.root.toPath()))
    store.remove(storedDir.first.reify<Directory>())
    assertEquals(0, store.size)
  }

  @Test
  fun testAddRemove_nestedDirectoryHierarchy() {
    val store = InodeStore()

    // Set up nested members + sibling
    val doc = Document(tempDir.newFile().toPath())
    store.add(doc)
    val subDirPath = tempDir.newFolder("subdir").toPath()
    Files.createFile(subDirPath.resolve("subfile.txt"))

    val stored = store.add(Directory(tempDir.root.toPath()))
    assertEquals(stored.second.size + 1, store.size)

    store.remove(stored.first.reify<Directory>())
    assertEquals(0, store.size)
  }

  @Test
  fun testAddRemove_addSubDirThenParentDir() {
    val store = InodeStore()

    val subDirPath = tempDir.newFolder("subdir").toPath()
    Files.createFile(subDirPath.resolve("subfile.txt"))
    store.add(Directory(subDirPath))

    assertEquals(2, store.size)
    val rootStored = store.add(Directory(tempDir.root.toPath()))
    assertEquals(3, store.size)

    store.remove(rootStored.first.reify<Directory>())
    assertEquals(0, store.size)
  }

  @Test
  fun testDuplicateInodes() {
    val store = InodeStore()
    val doc = Document(tempDir.newFile().toPath())
    val dir = Directory(tempDir.newFolder().toPath())

    val storedDoc = store.add(doc)
    val storedDoc2 = store.add(doc)
    assertTrue(storedDoc === storedDoc2)
    val oldSize = store.size

    val storedDir = store.add(dir)
    val storedDir2 = store.add(dir)

    assertTrue(storedDir.first === storedDir2.first)
    assertTrue(storedDir.second == storedDir2.second)

    assertEquals(store.size - 1, oldSize)
  }

  @Test
  fun testDuplicateInodes_DifferentTokenizers() {
    val store = InodeStore()

    val file = tempDir.newFile().toPath()

    val doc = Document(file)
    val docCustomTokenizer = Document(file, object : Tokenizable {
      override fun toTokens(docPath: Path): Map<StringToken, List<Int>> {
        return mapOf()
      }

      override fun toTokens(str: String): List<StringToken> {
        return listOf()
      }
    })

    val docStored = store.add(doc)
    val docCustomTokenizerStored = store.add(docCustomTokenizer)
    assertSame(docStored, docCustomTokenizerStored)

    val folder = tempDir.newFolder().toPath()
    Files.createFile(folder.resolve("subfile.txt"))

    val dir = Directory(folder)
    val dirCustomTokenizer = Directory(folder, object : Tokenizable {
      override fun toTokens(docPath: Path): Map<StringToken, List<Int>> {
       return mapOf()
      }

      override fun toTokens(str: String): List<StringToken> {
        return listOf()
      }
    })

    val dirStored = store.add(dir)
    val dirCustomTokenizerStored = store.add(dirCustomTokenizer)
    assertEquals(dirStored, dirCustomTokenizerStored)
  }

  @Test
  fun testIdAtomicity() {
    val store = InodeStore()
    val jobs: LinkedList<Job> = LinkedList()

    repeat(1000) {
      jobs.add(GlobalScope.launch {
        val tempPath = tempDir.newFile("rand$it").toPath().toAbsolutePath()
        val doc = Document(tempPath)
        store.add(doc)
        tempPath.toFile().delete()
      })
    }
    jobs.forEach { runBlocking { it.join() } }
    assertNotNull(store.get<Document>(999))
  }
}