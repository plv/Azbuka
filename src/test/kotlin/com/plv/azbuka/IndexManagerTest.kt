package com.plv.azbuka

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files

// TODO (#3): Write more tests for IndexManager
class IndexManagerTest {
  @get:Rule
  val tempDir = TemporaryFolder()

  @Test fun testAddRemoveDocument() {
    val manager = IndexManager()

    val tempPath = tempDir.newFile().toPath()
    Files.writeString(tempPath, "Test, hello world", Charsets.UTF_32)

    val stored = manager.add(Document(tempPath))
    assertEquals(stored.inode, manager.search("Test, hello world").first())

    manager.remove(stored)
    assertTrue(manager.search("Test, hello world").isEmpty())
  }
}