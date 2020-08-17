package com.plv.azbuka

import com.plv.azbuka.com.plv.bukva.Directory
import com.plv.azbuka.com.plv.bukva.Document
import com.plv.azbuka.com.plv.bukva.Inode
import com.plv.azbuka.indexing.FileTracker
import com.plv.azbuka.indexing.TokenIndex
import com.plv.azbuka.tokenization.PlainTextTokenizer
import com.plv.azbuka.tokenization.StringToken
import kotlinx.coroutines.*
import java.nio.file.Files
import kotlin.concurrent.withLock


/*
 * IndexManager ties together TokenIndex, InodeStore, and FileTracker and provides the sole public API for the library.
 * Its responsibilities include:
 *  - Keeping `TokenIndex` and `InodeStore` in sync
 *  - Delegating queries to `TokenIndex`'s query API and converting its `DocId` responses to
      `InodeStore`-provided `Document`s
 *  - Routing FileTracker events to appropriate `InodeStore` and `TokenIndex` operations
 */
class IndexManager(private val index: TokenIndex, private val store: InodeStore) {
  private val tracker: FileTracker

  private val IndexingScope: CoroutineScope = CoroutineScope(newFixedThreadPoolContext(4, "azbuka-indexing"))

  constructor() : this(TokenIndex(), InodeStore())

  init {
    /*
     * Setup responses to `FileTracker` events.
     * TODO (#2): Batch these events so that we have more coarse-grained insertions into InodeStore.
     */
    tracker = FileTracker(scope = IndexingScope)
      .onCreate { affectedPath, associatedDir ->
        val inheritedTokenizer = store.get<Directory>(associatedDir)!!.inode.tokenizer
        IndexingScope.launch(Dispatchers.IO) {
          if (Files.isDirectory(affectedPath)) {
            store.add(Directory(affectedPath, inheritedTokenizer))
            return@launch
          }

          val doc = store.add(Document(affectedPath, inheritedTokenizer))
          index.add(doc, doc.inode.toTokens())
        }
      }
      .onModify { affectedPath, _ ->
        if (Files.isDirectory(affectedPath)) {
          // We try to register directories recursively, so we can ignore any modify events to directories.
          return@onModify
        }

        // TODO: This is very inefficient. At the end of the day, I should have a smarter way of doing this,
        // but I first need to choose a better data structure/layout for my index
        GlobalScope.launch(Dispatchers.IO) {
          val doc = store.get<Document>(affectedPath)
            ?: throw Exception("Tracked file $affectedPath modified, but missing from store")
          val tokens = async(Dispatchers.IO) { doc.inode.toTokens() }
          index.remove(doc)
          index.add(doc, tokens.await())
        }
      }
      .onDelete { affectedPath, _ ->
        val inode = store.get<Inode>(affectedPath)
          ?: throw Exception("Path $affectedPath reported as deleted by tracker, but missing from store")
        when (inode.inode) {
          is Document -> {
            store.remove(inode.reify<Document>())
            index.remove(inode.reify())
          }
          is Directory -> {
            // We should get events for children deletions, so all we need to do is to delete the directory
            store.remove(inode.reify<Directory>())
          }
        }
      }
  }

  /*
   * Adds a directory to `InodeStore` and inserts all children `Document`s into `TokenIndex`.
   */
  fun add(dir: Directory, shouldTrack: Boolean = false): InodeStore.StoredInode<Directory> {
    if (!Files.exists(dir.path)) {
      throw NoSuchFileException(dir.path.toFile())
    }

    val storedDir = store.add(dir)
    val childInsertedDocuments =
      storedDir.second.filter { it.inode is Document }.map { it.reify<Document>() }

    val job = IndexingScope.launch {
      val deferredTokenizations = withContext(Dispatchers.IO) {
        childInsertedDocuments.map { async { it.inode.toTokens() } }
      }
      val tokenizations =
        childInsertedDocuments.zip(deferredTokenizations.awaitAll()).associateBy({ it.first }, { it.second })

      index.addAll(tokenizations)
    }

    if (shouldTrack) {
      tracker.track(dir.path)
    }

    runBlocking { job.join() }
    return storedDir.first.reify()
  }

  /*
   * Inserts a `Document` into InodeStore and TokenIndex
   */
  fun add(doc: Document, shouldTrack: Boolean = false): InodeStore.StoredInode<Document> {
    val tokenization = doc.toTokens()
    val stored = store.add(doc)
    index.add(stored, tokenization)

    if (shouldTrack) {
      tracker.track(doc.path)
    }

    return stored
  }

  // Need to do this because Kotlin complains if I have separate remove(StoredInode<Document>) / remove(StoredInode<Directory>)
  fun <T : Inode> remove(stored: InodeStore.StoredInode<T>) {
    when (stored.inode) {
      is Document -> removeDoc(stored.reify())
      is Directory -> removeDir(stored.reify())
    }
  }

  private fun removeDoc(doc: InodeStore.StoredInode<Document>) {
    store.remove(doc) ?: throw IllegalArgumentException("$doc was not found in store, so it can't be removed")
    tracker.untrack(doc.inode.path)
  }

  private fun removeDir(dir: InodeStore.StoredInode<Directory>) {
    store.remove(dir)
    tracker.untrack(dir.inode.path)
  }

  /*
   * Search APIs wrapping those of `TokenIndex`. They fetch TokenIndex's returned DocIds and turn them into human-friendly
   * `Document`s. Each function acquires the read lock of InodeStore so that the `DocId`s returned from `TokenIndex`'s
   * search are not invalidated by another thread.
   */

  /*
   * Primary function for natural language full text searches. Tokenizes the query using PlainTextTokenizer so that
   * the query can match what is actually inserted into the index by default.
   */
  fun search(query: String): Set<Document> = store.readLock.withLock {
    val tokenized = PlainTextTokenizer.toTokens(query)
    val ids = if (' ' in query) {
      index.searchAndConsecutive(tokenized)
    } else {
      index.search(tokenized.first())
    }
    store.getAll(ids).asSequence().filterNotNull().map { it.reify<Document>().inode }.toSet()
  }

  fun search(term: StringToken): Set<Document> = store.readLock.withLock {
    val ids = index.search(term)
    store.getAll(ids).asSequence().filterNotNull().map { it.reify<Document>().inode }.toSet()
  }

  fun searchAnd(terms: List<StringToken>): Set<Document> = store.readLock.withLock {
    val ids = index.searchAnd(terms)
    store.getAll(ids).asSequence().filterNotNull().map { it.reify<Document>().inode }.toSet()
  }

  fun searchAndConsecutive(terms: List<StringToken>): Set<Document>  = store.readLock.withLock {
    val ids = index.searchAndConsecutive(terms)
    store.getAll(ids).asSequence().filterNotNull().map { it.reify<Document>().inode }.toSet()
  }

  fun searchOr(terms: List<StringToken>): Set<Document> = store.readLock.withLock {
    val ids = index.searchOr(terms)
    store.getAll(ids).asSequence().filterNotNull().map { it.reify<Document>().inode }.toSet()
  }
}
