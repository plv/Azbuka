package com.plv.azbuka.indexing

import com.plv.azbuka.InodeStore
import com.plv.azbuka.com.plv.bukva.Document
import com.plv.azbuka.com.plv.bukva.Inode
import com.plv.azbuka.tokenization.StringToken
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

typealias DocId = Int

class TokenIndex {
  private var index: MutableMap<
      StringToken,
      MutableMap<DocId, List<Int>>
      > = mutableMapOf()

  private val lock: ReentrantReadWriteLock = ReentrantReadWriteLock()

  val sizeTokens: Int
    get() = index.keys.size
  val sizeDocuments: Int
    get() = index.values.flatMapTo(mutableSetOf()) { it.keys }.size


  /*
   * Inserts tokens and their positions into index and associates them with the Id belonging to inode.
   * The StoredInode is not actually kept in the index, but is required as a way of statically confirming that,
   * at some point, the inner Document was at some point added to InodeStore.
   */
  fun add(inode: InodeStore.StoredInode<Document>, tokenization: Map<StringToken, List<Int>>) = lock.write {
    tokenization.forEach {
      val (tkn, positions) = it
      if (!index.containsKey(tkn)) {
        val map = LinkedHashMap<DocId, List<Int>>()
        map[inode.id] = positions
        index[tkn] = map
      } else {
        index[tkn]!![inode.id] = positions
      }
    }
  }

  /*
   * Bulk transaction for atomicity.
   */
  fun addAll(tokenizations: Map<InodeStore.StoredInode<Document>, Map<StringToken, List<Int>>>) = lock.write {
    tokenizations.forEach {
      val (inode, tokenization) = it
      add(inode, tokenization)
    }
  }

  /*
   * Remove all usages of a particular document from the index. If a token does not belong to any other documents,
   * then it will be removedTokens from the index entirely, and returns a list of such deleted tokens.
   *
   * NOTE: This is a fairly slow operation, as we traverse through the entire index.
   */
  fun remove(doc: InodeStore.StoredInode<Document>): List<StringToken> = lock.write {
    val removedTokens: MutableList<StringToken> = mutableListOf()
    index.forEach {
      val (tkn, usages) = it
      usages[doc.id]?.run {
        index.getValue(tkn).remove(doc.id)
        if (index.getValue(tkn).isEmpty()) {
          removedTokens.add(tkn)
        }
      }
    }

    removedTokens.forEach { index.remove(it)!! }
    return removedTokens
  }

  fun removeAll(docs: Set<InodeStore.StoredInode<Document>>): Map<InodeStore.StoredInode<Document>, StringToken> =
    lock.write {
      val removedDocs: MutableMap<InodeStore.StoredInode<Document>, StringToken> = mutableMapOf()
      val docsById = docs.fold(mutableMapOf<Int, InodeStore.StoredInode<out Inode>>()) { accum, next ->
        accum[next.id] = next
        return@fold accum
      }

      index.forEach {
        val (tkn, usages) = it
        usages.keys.intersect(docsById.keys).map { docId ->
          index.getValue(tkn).remove(docId)!!
        }
      }

      return removedDocs
    }

  /*
   * Searches for a given StringToken in the index, and returns a set of `DocId`s containing that token/
   * Returns an empty set if the token can't be found.
   */
  fun search(token: StringToken): Set<DocId> = lock.read {
    return index[token]?.keys ?: setOf()
  }

  /*
   * Bulk search: performs a disjoint search on each token, and returns a map containing the results of each search.
   * Note that duplicate tokens in the List will collide to the same key in map.
   */
  fun searchAll(tokens: List<StringToken>): Map<StringToken, Map<DocId, List<Int>>> = lock.read {
    tokens.fold(mutableMapOf()) { accum, token ->
      accum[token] = index[token] ?: mapOf()
      return@fold accum
    }
  }

  /*
   * Searches for terms appearing in documents in the consecutive order that they are provided in. This type of search
   * is useful for implementing a "phrase" search (i.e. converting a space-separated natural english string into
   * a list of StringTokens that we can look for).
   */
  fun searchAndConsecutive(terms: List<StringToken>): Set<DocId> {
    val searchAll = searchAll(terms)
    return terms.drop(1).fold(searchAll.getValue(terms.first())) { precedingTermLocations, nextTerm ->
      val nextTermLocations = searchAll.getValue(nextTerm)

      val docsWhereAllTermsExist = precedingTermLocations.keys.intersect(nextTermLocations.keys)
      val docsWhereTermsAreConsecutive = mutableMapOf<DocId, MutableList<Int>>()
      for (doc in docsWhereAllTermsExist) {
        val precedingTermPositions = precedingTermLocations.getValue(doc)
        val nextTermPositions = nextTermLocations.getValue(doc)
        precedingTermPositions.forEach {
          if (nextTermPositions.contains(it + 1)) {
            if (docsWhereTermsAreConsecutive[doc]?.add(it + 1) == null) {
              docsWhereTermsAreConsecutive[doc] = mutableListOf(it + 1)
            }
          }
        }
      }
      return@fold docsWhereTermsAreConsecutive
    }.keys
  }

  fun searchAnd(terms: List<StringToken>): Set<DocId> {
    return searchAll(terms).values.reduce { accumulatedTokenLocations, nextTokenLocations ->
      val documentsContainingBoth = accumulatedTokenLocations.keys.intersect(nextTokenLocations.keys)
      return@reduce accumulatedTokenLocations.filterKeys { it in documentsContainingBoth }
    }.keys
  }

  fun searchOr(terms: List<StringToken>): Set<DocId> = searchAll(terms).values.flatMap { tokenLocations ->
    tokenLocations.keys
  }.toSet()
}
