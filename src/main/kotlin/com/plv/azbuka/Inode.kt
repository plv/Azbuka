package com.plv.azbuka.com.plv.bukva

import com.plv.azbuka.tokenization.PlainTextTokenizer
import com.plv.azbuka.tokenization.StringToken
import com.plv.azbuka.tokenization.Tokenizable
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes

/*
  Inode represents some filesystem object that we're interested in tracking or indexing. Its subtypes (Document, Directory)
  and wrappers (StoredInode<T : Inode>) form a base framework for the rest of the library.
 */
// TODO (#1):  Storing a Tokenizable in each Directory and Document is a bad idea. It wastes space with no real time benefits
// (esp. if API users may supply a tokenizer that is not a singleton). Also, this can leak if we accidentally keep
// references to a Tokenizable that is also a Document, even after we remove the Document from the DocumentStore.
// I'm crunched for time as of writing this comment, but the smarter thing to do would be to dynamic dispatch
// using a map of some sort.
sealed class Inode {
  abstract val path: Path
  abstract val tokenizer: Tokenizable
}

/*
  Directory is an Inode that cannot be tokenized and indexed itself, but can store the tokenizer for the inheritance
  of its children. A directory on an actual filesystem has many directory and document children, but we do not keep
  that information here. See InodeStore, which keeps a rough mapping between stored Directories and its child Inodes.

  Directory also contains a small, incomplete Kotlin DSL builder API wrapped around SimpleFileVisitor.
 */
data class Directory(override val path: Path, override val tokenizer: Tokenizable = PlainTextTokenizer) : Inode() {
  val traverseFns: MutableMap<TraverseEvent, (Path) -> FileVisitResult> = mutableMapOf()

  fun walkContents(traverse: Directory.() -> Unit) {
    this.traverse()
    Files.walkFileTree(path, object : SimpleFileVisitor<Path>() {
      override fun preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult {
        traverseFns[TraverseEvent.PreVisitDirectory]?.also { it(dir) }
        return FileVisitResult.CONTINUE
      }

      override fun visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult {
        traverseFns[TraverseEvent.VisitFile]?.also { it(file) }
        return FileVisitResult.CONTINUE
      }
    })
  }

  fun visitFile(event: (Path) -> FileVisitResult) {
    this.traverseFns[TraverseEvent.VisitFile] = event
  }

  fun preVisitDirectory(event: (Path) -> FileVisitResult) {
    this.traverseFns[TraverseEvent.PreVisitDirectory] = event
  }

  enum class TraverseEvent {
    VisitFile,
    PreVisitDirectory,
  }
}

/*
  A document that represents a file on the filesystem. It is tokenizable and can be stored in the index.
 */
data class Document(override val path: Path, override val tokenizer: Tokenizable = PlainTextTokenizer) : Inode(),
  Tokenizable by tokenizer {
  fun toTokens(): Map<StringToken, List<Int>> = toTokens(path)
}
