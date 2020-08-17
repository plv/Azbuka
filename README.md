# Azbuka


Azbuka is a string indexing and full text search library for Kotlin targeting the JVM. It has:

* A Simple API
* Thread-safe, atomic reads and writes to the index
* Automatic, opt-in reindexing on file and directory changes
* Support for adding other tokenizers

## Getting Started

Adding an existing Document:

```
$ import com.plv.azbuka.IndexManager
import com.plv.azbuka.com.plv.bukva.Document
import java.nio.file.Paths

val manager = IndexManager()
manager.add(Document(Paths.get("/tmp/tracking/someFile.txt")), shouldTrack = true)
manager.search("Azbuka")


```
Output:
```

kotlin.collections.Set<com.plv.azbuka.com.plv.bukva.Document> = [Document(path=/tmp/tracking/someFile.txt, tokenizer=com.plv.azbuka.tokenization.PlainTextTokenizer@57ac220f)]
```

## Demo

There is a simple demo available in `Demo.kt`. If you supply it arguments of paths you wish to track and query, it will register them and provide a REPL for you to perform basic searches.

