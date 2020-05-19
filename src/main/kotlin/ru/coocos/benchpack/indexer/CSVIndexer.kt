package ru.coocos.benchpack.indexer

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.index.NoMergePolicy
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.store.MMapDirectory
import org.apache.lucene.store.RAMDirectory
import java.io.File
import java.nio.file.Paths
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore

object CSVIndexer {

    @JvmStatic
    fun main(args: Array<String>) {
        val dir = "/opt/data/wiki_csv"
//        val dir = "/opt/data/wiki_csv" + System.getProperty("ind")
        val fsDir = RAMDirectory()
        val writer = IndexWriter(
//                FSDirectory.open(Paths.get("/opt/data/lucene")),
                fsDir,
                IndexWriterConfig(StandardAnalyzer())
                        .setRAMPerThreadHardLimitMB(512)
                        .setMergePolicy(NoMergePolicy.INSTANCE)
        )
        val diskSemaphore = Semaphore(32)
        val executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors())
        val futures = File(dir).listFiles()?.map {executorService.submit(CSVIndexerRunnable(writer, it.absolutePath, diskSemaphore))}?.toList()
        futures?.forEach { it.get() }
        writer.flush()
        writer.close()
        val usedGb = fsDir.ramBytesUsed() / 1024 / 1024 / 1024
        println("Used ${usedGb}GB")
        executorService.shutdown()
    }
}