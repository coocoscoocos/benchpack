package ru.coocos.benchpack.indexer

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.store.MMapDirectory
import java.nio.file.Paths
import java.util.concurrent.LinkedBlockingQueue
import kotlin.random.Random


class IndexerBenchmark : Runnable {

    override fun run() {

        val startMs = System.currentTimeMillis()
        val writer = IndexWriter(
                FSDirectory.open(Paths.get("/opt/ramdisk/lucene")),
//                FSDirectory.open(Paths.get("/home/coocos/lucene")),
//                MMapDirectory.open(Paths.get("/opt/data/lucene" + Random.nextLong())),
                IndexWriterConfig(StandardAnalyzer())
                        .setRAMPerThreadHardLimitMB(512)
        )

        val docQueue = LinkedBlockingQueue<Document>(100_000)
//        val wikiReaderThread = RandomDocThread(docQueue)
//        val wikiReaderThread = WikiReaderThread(docQueue)
//        wikiReaderThread.start()
        val indexerThreadList = (1..Runtime.getRuntime().availableProcessors()).map {
            val indexerThread = RandomIndexerThread(writer)
            indexerThread.start()
            indexerThread
        }.toList()
//        wikiReaderThread.join()
//        println("Reader done")
//        indexerThreadList.forEach {it.isFinished.set(true)}
        indexerThreadList.forEach(Thread::join)
        writer.flush()
        val endMs = System.currentTimeMillis()
        println("Ms: " + (endMs - startMs))
    }

}