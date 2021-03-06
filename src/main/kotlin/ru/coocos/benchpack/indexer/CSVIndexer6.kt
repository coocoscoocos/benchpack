package ru.coocos.benchpack.indexer

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.index.NoMergePolicy
import org.apache.lucene.store.RAMDirectory
import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

object CSVIndexer6 {

    val writerCount = 64
    val isFinished = AtomicBoolean(false)
    val indexThreadList = arrayListOf<Thread>()

    @JvmStatic
    fun main(args: Array<String>) {
        val dir = "/opt/data/wiki_csv"
        val fsDirList = arrayListOf<RAMDirectory>()
        val writerList = (1..writerCount).map {
            val fsDir = RAMDirectory()
            fsDirList.add(fsDir)
            IndexWriter(
                fsDir,
                IndexWriterConfig(StandardAnalyzer())
                    .setRAMPerThreadHardLimitMB(512)
                    .setMergePolicy(NoMergePolicy.INSTANCE)
            )
        }.toList()

        val queue = LinkedBlockingQueue<String>()
        val diskExecutorPool = Executors.newFixedThreadPool(32)
        val diskFutureList = File(dir).listFiles()?.map { diskExecutorPool.submit { queue.put(it.readText()) } }?.toList()
        var writerIndex = 0
        (1..96).forEach { createIndexThread(writerList[writerIndex++ % writerCount], queue) }
        diskFutureList?.forEach { it.get() }
        diskExecutorPool.shutdown()
        (97..128).forEach { createIndexThread(writerList[writerIndex++ % writerCount], queue) }
        isFinished.set(true)
        indexThreadList.forEach(Thread::join)
        fsDirList.forEach {
            println(it.ramBytesUsed())
        }
    }

    fun createIndexThread(writer : IndexWriter, queue : LinkedBlockingQueue<String>) : Thread {
        val thread = Thread {
            while (true) {
                val content = queue.poll()
                if (null == content) {
                    if (isFinished.get()) {
                        return@Thread
                    }
                    continue;
                }
                val parser = CSVParser.parse(content, CSVFormat.RFC4180)
                val docList = parser.map {
                    val document = Document()
                    document.add(StringField("title", it.get(0), Field.Store.YES))
                    document.add(TextField("text", it.get(1), Field.Store.YES))
                    document
                }.toList()
                writer.addDocuments(docList)
            }
        }
        thread.start()
        indexThreadList.add(thread)
        return thread
    }
}