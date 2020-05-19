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
import java.util.concurrent.atomic.AtomicLong

object CSVIndexer4 {

    val threadCount = 4

    @JvmStatic
    fun main(args: Array<String>) {
        val dir = "/opt/data/wiki_csv"
        val fsDirList = arrayListOf<RAMDirectory>()
        val writerList = (1..threadCount).map {
            val fsDir = RAMDirectory()
            fsDirList.add(fsDir)
            IndexWriter(
                fsDir,
                IndexWriterConfig(StandardAnalyzer())
                    .setRAMPerThreadHardLimitMB(512)
                    .setMergePolicy(NoMergePolicy.INSTANCE)
            )
        }.toList()
        val writerIndex = AtomicLong(0)
        val diskExecutorPool = Executors.newFixedThreadPool(32)
        val indexExecutorPool = Executors.newFixedThreadPool(96)
        val indexFutures = arrayListOf<Future<Unit>>()
        val diskFutures = File(dir).listFiles()?.map {
            diskExecutorPool.submit {
                val bytes = it.readText()
                println(bytes.length)
                val indexFuture = indexExecutorPool.submit<Unit> {
                    val parser = CSVParser.parse(bytes, CSVFormat.RFC4180)
                    val docList = parser.map {
                        val document = Document()
                        document.add(StringField("title", it.get(0), Field.Store.YES))
                        document.add(TextField("text", it.get(1), Field.Store.YES))
                        document
                    }.toList()
                    val index = (writerIndex.incrementAndGet() % threadCount).toInt()
                    val writer = writerList[index]
                    writer.addDocuments(docList)
                }
                indexFutures.add(indexFuture)
            }
        }?.toList()
        diskFutures?.forEach { it.get() }
        indexFutures.forEach { it.get() }
        diskExecutorPool.shutdown()
        indexExecutorPool.shutdown()
        fsDirList.forEach {
            println(it.ramBytesUsed())
        }
    }
}