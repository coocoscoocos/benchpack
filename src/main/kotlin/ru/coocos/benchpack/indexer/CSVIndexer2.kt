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

object CSVIndexer2 {

    @JvmStatic
    fun main(args: Array<String>) {
        val dir = "/opt/data/wiki_csv"
        val fsDir = RAMDirectory()
        val writer = IndexWriter(
                fsDir,
                IndexWriterConfig(StandardAnalyzer())
                        .setRAMPerThreadHardLimitMB(512)
                        .setMergePolicy(NoMergePolicy.INSTANCE)
        )
        val diskExecutorPool = Executors.newFixedThreadPool(32)
        val indexExecutorPool = Executors.newFixedThreadPool(96)
        val indexFutures = arrayListOf<Future<Unit>>()
        val titleWordCount = AtomicLong(0)
        val textWordCount = AtomicLong(0)
        val diskFutures = File(dir).listFiles()?.map {
            diskExecutorPool.submit {
                val bytes = it.readText()
                println(bytes.length)
                val indexFuture = indexExecutorPool.submit<Unit> {
                    val parser = CSVParser.parse(bytes, CSVFormat.RFC4180)
                    val docList = arrayListOf<Document>()
                    var titleWords = 0
                    var textWords = 0
                    parser.forEach {
                        titleWords = it.get(0).split(" ").size
                        textWords = it.get(1).split(" ").size
                        titleWordCount.addAndGet(titleWords.toLong())
                        textWordCount.addAndGet(textWords.toLong())
                        val document = Document()
                        document.add(StringField("title", it.get(0), Field.Store.YES))
                        document.add(TextField("text", it.get(1), Field.Store.YES))
                        docList.add(document)
                    }
                    parser.close()
                    writer.addDocuments(docList)
                }
                indexFutures.add(indexFuture)
            }
        }?.toList()
        diskFutures?.forEach { it.get() }
        indexFutures.forEach { it.get() }
        diskExecutorPool.shutdown()
        indexExecutorPool.shutdown()
    }
}