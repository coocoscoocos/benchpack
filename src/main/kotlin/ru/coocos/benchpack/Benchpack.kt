package ru.coocos.benchpack

import ru.coocos.benchpack.indexer.IndexerBenchmark

object Benchpack {

    @JvmStatic
    fun main(args: Array<String>) {
//        val mergerBenchmark = MergerBenchmark()
//        mergerBenchmark.run()
//        val mergerBenchmarkMt = MergerBenchmarkMt()
//        mergerBenchmarkMt.run()
        val indexer = IndexerBenchmark()
        indexer.run()

    }
}