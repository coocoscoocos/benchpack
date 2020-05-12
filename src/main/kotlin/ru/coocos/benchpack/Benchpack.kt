package ru.coocos.benchpack

import ru.coocos.benchpack.usermerger.MergerBenchmark
import ru.coocos.benchpack.usermerger.MergerBenchmarkMt

object Benchpack {

    @JvmStatic
    fun main(args: Array<String>) {
//        val mergerBenchmark = MergerBenchmark()
//        mergerBenchmark.run()
        val mergerBenchmarkMt = MergerBenchmarkMt()
        mergerBenchmarkMt.run()

    }
}