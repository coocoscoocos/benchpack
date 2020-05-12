package ru.coocos.benchpack.usermerger

import ru.coocos.merge.Merger
import ru.coocos.merge.User
import kotlin.random.Random

class MergerBenchmark : Runnable {

    override fun run() {
        println("Warming JVM...")
        (1..3).forEach {
            bench()
        }
        println("done")
        val results = mutableListOf<Long>()
        (1..3).forEach {
            print("run $it")
            val startMillis = System.currentTimeMillis()
            bench()
            val endMillis = System.currentTimeMillis()
            val result = endMillis -startMillis
            results.add(result)
            println("result = $result ms.")
        }
        println("Average: " + results.average())
    }

    private fun bench() {
        val userCount = 75_000L
        val merger = Merger()
        for (i in 1..userCount) {
            val name = "user" + 3 * i
            val mailCount = Random.nextInt(5, 15)
            val mailSet = (1..mailCount).map {"user" + Random.nextLong(userCount * 15) + "@domain.com" }.toHashSet()
            merger.add(User(name, mailSet))
        }
        val count = merger.getResult().count()
        println("  bench output: " + count)
    }
}