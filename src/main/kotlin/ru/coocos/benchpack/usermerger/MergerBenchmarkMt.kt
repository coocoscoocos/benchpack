package ru.coocos.benchpack.usermerger

import ru.coocos.merge.Merger
import ru.coocos.merge.User
import java.util.concurrent.ForkJoinPool
import kotlin.random.Random

class MergerBenchmarkMt : Runnable {

    override fun run() {
        println("Warming JVM...")
        (1..3).forEach {
            step()
        }
        println("done")
        val results = mutableListOf<Long>()
        (1..10).forEach {
            print("run $it")
            results.add(step())
        }
        println("Average: " + results.average())
        println("Min: " + results.min())
        println("Max: " + results.max())
    }

    private fun step() : Long {
        val userCount = 2_000_000L
        val users = (1..userCount).map {
            val name = "user" + 3 * it
            val mailCount = Random.nextInt(5, 10)
            val mailSet = (1..mailCount).map {"user" + Random.nextLong(userCount / 80) + "@domain.com" }.toHashSet()
            User(name, mailSet)
        } .toList();
        val startMillis = System.currentTimeMillis()
        val pool = ForkJoinPool(Runtime.getRuntime().availableProcessors())
        val r = pool.invoke(MergeRecursiveTask(users))
        println("  bench output: " + r.count())
        val endMillis = System.currentTimeMillis()
        return endMillis - startMillis
    }
}