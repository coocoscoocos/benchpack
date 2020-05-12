package ru.coocos.benchpack.usermerger

import ru.coocos.merge.Merger
import ru.coocos.merge.User
import java.util.concurrent.ForkJoinTask
import java.util.concurrent.RecursiveTask

class MergeRecursiveTask (val userList : List<User>) : RecursiveTask<List<User>>() {

    val maxUsersInTask = 50_000;

    override fun compute(): List<User> {
        if (userList.size > maxUsersInTask) {
            val users = mutableListOf<User>()
            ForkJoinTask.invokeAll(createSubtasks()).forEach {
                val r = it.join()
                users.addAll(r)
            }
            return users
        } else {
            return process(userList)
        }
    }

    private fun createSubtasks() : Collection<MergeRecursiveTask> {
        return userList.chunked(maxUsersInTask).map { MergeRecursiveTask(it) }.toList()
    }

    private fun process(users : List<User>) : List<User> {
        val merger = Merger()
        users.forEach { merger.add(it) }
        return merger.getResult()
    }

}