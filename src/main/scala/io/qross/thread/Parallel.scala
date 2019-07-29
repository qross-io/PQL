package io.qross.thread

import io.qross.time.Timer

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

 class Parallel {

    private val threads = new ArrayBuffer[Thread]()

    def add(thread: Thread): Unit = {
        this.threads += thread
    }

    def start(thread: Thread): Unit = {
        this.threads += thread
        thread.start()
    }

    def startAll(interval: Float = 0.1F): Unit = {
        for (t <- this.threads) {
            if (interval > 0) {
                Timer.sleep(interval)
            }
            t.start()
        }
    }

    def size: Int = this.threads.size

    def running: Boolean = {
        var alive = false
        breakable {
            for (t <- this.threads) {
                if (t.isAlive) {
                    alive = true
                    break
                }
            }
        }
        alive
    }

    def activeAmount: Int = {
        var amount = 0
        for (t <- this.threads) {
            if (t.isAlive) amount += 1
        }
        amount
    }

    def waitAll(): Unit = {
        while (this.running) {
            Timer.sleep(1F)
        }
    }

    def clear(): Unit = {
        this.threads.clear
    }
}
