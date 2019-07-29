package io.qross.thread

class Cube(max: Int = -1) {

    private var value = -1
    private var threads = 0

    def increase(v: Int = 1): Int = synchronized {
        this.value += v
        this.value
    }

    def get: Int = this.value

    def achieve(): Unit = synchronized {
        this.value = this.max
    }

    def achieved: Boolean = this.value >= this.max


    def reset(): Unit = synchronized {
        this.value = -1
    }

    //活跃线程+1, 在线程内部判断时使用
    def mark(): Unit = synchronized {
        this.threads += 1
    }

    //活跃线程-1, 在线程内部判断时使用
    def wipe(): Unit = synchronized {
        this.threads -= 1
    }

    //获取活跃线程数量
    def active: Int = this.threads

    //是否所有线程已经退出
    def closed: Boolean = this.threads == 0
}
