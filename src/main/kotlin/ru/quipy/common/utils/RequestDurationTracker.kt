import org.apache.commons.math3.stat.descriptive.rank.Percentile
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.withLock
import kotlin.math.ceil

class RequestDurationTracker(private val minSize: Int = 10, private val maxSize: Int = 100) {
    private val buffer = LongArray(maxSize)
    private var size = 0
    private var head = 0
    private val bufferRwLock = ReentrantReadWriteLock()

    fun track(block: () -> Unit) {
        val start = System.currentTimeMillis()
        try {
            block()
        } finally {
            addDuration(System.currentTimeMillis() - start)
        }
    }

    fun addDuration(duration: Long) {
//        println("adding duration $duration")
        bufferRwLock.writeLock().withLock {
            buffer[head] = duration
            head = (head + 1) % maxSize
            if (size < maxSize) {
                size++
            }
        }
    }

    fun getPercentileOrDefault(percentile: Double, default: Double): Double {
        if (size < minSize) {
//            println("default percentile: $default")
            return default
        }

        val durations = getDurations()
        durations.sort()
//        println("durations: ${durations.joinToString()}")
        val index = ceil(percentile / 100.0 * durations.size).toInt() - 1
        val res = durations[index]
//        println("percentile $percentile value: $res")
        return res
//        return percentile.evaluate(n)
    }

    fun getDurations(): DoubleArray {
        var buffer: LongArray = LongArray(0)
        var size: Int = 0
//        println("getting durations, read lock count: ${bufferRwLock.readLockCount}, write locked ${bufferRwLock.isWriteLocked}")
        bufferRwLock.readLock().withLock {
//            println("locked for read")
            buffer = this.buffer.copyOf(this.maxSize)
            size = this.size
        }
//        println("unlocked for read")

//        try {
//            val durations = DoubleArray(size)
//            for (i in 0 until size) {
//                val index = (head + i) % maxSize
//                durations[i] = buffer[index].toDouble()
//            }
//
//            println("durations: $durations")
//            return durations
//        } catch (e: Exception) {
//            println("error while reading durations: $e")
//            throw e
//        }
        val durations = DoubleArray(size)
        for (i in 0 until size) {
            durations[i] = buffer[i].toDouble()
        }

        return durations
    }
}