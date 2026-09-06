package io.github.usernamehaha.wsclient.sample

import io.github.usernamehaha.wsclient.Subscription
import java.util.concurrent.atomic.AtomicInteger
import org.json.JSONObject

/**
 * Binance 公开行情流的协议细节。库本身不认识任何协议，接入方需要提供的就是这个文件里的三样东西：
 * 连接地址、订阅消息怎么拼、消息属于哪个 topic。
 */
object BinanceStreams {
    const val URL = "wss://data-stream.binance.vision/ws"

    private val requestId = AtomicInteger()

    fun miniTicker(symbol: String): Subscription = stream("${symbol.lowercase()}@miniTicker")

    fun trades(symbol: String): Subscription = stream("${symbol.lowercase()}@trade")

    private fun stream(name: String): Subscription = Subscription(
        topic = name,
        subscribeMessage = """{"method":"SUBSCRIBE","params":["$name"],"id":${requestId.incrementAndGet()}}""",
        unsubscribeMessage = """{"method":"UNSUBSCRIBE","params":["$name"],"id":${requestId.incrementAndGet()}}""",
    )

    /** 推送形如 `{"e":"trade","s":"BTCUSDT",...}`；订阅确认 `{"result":null,"id":1}` 没有这两个字段，返回 null。 */
    fun topicOf(message: String): String? {
        val json = JSONObject(message)
        val event = json.optString("e").ifEmpty { return null }
        val symbol = json.optString("s").ifEmpty { return null }
        val stream = if (event == "24hrMiniTicker") "miniTicker" else event
        return "${symbol.lowercase()}@$stream"
    }
}
