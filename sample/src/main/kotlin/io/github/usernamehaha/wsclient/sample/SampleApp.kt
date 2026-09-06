package io.github.usernamehaha.wsclient.sample

import android.app.Application
import android.util.Log
import io.github.usernamehaha.wsclient.WsClient
import io.github.usernamehaha.wsclient.WsEvent
import io.github.usernamehaha.wsclient.WsListener
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import okhttp3.Call
import okhttp3.EventListener
import okhttp3.OkHttpClient

class SampleApp : Application() {
    lateinit var wsClient: WsClient
        private set

    private val _events = MutableSharedFlow<WsEvent>(extraBufferCapacity = 64, onBufferOverflow = BufferOverflow.DROP_OLDEST)
    val events: SharedFlow<WsEvent> = _events

    @Volatile
    private var currentCall: Call? = null

    override fun onCreate() {
        super.onCreate()
        val okHttpClient = OkHttpClient.Builder()
            // 只为了演示：记住当前 WebSocket 对应的 Call，好在界面上模拟一次异常断线
            .eventListener(object : EventListener() {
                override fun callStart(call: Call) {
                    currentCall = call
                }
            })
            .build()

        // 长连接跟着进程走，不跟着页面走；页面只决定订阅什么
        wsClient = WsClient.create(okHttpClient) {
            url(BinanceStreams.URL)
            topicOf = BinanceStreams::topicOf
            // 库默认不打任何日志，要不要打、打到哪里由接入方决定
            listener = WsListener { event ->
                Log.d("WsClient", event.toString())
                _events.tryEmit(event)
            }
        }
        wsClient.connect()
    }

    /** 直接掐断底层连接，库会把它当成一次网络异常。 */
    fun simulateConnectionLoss() {
        currentCall?.cancel()
    }
}
