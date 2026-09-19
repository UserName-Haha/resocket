package io.github.usernamehaha.resocket.sample

import android.app.Application
import android.util.Log
import io.github.usernamehaha.resocket.ReSocket
import io.github.usernamehaha.resocket.ReSocketEvent
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import okhttp3.OkHttpClient

class SampleApp : Application() {
    lateinit var reSocket: ReSocket
        private set

    private val _events = MutableSharedFlow<ReSocketEvent>(extraBufferCapacity = 64, onBufferOverflow = BufferOverflow.DROP_OLDEST)
    val events: SharedFlow<ReSocketEvent> = _events

    private val sockets = TrackingSocketFactory()

    override fun onCreate() {
        super.onCreate()
        val okHttpClient = OkHttpClient.Builder()
            // 只为了演示：记住底层的 socket，好在界面上模拟一次异常断线
            .socketFactory(sockets)
            .build()

        // 长连接跟着进程走，不跟着页面走；页面只决定订阅什么
        reSocket = ReSocket.create(okHttpClient) {
            url(BinanceStreams.URL)
            topicOf = BinanceStreams::topicOf
            // 库默认不打任何日志，要不要打、打到哪里由接入方决定
            listener { event ->
                Log.d("ReSocket", event.toString())
                _events.tryEmit(event)
            }
        }
        reSocket.connect()
    }

    /** 直接关掉底层的 TCP 连接，库会把它当成一次网络异常。 */
    fun simulateConnectionLoss() = sockets.closeAll()
}
