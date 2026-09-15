package io.github.usernamehaha.wsclient.internal

import java.util.concurrent.CopyOnWriteArrayList
import okio.ByteString

internal class FakeTransport : Transport {
    val connections = CopyOnWriteArrayList<FakeConnection>()
    val latest: FakeConnection get() = connections.last()
    var connectError: Exception? = null

    override fun connect(url: String, listener: TransportListener): Connection {
        require(url.startsWith("ws")) { "不合法的 url：$url" }
        connectError?.let { throw it }
        return FakeConnection(url, listener).also { connections += it }
    }
}

internal class FakeConnection(val url: String, private val listener: TransportListener) : Connection {
    val sent = CopyOnWriteArrayList<String>()
    var sendSucceeds = true
    var closed = false
    var cancelled = false

    override fun send(text: String): Boolean {
        if (sendSucceeds) sent += text
        return sendSucceeds
    }

    override fun close() {
        closed = true
    }

    override fun cancel() {
        cancelled = true
    }

    fun open() = listener.onOpen()
    fun receive(text: String) = listener.onText(text)
    fun receive(bytes: ByteString) = listener.onBinary(bytes)
    fun fail(error: Throwable) = listener.onFailure(error)
    fun serverClose(code: Int, reason: String = "") = listener.onClosed(code, reason)
}
