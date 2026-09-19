package io.github.usernamehaha.resocket.sample

import java.net.InetAddress
import java.net.Socket
import java.util.concurrent.CopyOnWriteArrayList
import javax.net.SocketFactory

/** 记住创建过的 socket，用来在演示里模拟网络异常。OkHttp 只会调用无参的 createSocket()。 */
class TrackingSocketFactory : SocketFactory() {
    private val delegate = getDefault()
    private val sockets = CopyOnWriteArrayList<Socket>()

    fun closeAll() {
        sockets.forEach { runCatching { it.close() } }
        sockets.clear()
    }

    private fun track(socket: Socket): Socket {
        sockets.removeAll { it.isClosed }
        sockets += socket
        return socket
    }

    override fun createSocket(): Socket = track(delegate.createSocket())

    override fun createSocket(host: String, port: Int): Socket = track(delegate.createSocket(host, port))

    override fun createSocket(host: String, port: Int, localHost: InetAddress, localPort: Int): Socket =
        track(delegate.createSocket(host, port, localHost, localPort))

    override fun createSocket(host: InetAddress, port: Int): Socket = track(delegate.createSocket(host, port))

    override fun createSocket(address: InetAddress, port: Int, localAddress: InetAddress, localPort: Int): Socket =
        track(delegate.createSocket(address, port, localAddress, localPort))
}
