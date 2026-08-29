package io.github.usernamehaha.wsclient

/**
 * 一个订阅。库不理解具体协议，只负责在合适的时机把 [subscribeMessage] 发出去：
 * 第一个收集者出现时、以及每次重连成功后。
 *
 * @property topic 订阅的唯一标识，和 [WsConfig.topicOf] 从消息里取出的值对应。
 * 同一个 topic 被多处订阅时只向服务端订阅一次，以最先到达的 [Subscription] 为准
 * @property unsubscribeMessage 最后一个收集者离开时发送；为 null 表示协议不需要退订
 */
public class Subscription(
    public val topic: String,
    public val subscribeMessage: String,
    public val unsubscribeMessage: String? = null,
) {
    override fun equals(other: Any?): Boolean =
        other is Subscription &&
            other.topic == topic &&
            other.subscribeMessage == subscribeMessage &&
            other.unsubscribeMessage == unsubscribeMessage

    override fun hashCode(): Int =
        31 * (31 * topic.hashCode() + subscribeMessage.hashCode()) + (unsubscribeMessage?.hashCode() ?: 0)

    override fun toString(): String = "Subscription($topic)"
}
