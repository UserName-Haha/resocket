package io.github.usernamehaha.wsclient.sample

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import io.github.usernamehaha.wsclient.Backpressure
import io.github.usernamehaha.wsclient.ConnectionState
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.runningFold
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import org.json.JSONObject

data class Trade(val price: String, val quantity: String, val isSell: Boolean)

@OptIn(ExperimentalCoroutinesApi::class)
class MainViewModel(application: Application) : AndroidViewModel(application) {
    private val app = application as SampleApp
    private val client = app.wsClient

    val symbols = listOf("BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT")

    val connectionState: StateFlow<ConnectionState> = client.state

    /**
     * 最新价只关心当前值，用 latest：界面卡顿时中间的价格直接跳过。
     * WhileSubscribed 让界面退到后台 5 秒后停止收集，库随之向服务端退订；回到前台重新收集时再订阅。
     */
    val prices: Map<String, StateFlow<String?>> = symbols.associateWith { symbol ->
        client.subscribe(BinanceStreams.miniTicker(symbol), Backpressure.latest())
            .map { JSONObject(it).getString("c") }
            .stateIn(viewModelScope, SharingStarted.WhileSubscribed(STOP_TIMEOUT_MILLIS), null)
    }

    private val _selected = MutableStateFlow<String?>(null)
    val selected: StateFlow<String?> = _selected

    /** 逐笔成交每一条都有意义，用有界缓冲；切换交易对时 flatMapLatest 取消上一个收集，也就退订了上一个交易对。 */
    val trades: StateFlow<List<Trade>> = _selected
        .flatMapLatest { symbol ->
            if (symbol == null) return@flatMapLatest flowOf(emptyList<Trade>())
            client.subscribe(BinanceStreams.trades(symbol), Backpressure.dropOldest(256))
                .map { message ->
                    val json = JSONObject(message)
                    Trade(json.getString("p"), json.getString("q"), isSell = json.getBoolean("m"))
                }
                .runningFold(emptyList<Trade>()) { recent, trade -> (listOf(trade) + recent).take(MAX_TRADES) }
        }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(STOP_TIMEOUT_MILLIS), emptyList())

    private val _log = MutableStateFlow(emptyList<String>())
    val log: StateFlow<List<String>> = _log

    init {
        viewModelScope.launch {
            val time = SimpleDateFormat("HH:mm:ss", Locale.US)
            app.events.collect { event ->
                _log.update { (listOf("${time.format(Date())}  $event") + it).take(MAX_LOG_LINES) }
            }
        }
    }

    fun select(symbol: String) {
        _selected.update { if (it == symbol) null else symbol }
    }

    fun toggleConnection() {
        if (client.state.value is ConnectionState.Disconnected) client.connect() else client.disconnect()
    }

    fun simulateConnectionLoss() = app.simulateConnectionLoss()

    fun reconnectNow() = client.reconnectNow()

    private companion object {
        const val STOP_TIMEOUT_MILLIS = 5_000L
        const val MAX_TRADES = 30
        const val MAX_LOG_LINES = 100
    }
}
