package io.github.usernamehaha.resocket.sample

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewmodel.compose.viewModel
import io.github.usernamehaha.resocket.ConnectionState
import kotlinx.coroutines.flow.StateFlow

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        setContent { MaterialTheme { MainScreen() } }
    }

    override fun onStart() {
        super.onStart()
        // 回到前台时如果正在等待重连，不用等退避走完
        (application as SampleApp).reSocket.reconnectNow()
    }
}

@Composable
private fun MainScreen(viewModel: MainViewModel = viewModel()) {
    val state by viewModel.connectionState.collectAsStateWithLifecycle()
    val selected by viewModel.selected.collectAsStateWithLifecycle()
    val trades by viewModel.trades.collectAsStateWithLifecycle()
    val log by viewModel.log.collectAsStateWithLifecycle()

    Scaffold { padding ->
        LazyColumn(
            modifier = Modifier.fillMaxSize().padding(padding),
            contentPadding = PaddingValues(16.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            item { ConnectionCard(state, viewModel) }
            items(viewModel.symbols) { symbol ->
                SymbolRow(symbol, viewModel.prices.getValue(symbol), selected == symbol) { viewModel.select(symbol) }
            }
            if (selected != null) {
                item { Text("$selected 逐笔成交", style = MaterialTheme.typography.titleSmall) }
                items(trades) { trade ->
                    Text(
                        "${trade.price}    ${trade.quantity}",
                        color = if (trade.isSell) Color(0xFFD32F2F) else Color(0xFF2E7D32),
                        fontFamily = FontFamily.Monospace,
                        style = MaterialTheme.typography.bodySmall,
                    )
                }
            }
            item {
                HorizontalDivider(Modifier.padding(vertical = 8.dp))
                Text("事件", style = MaterialTheme.typography.titleSmall)
            }
            items(log) { line ->
                Text(line, style = MaterialTheme.typography.bodySmall, fontFamily = FontFamily.Monospace)
            }
        }
    }
}

@Composable
private fun ConnectionCard(state: ConnectionState, viewModel: MainViewModel) {
    val description = when (state) {
        is ConnectionState.Disconnected -> "未连接" + (state.cause?.let { "（$it）" } ?: "")
        is ConnectionState.Connecting -> if (state.attempt == 0) "连接中" else "第 ${state.attempt} 次重连中"
        ConnectionState.Connected -> "已连接"
        is ConnectionState.WaitingToReconnect -> "${state.delay} 后第 ${state.attempt} 次重连"
        ConnectionState.Closed -> "已关闭"
    }
    Card(Modifier.fillMaxWidth()) {
        Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            Text(description, style = MaterialTheme.typography.titleMedium)
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                OutlinedButton(onClick = viewModel::toggleConnection) {
                    Text(if (state is ConnectionState.Disconnected) "连接" else "断开")
                }
                OutlinedButton(onClick = viewModel::simulateConnectionLoss, enabled = state == ConnectionState.Connected) {
                    Text("模拟断线")
                }
                OutlinedButton(onClick = viewModel::reconnectNow, enabled = state is ConnectionState.WaitingToReconnect) {
                    Text("立即重连")
                }
            }
        }
    }
}

@Composable
private fun SymbolRow(symbol: String, priceFlow: StateFlow<String?>, selected: Boolean, onClick: () -> Unit) {
    val price by priceFlow.collectAsStateWithLifecycle()
    Card(
        modifier = Modifier.fillMaxWidth().clickable(onClick = onClick),
        colors = if (selected) {
            CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.primaryContainer)
        } else {
            CardDefaults.cardColors()
        },
    ) {
        Row(Modifier.fillMaxWidth().padding(16.dp), horizontalArrangement = Arrangement.SpaceBetween) {
            Text(symbol)
            Text(price ?: "--", fontFamily = FontFamily.Monospace)
        }
    }
}
