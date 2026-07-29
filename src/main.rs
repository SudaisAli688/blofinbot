#![allow(dead_code)]
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde::{Serialize, Deserialize};
use chrono::Utc;

// DexScreener API Structs
#[derive(Debug, Deserialize, Clone)]
struct TokenProfile {
    url: String,
    #[serde(rename = "chainId")]
    chain_id: String,
    #[serde(rename = "tokenAddress")]
    token_address: String,
    icon: Option<String>,
    header: Option<String>,
    description: Option<String>,
    links: Option<Vec<TokenLink>>,
}

#[derive(Debug, Deserialize, Clone)]
struct TokenLink {
    #[serde(rename = "type")]
    link_type: Option<String>,
    label: Option<String>,
    url: String,
}

#[derive(Debug, Deserialize, Clone)]
struct TokenBoost {
    url: String,
    #[serde(rename = "chainId")]
    chain_id: String,
    #[serde(rename = "tokenAddress")]
    token_address: String,
    amount: Option<f64>,
    #[serde(rename = "totalAmount")]
    total_amount: Option<f64>,
    icon: Option<String>,
    header: Option<String>,
    description: Option<String>,
    links: Option<Vec<TokenLink>>,
}

#[derive(Debug, Deserialize, Clone)]
struct DexPairsResponse {
    pairs: Option<Vec<DexPair>>,
}

#[derive(Debug, Deserialize, Clone)]
struct DexPair {
    #[serde(rename = "chainId")]
    chain_id: String,
    #[serde(rename = "dexId")]
    dex_id: String,
    url: String,
    #[serde(rename = "pairAddress")]
    pair_address: String,
    #[serde(rename = "baseToken")]
    base_token: TokenInfo,
    #[serde(rename = "priceUsd")]
    price_usd: Option<String>,
    txns: Option<TxnsInfo>,
    volume: Option<VolumeInfo>,
    liquidity: Option<LiquidityInfo>,
    fdv: Option<f64>,
    #[serde(rename = "marketCap")]
    market_cap: Option<f64>,
    #[serde(rename = "pairCreatedAt")]
    pair_created_at: Option<i64>,
    info: Option<PairInfoExtra>,
}

#[derive(Debug, Deserialize, Clone)]
struct TokenInfo {
    address: String,
    name: String,
    symbol: String,
}

#[derive(Debug, Deserialize, Clone)]
struct TxnsInfo {
    m5: Option<TxnsWindow>,
}

#[derive(Debug, Deserialize, Clone)]
struct TxnsWindow {
    buys: i32,
    sells: i32,
}

#[derive(Debug, Deserialize, Clone)]
struct VolumeInfo {
    m5: Option<f64>,
    h1: Option<f64>,
}

#[derive(Debug, Deserialize, Clone)]
struct LiquidityInfo {
    usd: Option<f64>,
}

#[derive(Debug, Deserialize, Clone)]
struct PairInfoExtra {
    #[serde(rename = "imageUrl")]
    image_url: Option<String>,
    websites: Option<Vec<WebsiteInfo>>,
    socials: Option<Vec<SocialInfo>>,
}

#[derive(Debug, Deserialize, Clone)]
struct WebsiteInfo {
    url: String,
}

#[derive(Debug, Deserialize, Clone)]
struct SocialInfo {
    url: String,
    #[serde(rename = "type")]
    social_type: Option<String>,
}

// Bot Configuration
#[derive(Debug, Serialize, Deserialize, Clone)]
struct Config {
    telegram_token: String,
    telegram_chat_id: String,
    poll_interval_seconds: u64,
    min_liquidity_usd: f64,
    min_volume_usd_h1: f64,
    min_volume_usd_m5: f64,
    min_market_cap_usd: f64,
    max_pair_age_hours: f64,
    min_buys_m5: i32,
    min_txns_m5: i32,
    web_port: u16,
}

// App State for Dashboard
#[derive(Debug, Serialize, Clone)]
struct AppState {
    status: String,
    uptime_seconds: u64,
    total_scans: u64,
    total_alerts: u64,
    last_scan_time: String,
    filters: Config,
    alerts: Vec<AlertedToken>,
    logs: Vec<String>,
}

#[derive(Debug, Serialize, Clone, Deserialize)]
struct AlertedToken {
    time_alerted: String,
    token_address: String,
    pair_address: String,
    name: String,
    symbol: String,
    dex_id: String,
    price_usd: String,
    market_cap: f64,
    liquidity_usd: f64,
    volume_m5: f64,
    volume_h1: f64,
    buys_m5: i32,
    sells_m5: i32,
    age_hours: f64,
    url: String,
    websites: Vec<String>,
    socials: Vec<String>,
    is_boosted: bool,
    is_profile: bool,
    image_url: Option<String>,
}

// Utility formatting functions
fn format_number(n: f64) -> String {
    if n >= 1_000_000_000.0 {
        format!("{:.2}B", n / 1_000_000_000.0)
    } else if n >= 1_000_000.0 {
        format!("{:.2}M", n / 1_000_000.0)
    } else if n >= 1_000.0 {
        format!("{:.1}K", n / 1_000.0)
    } else {
        format!("{:.2}", n)
    }
}

fn format_price(p: f64) -> String {
    if p == 0.0 {
        "0.00".to_string()
    } else if p < 0.000001 {
        format!("{:.8}", p)
    } else if p < 0.001 {
        format!("{:.6}", p)
    } else if p < 1.0 {
        format!("{:.4}", p)
    } else {
        format!("{:.2}", p)
    }
}

async fn add_log(state: &Arc<RwLock<AppState>>, msg: &str) {
    let now = Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();
    let formatted_msg = format!("[{}] {}", now, msg);
    println!("{}", formatted_msg);
    let mut w_state = state.write().await;
    w_state.logs.insert(0, formatted_msg);
    if w_state.logs.len() > 50 {
        w_state.logs.truncate(50);
    }
}

async fn send_telegram_alert(
    token: &str,
    chat_id: &str,
    pair: &DexPair,
    is_boosted: bool,
    state: &Arc<RwLock<AppState>>,
) {
    if token.is_empty() || chat_id.is_empty() {
        let msg = format!(
            "📢 [Simulation Alert] Passed Filters: {} ({}) | MCAP: ${} | DEX: {}",
            pair.base_token.name,
            pair.base_token.symbol,
            pair.market_cap.map(format_number).unwrap_or_else(|| "N/A".to_string()),
            pair.dex_id
        );
        add_log(state, &msg).await;
        return;
    }

    let client = reqwest::Client::new();
    let url = format!("https://api.telegram.org/bot{}/sendMessage", token);

    let symbol = &pair.base_token.symbol;
    let name = &pair.base_token.name;
    let mint = &pair.base_token.address;
    let dex_id = &pair.dex_id;
    let price_usd = pair.price_usd.as_deref().unwrap_or("0.0");

    let mcap_str = match pair.market_cap {
        Some(mc) => format!("${}", format_number(mc)),
        None => "N/A".to_string(),
    };

    let liq_str = match &pair.liquidity {
        Some(liq) => match liq.usd {
            Some(liq_usd) => format!("${}", format_number(liq_usd)),
            None => "N/A".to_string(),
        },
        None => "N/A (Bonding Curve)".to_string(),
    };

    let vol_5m = pair.volume.as_ref().and_then(|v| v.m5).unwrap_or(0.0);
    let vol_1h = pair.volume.as_ref().and_then(|v| v.h1).unwrap_or(0.0);

    let buys_5m = pair.txns.as_ref().and_then(|t| t.m5.as_ref()).map(|w| w.buys).unwrap_or(0);
    let sells_5m = pair.txns.as_ref().and_then(|t| t.m5.as_ref()).map(|w| w.sells).unwrap_or(0);

    let dexscreener_url = &pair.url;
    let rugcheck_url = format!("https://rugcheck.xyz/tokens/{}", mint);
    let solscan_url = format!("https://solscan.io/token/{}", mint);
    let jupiter_url = format!("https://jup.ag/swap/SOL-{}", mint);

    let source_badge = if is_boosted {
        "🔥 <b>DEXSCREENER BOOSTED LAUNCH</b>"
    } else {
        "⭐ <b>DEXSCREENER NEW PROFILE</b>"
    };

    let html_message = format!(
        "🚨 <b>SOLANA NEW LAUNCH ALERT</b> 🚨\n\n\
        {}\n\n\
        <b>Token:</b> {} ({})\n\
        <b>Mint:</b> <code>{}</code>\n\n\
        📈 <b>Metrics:</b>\n\
        • <b>Price:</b> ${}\n\
        • <b>Market Cap:</b> {}\n\
        • <b>Liquidity:</b> {}\n\
        • <b>DEX Platform:</b> <code>{}</code>\n\
        • <b>Volume (5m):</b> ${} | <b>(1h):</b> ${}\n\
        • <b>Txns (5m):</b> 🟢 {} Buys | 🔴 {} Sells\n\n\
        🔗 <b>Quick Links:</b>\n\
        👉 <a href=\"{}\">DexScreener</a> | <a href=\"{}\">RugCheck</a>\n\
        👉 <a href=\"{}\">Solscan</a> | <a href=\"{}\">Jupiter Swap</a>",
        source_badge, name, symbol, mint, price_usd, mcap_str, liq_str, dex_id,
        format_number(vol_5m), format_number(vol_1h), buys_5m, sells_5m,
        dexscreener_url, rugcheck_url, solscan_url, jupiter_url
    );

    let payload = serde_json::json!({
        "chat_id": chat_id,
        "text": html_message,
        "parse_mode": "HTML",
        "disable_web_page_preview": false
    });

    match client.post(&url).json(&payload).send().await {
        Ok(res) => {
            if res.status().is_success() {
                add_log(state, &format!("🚀 Telegram alert sent successfully for {} ({})", name, symbol)).await;
            } else {
                let err_text = res.text().await.unwrap_or_default();
                add_log(state, &format!("❌ Failed to send Telegram alert: {}", err_text)).await;
            }
        }
        Err(e) => {
            add_log(state, &format!("❌ Error sending Telegram request: {}", e)).await;
        }
    }
}

// Function to generate the beautiful self-contained HTML dashboard
fn generate_dashboard_html(state: &AppState) -> String {
    let state_json = serde_json::to_string(state).unwrap_or_else(|_| "{}".to_string());

    let template = r#"<!DOCTYPE html>
<html lang="en" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Solana DexScreener Alpha Scanner</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
        body {
            font-family: 'Plus Jakarta Sans', sans-serif;
        }
        .code-font {
            font-family: 'JetBrains Mono', monospace;
        }
        /* Custom scrollbar */
        ::-webkit-scrollbar {
            width: 6px;
            height: 6px;
        }
        ::-webkit-scrollbar-track {
            background: #020617;
        }
        ::-webkit-scrollbar-thumb {
            background: #1e293b;
            border-radius: 4px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: #334155;
        }
    </style>
    <script>
        tailwind.config = {
            darkMode: 'class',
            theme: {
                extend: {
                    colors: {
                        solana: {
                            purple: '#9945FF',
                            green: '#14F195',
                            dark: '#0a051b',
                        }
                    }
                }
            }
        }
    </script>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen flex flex-col selection:bg-solana-green/20 selection:text-solana-green">

    <!-- Top Glow Grid -->
    <div class="absolute top-0 left-1/4 w-96 h-96 bg-solana-purple/10 rounded-full blur-[120px] pointer-events-none"></div>
    <div class="absolute top-10 right-1/4 w-96 h-96 bg-solana-green/10 rounded-full blur-[120px] pointer-events-none"></div>

    <!-- Navigation Header -->
    <header class="border-b border-slate-900 bg-slate-950/80 backdrop-blur-md sticky top-0 z-50 px-6 py-4">
        <div class="max-w-7xl mx-auto flex flex-col md:flex-row justify-between items-center gap-4">
            <div class="flex items-center gap-3">
                <div class="w-10 h-10 rounded-xl bg-gradient-to-tr from-solana-purple to-solana-green p-[2px] shadow-lg shadow-solana-purple/20">
                    <div class="w-full h-full bg-slate-950 rounded-[10px] flex items-center justify-center">
                        <i class="fa-solid fa-radar text-transparent bg-clip-text bg-gradient-to-tr from-solana-purple to-solana-green text-sm"></i>
                    </div>
                </div>
                <div>
                    <h1 class="text-xl font-bold tracking-tight bg-gradient-to-r from-white via-slate-100 to-slate-400 bg-clip-text text-transparent">
                        SOLANA DEX SCREENER SCANNER
                    </h1>
                    <p class="text-xs text-slate-500 font-medium">Real-Time Alpha Discovery Bot & Telegram Alerter</p>
                </div>
            </div>

            <!-- Global Status Bar -->
            <div class="flex flex-wrap items-center gap-3">
                <span class="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 shadow-sm shadow-emerald-500/5">
                    <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
                    ACTIVE SCANNER
                </span>
                <span class="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-semibold bg-blue-500/10 text-blue-400 border border-blue-500/20" id="port-badge">
                    <i class="fa-solid fa-server"></i> PORT 8080
                </span>
                <button onclick="window.location.reload()" class="p-2 rounded-xl bg-slate-900 border border-slate-800 text-slate-400 hover:text-white hover:bg-slate-800 hover:border-slate-700 transition duration-200">
                    <i class="fa-solid fa-arrows-rotate"></i> Refresh
                </button>
            </div>
        </div>
    </header>

    <main class="flex-grow max-w-7xl w-full mx-auto p-6 space-y-6">

        <!-- Stats Cards Row -->
        <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
            <!-- Card 1: Uptime -->
            <div class="bg-slate-900/50 border border-slate-900 rounded-2xl p-5 relative overflow-hidden">
                <div class="absolute -right-2 -bottom-2 opacity-5 text-slate-100 text-7xl"><i class="fa-solid fa-clock"></i></div>
                <div class="text-slate-500 text-xs font-bold uppercase tracking-wider mb-1">Bot Uptime</div>
                <div class="text-2xl font-bold text-white tracking-tight" id="stat-uptime">0s</div>
                <div class="text-[10px] text-slate-500 mt-2 flex items-center gap-1">
                    <span class="w-1.5 h-1.5 rounded-full bg-solana-green"></span> Running continuously
                </div>
            </div>

            <!-- Card 2: Total Scans -->
            <div class="bg-slate-900/50 border border-slate-900 rounded-2xl p-5 relative overflow-hidden">
                <div class="absolute -right-2 -bottom-2 opacity-5 text-slate-100 text-7xl"><i class="fa-solid fa-magnifying-glass-chart"></i></div>
                <div class="text-slate-500 text-xs font-bold uppercase tracking-wider mb-1">Total Scans</div>
                <div class="text-2xl font-bold text-white tracking-tight animate-pulse" id="stat-scans">0</div>
                <div class="text-[10px] text-slate-500 mt-2" id="stat-last-scan">Last: Just now</div>
            </div>

            <!-- Card 3: Total Alerts Sent -->
            <div class="bg-slate-900/50 border border-slate-900 rounded-2xl p-5 relative overflow-hidden">
                <div class="absolute -right-2 -bottom-2 opacity-5 text-slate-100 text-7xl"><i class="fa-solid fa-bell"></i></div>
                <div class="text-slate-500 text-xs font-bold uppercase tracking-wider mb-1">Total Alerts</div>
                <div class="text-2xl font-bold bg-gradient-to-r from-solana-green to-emerald-400 bg-clip-text text-transparent tracking-tight" id="stat-alerts">0</div>
                <div class="text-[10px] text-slate-500 mt-2">Pass rate of incoming pairs</div>
            </div>

            <!-- Card 4: Telegram Status -->
            <div class="bg-slate-900/50 border border-slate-900 rounded-2xl p-5 relative overflow-hidden" id="card-telegram-status">
                <div class="absolute -right-2 -bottom-2 opacity-5 text-slate-100 text-7xl"><i class="fa-brands fa-telegram"></i></div>
                <div class="text-slate-500 text-xs font-bold uppercase tracking-wider mb-1">Telegram Alerter</div>
                <div class="text-lg font-semibold text-white tracking-tight mt-1" id="tg-status-text">Simulated Mode</div>
                <div class="text-[10px] text-slate-400 mt-2" id="tg-status-sub">No Bot Credentials Set</div>
            </div>
        </div>

        <!-- Filters Block -->
        <div class="bg-slate-900/30 border border-slate-900/80 rounded-2xl p-5">
            <h3 class="text-sm font-bold text-slate-300 uppercase tracking-wider mb-4 flex items-center gap-2">
                <i class="fa-solid fa-sliders text-solana-green"></i> Scanner Filter Thresholds
            </h3>
            <div class="grid grid-cols-2 md:grid-cols-5 gap-4">
                <div class="bg-slate-950 p-3.5 rounded-xl border border-slate-900">
                    <span class="block text-slate-500 text-[10px] uppercase font-bold tracking-wider mb-1">Min Liquidity</span>
                    <span class="text-sm font-semibold text-slate-200" id="filter-liq">$0.00</span>
                </div>
                <div class="bg-slate-950 p-3.5 rounded-xl border border-slate-900">
                    <span class="block text-slate-500 text-[10px] uppercase font-bold tracking-wider mb-1">Min Vol 1H</span>
                    <span class="text-sm font-semibold text-slate-200" id="filter-vol">$0.00</span>
                </div>
                <div class="bg-slate-950 p-3.5 rounded-xl border border-slate-900">
                    <span class="block text-slate-500 text-[10px] uppercase font-bold tracking-wider mb-1">Min Vol 5M</span>
                    <span class="text-sm font-semibold text-slate-200" id="filter-vol5m">$0.00</span>
                </div>
                <div class="bg-slate-950 p-3.5 rounded-xl border border-slate-900">
                    <span class="block text-slate-500 text-[10px] uppercase font-bold tracking-wider mb-1">Min Market Cap</span>
                    <span class="text-sm font-semibold text-slate-200" id="filter-mcap">$0.00</span>
                </div>
                <div class="bg-slate-950 p-3.5 rounded-xl border border-slate-900">
                    <span class="block text-slate-500 text-[10px] uppercase font-bold tracking-wider mb-1">Max Age</span>
                    <span class="text-sm font-semibold text-slate-200" id="filter-age">0 hours</span>
                </div>
            </div>
        </div>

        <!-- Split Grid (Table & Terminal Log) -->
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">

            <!-- Alerts Table Section (2/3 width on lg) -->
            <div class="lg:col-span-2 bg-slate-900/50 border border-slate-900 rounded-2xl p-5 flex flex-col min-h-[500px]">
                <div class="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-2 mb-6">
                    <div>
                        <h2 class="text-lg font-bold text-white flex items-center gap-2">
                            <i class="fa-solid fa-circle-exclamation text-solana-purple"></i> Passed Alerts List
                        </h2>
                        <p class="text-xs text-slate-500">Early-stage Solana token listings on DexScreener passing filters</p>
                    </div>
                    <div class="text-xs text-slate-500" id="alerts-count-label">0 alerts loaded</div>
                </div>

                <div class="flex-grow overflow-x-auto">
                    <table class="w-full text-left border-collapse min-w-[650px]">
                        <thead>
                            <tr class="border-b border-slate-800 text-[10px] font-bold text-slate-500 uppercase tracking-wider">
                                <th class="py-3 px-4">Token Info</th>
                                <th class="py-3 px-4">DEX / Platform</th>
                                <th class="py-3 px-4 text-right">Price & MCAP</th>
                                <th class="py-3 px-4 text-right">Liquidity</th>
                                <th class="py-3 px-4 text-right">Volume (5m/1h)</th>
                                <th class="py-3 px-4 text-right">Links</th>
                            </tr>
                        </thead>
                        <tbody id="alerts-table-body" class="divide-y divide-slate-900/80">
                            <!-- Rows will be injected by JavaScript -->
                            <tr>
                                <td colspan="6" class="py-12 text-center text-slate-500 text-sm">
                                    <i class="fa-solid fa-spinner animate-spin text-solana-purple text-lg mb-2 block"></i>
                                    Waiting for first matched Solana listing...
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <!-- Terminal log & Instructions Section (1/3 width on lg) -->
            <div class="space-y-6 flex flex-col">
                <!-- Terminal Console Card -->
                <div class="bg-slate-900/50 border border-slate-900 rounded-2xl p-5 flex flex-col h-[340px]">
                    <div class="flex justify-between items-center mb-3">
                        <h2 class="text-sm font-bold text-slate-300 uppercase tracking-wider flex items-center gap-2">
                            <i class="fa-solid fa-terminal text-solana-green"></i> Bot Console Logs
                        </h2>
                        <span class="w-2.5 h-2.5 rounded-full bg-solana-green animate-ping"></span>
                    </div>
                    <div class="flex-grow bg-slate-950 rounded-xl p-4 border border-slate-900 code-font text-xs overflow-y-auto space-y-2 text-slate-400" id="terminal-logs">
                        <!-- Logs injected by JS -->
                        <div class="text-slate-500">[System] Initializing bot log monitor...</div>
                    </div>
                </div>

                <!-- Setup / Configuration Instructions -->
                <div class="bg-slate-900/50 border border-slate-900 rounded-2xl p-5 flex-grow">
                    <h2 class="text-sm font-bold text-slate-300 uppercase tracking-wider mb-3 flex items-center gap-2">
                        <i class="fa-solid fa-circle-info text-solana-purple"></i> Setup Telegram Alerts
                    </h2>
                    <p class="text-xs text-slate-400 mb-4 leading-relaxed">
                        By default, the bot runs in <b class="text-solana-green">Simulation Mode</b> logging all alerts to this dashboard. Follow these steps to activate real Telegram channel/group notifications:
                    </p>
                    <ol class="space-y-2.5 text-xs text-slate-400 list-decimal pl-4">
                        <li>Open Telegram and message <a href="https://t.me/BotFather" target="_blank" class="text-solana-purple hover:underline font-bold">@BotFather</a></li>
                        <li>Create a bot using <code class="bg-slate-950 px-1 py-0.5 rounded text-solana-green">/newbot</code> and copy the <b>API Token</b></li>
                        <li>Add your bot to a channel or group as an <b>administrator</b></li>
                        <li>Get the Chat ID (use <a href="https://t.me/RawDataBot" target="_blank" class="text-solana-purple hover:underline">@RawDataBot</a> in your group)</li>
                        <li>Edit <code class="bg-slate-950 px-1.5 py-0.5 rounded text-slate-200">config.json</code> in this workspace with your tokens</li>
                        <li>Restart the scanner to begin broadcasting instantly!</li>
                    </ol>
                </div>
            </div>

        </div>

    </main>

    <footer class="border-t border-slate-900 bg-slate-950/40 py-6 px-6 text-center text-xs text-slate-500">
        <div class="max-w-7xl mx-auto flex flex-col sm:flex-row justify-between items-center gap-4">
            <div>Solana DexScreener Alpha Scanner © 2026. Made with Rust 🦀</div>
            <div class="flex gap-4">
                <span class="hover:text-slate-400"><i class="fa-solid fa-gauge-high"></i> High-Speed Polling</span>
                <span class="hover:text-slate-400"><i class="fa-solid fa-shield-halved"></i> Rate-Limit Compliant</span>
            </div>
        </div>
    </footer>

    <!-- EMBEDDED STATE (Used when static html file is loaded directly) -->
    <script id="embedded-state" type="application/json">
        __STATE_JSON__
    </script>

    <!-- Main Dashboard Controller JS -->
    <script>
        const fallbackState = {};

        function getUptimeString(seconds) {
            const h = Math.floor(seconds / 3600);
            const m = Math.floor((seconds % 3600) / 60);
            const s = seconds % 60;
            let parts = [];
            if (h > 0) parts.push(h + "h");
            if (m > 0) parts.push(m + "m");
            parts.push(s + "s");
            return parts.join(" ");
        }

        function formatCompactNumber(n) {
            if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
            if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
            if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
            return n.toFixed(2);
        }

        function formatCompactPrice(p) {
            if (p === 0) return '0.00';
            if (p < 0.000001) return p.toFixed(8);
            if (p < 0.001) return p.toFixed(6);
            if (p < 1.0) return p.toFixed(4);
            return p.toFixed(2);
        }

        function copyAddress(address, btnId) {
            navigator.clipboard.writeText(address).then(() => {
                const btn = document.getElementById(btnId);
                const originalHtml = btn.innerHTML;
                btn.innerHTML = '<i class="fa-solid fa-check text-solana-green"></i>';
                setTimeout(() => {
                    btn.innerHTML = originalHtml;
                }, 1500);
            });
        }

        function updateDashboard(state) {
            if (!state || Object.keys(state).length === 0) return;

            // Stats
            document.getElementById('stat-uptime').innerText = getUptimeString(state.uptime_seconds || 0);
            document.getElementById('stat-scans').innerText = state.total_scans || 0;
            document.getElementById('stat-alerts').innerText = state.total_alerts || 0;
            document.getElementById('stat-last-scan').innerText = "Last: " + (state.last_scan_time || "N/A");

            // Filters
            if (state.filters) {
                document.getElementById('filter-liq').innerText = "$" + formatCompactNumber(state.filters.min_liquidity_usd);
                document.getElementById('filter-vol').innerText = "$" + formatCompactNumber(state.filters.min_volume_usd_h1);
                document.getElementById('filter-vol5m').innerText = "$" + formatCompactNumber(state.filters.min_volume_usd_m5);
                document.getElementById('filter-mcap').innerText = "$" + formatCompactNumber(state.filters.min_market_cap_usd);
                document.getElementById('filter-age').innerText = state.filters.max_pair_age_hours + " hours";

                const hasTg = state.filters.telegram_token && state.filters.telegram_token.trim().length > 0;
                const tgCard = document.getElementById('card-telegram-status');
                const tgText = document.getElementById('tg-status-text');
                const tgSub = document.getElementById('tg-status-sub');
                
                if (hasTg) {
                    tgText.innerText = "Enabled & Broadcasting";
                    tgText.className = "text-md font-semibold text-emerald-400 tracking-tight mt-1";
                    tgSub.innerText = "Chat ID: " + (state.filters.telegram_chat_id || 'N/A');
                    tgCard.className = "bg-slate-900/50 border border-emerald-900/40 rounded-2xl p-5 relative overflow-hidden";
                } else {
                    tgText.innerText = "Simulation Mode";
                    tgText.className = "text-md font-semibold text-yellow-400 tracking-tight mt-1";
                    tgSub.innerText = "Logs saved here. Edit config.json to live-feed";
                    tgCard.className = "bg-slate-900/50 border border-slate-900 rounded-2xl p-5 relative overflow-hidden";
                }
            }

            // Logs
            if (state.logs) {
                const consoleDiv = document.getElementById('terminal-logs');
                consoleDiv.innerHTML = state.logs.map(log => {
                    let colorClass = "text-slate-400";
                    if (log.includes("[Simulation Alert]") || log.includes("alert sent")) colorClass = "text-solana-green font-semibold";
                    else if (log.includes("❌")) colorClass = "text-rose-400";
                    else if (log.includes("⚠️") || log.includes("[WARN]")) colorClass = "text-yellow-400";
                    else if (log.includes("[SCAN]")) colorClass = "text-cyan-400";
                    
                    return '<div class="' + colorClass + '">' + log + '</div>';
                }).join("");
            }

            // Alerts table
            const tableBody = document.getElementById('alerts-table-body');
            document.getElementById('alerts-count-label').innerText = (state.alerts || []).length + " alerts loaded";

            if (!state.alerts || state.alerts.length === 0) {
                tableBody.innerHTML = `
                    <tr>
                        <td colspan="6" class="py-12 text-center text-slate-500 text-sm">
                            <i class="fa-solid fa-radar animate-pulse text-solana-purple text-2xl mb-2 block"></i>
                            Listening for new Solana launches passing filter thresholds...
                        </td>
                    </tr>`;
                return;
            }

            tableBody.innerHTML = state.alerts.map((alert, index) => {
                const ageMinutes = Math.round(alert.age_hours * 60);
                const ageStr = ageMinutes < 60 ? ageMinutes + "m ago" : alert.age_hours.toFixed(1) + "h ago";
                
                // Platforms and badges
                let badgeClass = "bg-slate-950 text-slate-400 border-slate-800";
                if (alert.dex_id === "raydium") badgeClass = "bg-blue-500/10 text-blue-400 border-blue-500/20";
                else if (alert.dex_id === "pumpfun") badgeClass = "bg-pink-500/10 text-pink-400 border-pink-500/20";
                else if (alert.dex_id === "meteora") badgeClass = "bg-emerald-500/10 text-emerald-400 border-emerald-500/20";
                else if (alert.dex_id === "orca") badgeClass = "bg-teal-500/10 text-teal-400 border-teal-500/20";

                const isBoostedBadge = alert.is_boosted 
                    ? '<span class="inline-flex items-center px-1.5 py-0.5 rounded text-[9px] font-semibold bg-amber-500/20 text-amber-400 border border-amber-500/30 ml-1">🔥 BOOST</span>' 
                    : "";

                const liquidityDisplay = alert.liquidity_usd > 0 
                    ? "$" + formatCompactNumber(alert.liquidity_usd)
                    : '<span class="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-semibold bg-purple-500/10 text-purple-400 border border-purple-500/20">Bonding Curve</span>';

                const buysSellsDisplay = alert.buys_m5 > 0 || alert.sells_m5 > 0
                    ? '<div class="text-[10px] text-slate-500 mt-1">🟢 ' + alert.buys_m5 + ' / 🔴 ' + alert.sells_m5 + ' tx</div>'
                    : '';

                const tokenLogo = alert.image_url 
                    ? '<img src="' + alert.image_url + '" class="w-8 h-8 rounded-lg object-cover border border-slate-800">'
                    : '<div class="w-8 h-8 rounded-lg bg-gradient-to-tr from-solana-purple to-solana-green flex items-center justify-center text-xs font-bold text-white shadow">' + alert.symbol.substring(0,2) + '</div>';

                const btnCopyId = 'copy-btn-' + index;

                return `
                    <tr class="hover:bg-slate-900/30 group transition">
                        <!-- Token Column -->
                        <td class="py-4 px-4">
                            <div class="flex items-center gap-3">
                                ${tokenLogo}
                                <div class="min-w-0">
                                    <div class="flex items-center gap-1">
                                        <span class="font-bold text-white group-hover:text-solana-green transition duration-150">${alert.symbol}</span>
                                        <span class="text-xs text-slate-500 truncate hidden sm:inline max-w-[100px]">${alert.name}</span>
                                        ${isBoostedBadge}
                                    </div>
                                    <div class="flex items-center gap-1 text-[10px] text-slate-500 mt-0.5 code-font">
                                        <span class="truncate max-w-[110px]">${alert.token_address}</span>
                                        <button id="${btnCopyId}" onclick="copyAddress('${alert.token_address}', '${btnCopyId}')" class="hover:text-slate-300 focus:outline-none p-0.5">
                                            <i class="fa-regular fa-copy"></i>
                                        </button>
                                    </div>
                                </div>
                            </div>
                        </td>

                        <!-- DEX Column -->
                        <td class="py-4 px-4 text-xs font-semibold">
                            <span class="inline-flex items-center px-2 py-0.5 rounded border capitalize ${badgeClass}">
                                ${alert.dex_id}
                            </span>
                            <div class="text-[10px] text-slate-500 mt-1">${ageStr}</div>
                        </td>

                        <!-- Price & MCAP -->
                        <td class="py-4 px-4 text-right">
                            <div class="font-semibold text-slate-200 text-sm code-font">$${formatCompactPrice(parseFloat(alert.price_usd))}</div>
                            <div class="text-[10px] text-slate-500 mt-1">MCAP: $${formatCompactNumber(alert.market_cap)}</div>
                        </td>

                        <!-- Liquidity -->
                        <td class="py-4 px-4 text-right font-medium text-slate-300 text-sm">
                            ${liquidityDisplay}
                        </td>

                        <!-- Volume -->
                        <td class="py-4 px-4 text-right">
                            <div class="text-xs font-semibold text-slate-300">5M: $${formatCompactNumber(alert.volume_m5)}</div>
                            <div class="text-[10px] text-slate-500 mt-0.5">1H: $${formatCompactNumber(alert.volume_h1)}</div>
                            ${buysSellsDisplay}
                        </td>

                        <!-- Quick Links -->
                        <td class="py-4 px-4 text-right">
                            <div class="flex items-center justify-end gap-1.5">
                                <a href="${alert.url}" target="_blank" title="DexScreener" class="w-8 h-8 rounded-lg bg-slate-900 hover:bg-slate-800 text-slate-300 hover:text-white border border-slate-800 hover:border-slate-700 flex items-center justify-center transition">
                                    <i class="fa-solid fa-chart-line text-xs"></i>
                                </a>
                                <a href="https://jup.ag/swap/SOL-${alert.token_address}" target="_blank" title="Jupiter Swap" class="w-8 h-8 rounded-lg bg-slate-900 hover:bg-slate-800 text-slate-300 hover:text-white border border-slate-800 hover:border-slate-700 flex items-center justify-center transition">
                                    <i class="fa-solid fa-rotate text-xs"></i>
                                </a>
                                <a href="https://rugcheck.xyz/tokens/${alert.token_address}" target="_blank" title="RugCheck Sec" class="w-8 h-8 rounded-lg bg-slate-900 hover:bg-slate-800 text-slate-300 hover:text-white border border-slate-800 hover:border-slate-700 flex items-center justify-center transition">
                                    <i class="fa-solid fa-shield-halved text-xs"></i>
                                </a>
                                <a href="https://solscan.io/token/${alert.token_address}" target="_blank" title="Solscan Explorer" class="w-8 h-8 rounded-lg bg-slate-900 hover:bg-slate-800 text-slate-300 hover:text-white border border-slate-800 hover:border-slate-700 flex items-center justify-center transition">
                                    <i class="fa-solid fa-magnifying-glass text-xs"></i>
                                </a>
                            </div>
                        </td>
                    </tr>
                `;
            }).join("");
        }

        // Initialize from embedded state
        const embeddedDataElement = document.getElementById('embedded-state');
        if (embeddedDataElement && embeddedDataElement.textContent.trim().length > 5) {
            try {
                const embeddedData = JSON.parse(embeddedDataElement.textContent);
                updateDashboard(embeddedData);
            } catch (e) {
                console.error("Failed to parse embedded state", e);
            }
        }

        // Setup API Poller
        async function fetchLatestData() {
            try {
                const response = await fetch('/api/data');
                if (response.ok) {
                    const data = await response.json();
                    updateDashboard(data);
                    document.getElementById('port-badge').className = "inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-semibold bg-blue-500/10 text-blue-400 border border-blue-500/20";
                    document.getElementById('port-badge').innerHTML = '<i class="fa-solid fa-server"></i> PORT 8080 (LIVE)';
                }
            } catch (e) {
                // This means the background web server isn't serving, or we are loading statically
                document.getElementById('port-badge').className = "inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-semibold bg-slate-800 text-slate-400 border border-slate-700";
                document.getElementById('port-badge').innerHTML = '<i class="fa-solid fa-file-code"></i> STATIC VIEW';
            }
        }

        // Initial fetch and start interval
        fetchLatestData();
        setInterval(fetchLatestData, 4000);
    </script>
</body>
</html>"#;

    template.replace("__STATE_JSON__", &state_json)
}

// Function to generate and write the static dashboard.html file
fn write_dashboard(state: &AppState) {
    let html = generate_dashboard_html(state);
    if let Err(e) = std::fs::write("dashboard.html", html) {
        eprintln!("[File Logger] Error writing dashboard.html: {}", e);
    }
}

// Function to generate a simple Markdown snapshot of alerts
fn generate_markdown_dashboard(state: &AppState) -> String {
    let mut md = String::new();
    md.push_str("# 🚀 Solana DexScreener Scanner Dashboard\n\n");
    md.push_str(&format!("**Status:** Active 🟢 | **Uptime:** {}s | **Total Scans:** {} | **Total Alerts:** {}\n", 
                         state.uptime_seconds, state.total_scans, state.total_alerts));
    md.push_str(&format!("**Last Scan Time:** {} UTC\n\n", state.last_scan_time));
    
    md.push_str("## ⚙️ Filter Configuration\n");
    md.push_str(&format!("- **Min Liquidity:** ${}\n", format_number(state.filters.min_liquidity_usd)));
    md.push_str(&format!("- **Min Volume 1H:** ${}\n", format_number(state.filters.min_volume_usd_h1)));
    md.push_str(&format!("- **Min Volume 5M:** ${}\n", format_number(state.filters.min_volume_usd_m5)));
    md.push_str(&format!("- **Min Market Cap:** ${}\n", format_number(state.filters.min_market_cap_usd)));
    md.push_str(&format!("- **Max Pair Age:** {} hours\n\n", state.filters.max_pair_age_hours));

    md.push_str("## 🚨 Recent Alerts\n");
    if state.alerts.is_empty() {
        md.push_str("*Waiting for first matched Solana listing...*\n");
    } else {
        md.push_str("| Token | DEX | Price | MCAP | Liquidity | Vol (5m/1h) | Age | Quick Links |\n");
        md.push_str("| --- | --- | --- | --- | --- | --- | --- | --- |\n");
        for alert in &state.alerts {
            let liq_str = if alert.liquidity_usd > 0.0 {
                format!("${}", format_number(alert.liquidity_usd))
            } else {
                "Bonding Curve".to_string()
            };
            
            let links = format!(
                "[DexScreener]({}) \\| [Jupiter](https://jup.ag/swap/SOL-{}) \\| [RugCheck](https://rugcheck.xyz/tokens/{})",
                alert.url, alert.token_address, alert.token_address
            );

            let price_parsed: f64 = alert.price_usd.parse().unwrap_or(0.0);

            md.push_str(&format!(
                "| **{}** ({})<br>`{}` | `{}` | ${} | ${} | {} | ${} / ${} | {:.1}h | {} |\n",
                alert.symbol, alert.name, alert.token_address, alert.dex_id,
                format_price(price_parsed),
                format_number(alert.market_cap), liq_str,
                format_number(alert.volume_m5), format_number(alert.volume_h1),
                alert.age_hours, links
            ));
        }
    }

    md.push_str("\n## 📋 Console Logs\n");
    md.push_str("```text\n");
    for log in state.logs.iter().take(15) {
        md.push_str(&format!("{}\n", log));
    }
    md.push_str("```\n");

    md
}

fn write_markdown_dashboard_file(state: &AppState) {
    let md = generate_markdown_dashboard(state);
    if let Err(e) = std::fs::write("dashboard.md", md) {
        eprintln!("[File Logger] Error writing dashboard.md: {}", e);
    }
}

// Simple Web Server to serve the dashboard HTML and data.json
async fn start_web_server(port: u16, state: Arc<RwLock<AppState>>) {
    let addr = format!("0.0.0.0:{}", port);
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => {
            println!("[Web Server] Listening on http://{}", addr);
            l
        }
        Err(e) => {
            eprintln!("[Web Server] Failed to bind to port {}: {}", port, e);
            return;
        }
    };

    loop {
        let (mut socket, _) = match listener.accept().await {
            Ok(s) => s,
            Err(_) => continue,
        };

        let state_clone = state.clone();
        tokio::spawn(async move {
            let mut buffer = [0; 1024];
            let n = match socket.read(&mut buffer).await {
                Ok(n) if n > 0 => n,
                _ => return,
            };

            let request = String::from_utf8_lossy(&buffer[..n]);
            let first_line = request.lines().next().unwrap_or("");

            let (status, content_type, body) = if first_line.contains("GET /api/data") || first_line.contains("GET /data.json") {
                let r_state = state_clone.read().await;
                let json = serde_json::to_string(&*r_state).unwrap_or_else(|_| "{}".to_string());
                ("HTTP/1.1 200 OK", "application/json", json)
            } else {
                match tokio::fs::read_to_string("dashboard.html").await {
                    Ok(html) => ("HTTP/1.1 200 OK", "text/html; charset=utf-8", html),
                    Err(_) => ("HTTP/1.1 404 NOT FOUND", "text/plain", "Dashboard file not found. Wait for first scan to generate it.".to_string()),
                }
            };

            let response = format!(
                "{}\r\nContent-Type: {}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n{}",
                status, content_type, body.len(), body
            );

            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.flush().await;
        });
    }
}

// Persistent History Managers
fn load_history() -> HashSet<String> {
    if let Ok(data) = std::fs::read_to_string("alert_history.json") {
        if let Ok(list) = serde_json::from_str::<Vec<String>>(&data) {
            println!("[Boot] Loaded {} alerted tokens from alert_history.json", list.len());
            return list.into_iter().collect();
        }
    }
    HashSet::new()
}

fn save_history(history: &HashSet<String>) {
    let list: Vec<String> = history.iter().cloned().collect();
    if let Ok(data) = serde_json::to_string(&list) {
        let _ = std::fs::write("alert_history.json", data);
    }
}

#[tokio::main]
async fn main() {
    println!("🦀 Starting Solana DexScreener Alpha Scanner Bot...");

    // 1. Load configuration
    let config_data = std::fs::read_to_string("config.json").unwrap_or_else(|_| {
        println!("[Config] config.json not found! Creating default settings...");
        let default_cfg = r#"{
  "telegram_token": "",
  "telegram_chat_id": "",
  "poll_interval_seconds": 15,
  "min_liquidity_usd": 1000.0,
  "min_volume_usd_h1": 500.0,
  "min_volume_usd_m5": 100.0,
  "min_market_cap_usd": 5000.0,
  "max_pair_age_hours": 24.0,
  "min_buys_m5": 0,
  "min_txns_m5": 0,
  "web_port": 8080
}"#;
        std::fs::write("config.json", default_cfg).expect("Failed to write default config.json");
        default_cfg.to_string()
    });

    let config: Config = serde_json::from_str(&config_data).expect("Error parsing config.json. Ensure format is correct.");
    println!("[Config] Configuration Loaded. Web dashboard on port {}", config.web_port);

    // 2. Load alert history
    let mut alert_history = load_history();
    let mut checked_cache: HashMap<String, Instant> = HashMap::new(); // Token cache to avoid API hitting

    // 3. Initialize App State
    let start_time = Instant::now();
    let app_state = Arc::new(RwLock::new(AppState {
        status: "Active".to_string(),
        uptime_seconds: 0,
        total_scans: 0,
        total_alerts: alert_history.len() as u64,
        last_scan_time: "Never".to_string(),
        filters: config.clone(),
        alerts: Vec::new(),
        logs: vec![format!("[Boot] Bot started successfully. Loaded {} alerted history tokens.", alert_history.len())],
    }));

    // Generate initial static file
    {
        let r_state = app_state.read().await;
        write_dashboard(&*r_state);
        write_markdown_dashboard_file(&*r_state);
    }

    // 4. Spawn the web server in a background task
    let web_state_clone = app_state.clone();
    let web_port = config.web_port;
    tokio::spawn(async move {
        start_web_server(web_port, web_state_clone).await;
    });

    // 5. Main Poll Loop
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        .build()
        .unwrap();

    let mut tick_counter = 0;
    loop {
        tick_counter += 1;
        let loop_start = Instant::now();

        // Update App Uptime and general status
        {
            let mut w_state = app_state.write().await;
            w_state.uptime_seconds = start_time.elapsed().as_secs();
            w_state.total_scans += 1;
            w_state.last_scan_time = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        }

        // Periodically clear expired check cache (older than 30 minutes)
        checked_cache.retain(|_, last_checked| {
            last_checked.elapsed() < Duration::from_secs(1800)
        });

        add_log(&app_state, &format!("🔍 [SCAN #{}] Fetching candidates from DexScreener...", tick_counter)).await;

        // Fetch latest profiles and boosted tokens
        let mut candidates: HashSet<String> = HashSet::new();
        let mut source_map: HashMap<String, (bool, bool)> = HashMap::new(); // token_address -> (is_boosted, is_profile)

        // A. Token Profiles Endpoint
        match client.get("https://api.dexscreener.com/token-profiles/latest/v1").send().await {
            Ok(res) => {
                if let Ok(profiles) = res.json::<Vec<TokenProfile>>().await {
                    for p in profiles {
                        if p.chain_id == "solana" {
                            candidates.insert(p.token_address.clone());
                            let entry = source_map.entry(p.token_address).or_insert((false, false));
                            entry.1 = true;
                        }
                    }
                }
            }
            Err(e) => {
                add_log(&app_state, &format!("⚠️ Failed to fetch token-profiles: {}", e)).await;
            }
        }

        // B. Token Boosts Endpoint
        match client.get("https://api.dexscreener.com/token-boosts/latest/v1").send().await {
            Ok(res) => {
                if let Ok(boosts) = res.json::<Vec<TokenBoost>>().await {
                    for b in boosts {
                        if b.chain_id == "solana" {
                            candidates.insert(b.token_address.clone());
                            let entry = source_map.entry(b.token_address).or_insert((false, false));
                            entry.0 = true;
                        }
                    }
                }
            }
            Err(e) => {
                add_log(&app_state, &format!("⚠️ Failed to fetch token-boosts: {}", e)).await;
            }
        }

        add_log(&app_state, &format!("📋 Found {} unique Solana token candidates", candidates.len())).await;

        // Process Candidates
        let mut checked_in_this_loop = 0;
        for token_addr in candidates {
            // Rate limit & speed check: skip if already alerted or checked recently
            if alert_history.contains(&token_addr) {
                continue;
            }

            if checked_cache.contains_key(&token_addr) {
                continue;
            }

            // Stagger requests to avoid getting rate limited! (DexScreener allows 300 req/min for pair/token details)
            if checked_in_this_loop > 0 {
                tokio::time::sleep(Duration::from_millis(400)).await;
            }
            checked_in_this_loop += 1;

            // Fetch Token Pair Data
            let token_details_url = format!("https://api.dexscreener.com/latest/dex/tokens/{}", token_addr);
            match client.get(&token_details_url).send().await {
                Ok(res) => {
                    if let Ok(resp_data) = res.json::<DexPairsResponse>().await {
                        if let Some(mut pairs) = resp_data.pairs {
                            // Sort pairs so Raydium/Orca with liquidity is preferred over raw pumpfun curve
                            pairs.sort_by(|a, b| {
                                let a_liq = a.liquidity.as_ref().and_then(|l| l.usd).unwrap_or(0.0);
                                let b_liq = b.liquidity.as_ref().and_then(|l| l.usd).unwrap_or(0.0);
                                b_liq.partial_cmp(&a_liq).unwrap_or(std::cmp::Ordering::Equal)
                            });

                            let sol_pairs: Vec<DexPair> = pairs.into_iter().filter(|p| p.chain_id == "solana").collect();

                            if let Some(best_pair) = sol_pairs.first() {
                                // Apply Filters
                                let price_val = best_pair.price_usd.as_deref().unwrap_or("0.0").parse::<f64>().unwrap_or(0.0);
                                let mcap_val = best_pair.market_cap.unwrap_or(best_pair.fdv.unwrap_or(0.0));
                                let liq_val = best_pair.liquidity.as_ref().and_then(|l| l.usd).unwrap_or(0.0);
                                let vol_h1_val = best_pair.volume.as_ref().and_then(|v| v.h1).unwrap_or(0.0);
                                let vol_m5_val = best_pair.volume.as_ref().and_then(|v| v.m5).unwrap_or(0.0);
                                
                                let buys_m5_val = best_pair.txns.as_ref().and_then(|t| t.m5.as_ref()).map(|w| w.buys).unwrap_or(0);
                                let sells_m5_val = best_pair.txns.as_ref().and_then(|t| t.m5.as_ref()).map(|w| w.sells).unwrap_or(0);
                                let txns_m5_val = buys_m5_val + sells_m5_val;

                                let now_ms = Utc::now().timestamp_millis();
                                let age_hours = if let Some(created_at) = best_pair.pair_created_at {
                                    let diff = ((now_ms - created_at) as f64) / 1000.0 / 3600.0;
                                    if diff < 0.0 { 0.0 } else { diff }
                                } else {
                                    0.0
                                };

                                // Check Filters
                                let mut passes = true;
                                let mut filter_reason = String::new();

                                // If it is a normal pool (raydium etc) and has explicit liquidity, we check min_liquidity
                                // If it is pump.fun and doesn't have explicit liquidity, we skip liquidity check to allow early pumps!
                                if best_pair.dex_id != "pumpfun" && liq_val < config.min_liquidity_usd {
                                    passes = false;
                                    filter_reason = format!("Liquidity (${:.0}) < min (${:.0})", liq_val, config.min_liquidity_usd);
                                }
                                if mcap_val < config.min_market_cap_usd {
                                    passes = false;
                                    filter_reason = format!("MCAP (${:.0}) < min (${:.0})", mcap_val, config.min_market_cap_usd);
                                }
                                if vol_h1_val < config.min_volume_usd_h1 {
                                    passes = false;
                                    filter_reason = format!("Vol 1H (${:.0}) < min (${:.0})", vol_h1_val, config.min_volume_usd_h1);
                                }
                                if vol_m5_val < config.min_volume_usd_m5 {
                                    passes = false;
                                    filter_reason = format!("Vol 5M (${:.0}) < min (${:.0})", vol_m5_val, config.min_volume_usd_m5);
                                }
                                if age_hours > config.max_pair_age_hours && best_pair.pair_created_at.is_some() {
                                    passes = false;
                                    filter_reason = format!("Age ({:.1}h) > max ({}h)", age_hours, config.max_pair_age_hours);
                                }
                                if buys_m5_val < config.min_buys_m5 {
                                    passes = false;
                                    filter_reason = format!("Buys 5M ({}) < min ({})", buys_m5_val, config.min_buys_m5);
                                }
                                if txns_m5_val < config.min_txns_m5 {
                                    passes = false;
                                    filter_reason = format!("Txns 5M ({}) < min ({})", txns_m5_val, config.min_txns_m5);
                                }

                                if passes {
                                    // Alert & Record
                                    alert_history.insert(token_addr.clone());
                                    save_history(&alert_history);

                                    let sources = source_map.get(&token_addr).cloned().unwrap_or((false, false));
                                    
                                    // Prepare Web Alert item
                                    let websites = best_pair.info.as_ref()
                                        .and_then(|inf| inf.websites.as_ref())
                                        .map(|webs| webs.iter().map(|w| w.url.clone()).collect())
                                        .unwrap_or_default();
                                        
                                    let socials = best_pair.info.as_ref()
                                        .and_then(|inf| inf.socials.as_ref())
                                        .map(|socs| socs.iter().map(|s| s.url.clone()).collect())
                                        .unwrap_or_default();

                                    let image_url = best_pair.info.as_ref().and_then(|inf| inf.image_url.clone());

                                    let alerted_token = AlertedToken {
                                        time_alerted: Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                                        token_address: token_addr.clone(),
                                        pair_address: best_pair.pair_address.clone(),
                                        name: best_pair.base_token.name.clone(),
                                        symbol: best_pair.base_token.symbol.clone(),
                                        dex_id: best_pair.dex_id.clone(),
                                        price_usd: format_price(price_val),
                                        market_cap: mcap_val,
                                        liquidity_usd: liq_val,
                                        volume_m5: vol_m5_val,
                                        volume_h1: vol_h1_val,
                                        buys_m5: buys_m5_val,
                                        sells_m5: sells_m5_val,
                                        age_hours,
                                        url: best_pair.url.clone(),
                                        websites,
                                        socials,
                                        is_boosted: sources.0,
                                        is_profile: sources.1,
                                        image_url,
                                    };

                                    // Push and limit size to 100 alerts on dashboard
                                    {
                                        let mut w_state = app_state.write().await;
                                        w_state.alerts.insert(0, alerted_token);
                                        if w_state.alerts.len() > 100 {
                                            w_state.alerts.truncate(100);
                                        }
                                        w_state.total_alerts = alert_history.len() as u64;
                                    }

                                    // Send Alert to Telegram (runs simulation block if credentials are empty)
                                    send_telegram_alert(&config.telegram_token, &config.telegram_chat_id, best_pair, sources.0, &app_state).await;

                                    // Write/regenerate dashboards
                                    let r_state = app_state.read().await;
                                    write_dashboard(&*r_state);
                                    write_markdown_dashboard_file(&*r_state);
                                } else {
                                    // Add to checked cache so we don't query again for some time
                                    checked_cache.insert(token_addr.clone(), Instant::now());
                                    println!("[Filter] Candidate {} failed filters: {}", token_addr, filter_reason);
                                }
                            }
                        }
                    }
                }
                Err(_) => {
                    // Fail gracefully, maybe rate limit. Stagger a bit more.
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }

        // Update dashboard file even if no alerts were sent (for uptime/scan count updates)
        {
            let r_state = app_state.read().await;
            write_dashboard(&*r_state);
            write_markdown_dashboard_file(&*r_state);
        }

        // Loop sleep interval minus processing duration
        let elapsed = loop_start.elapsed();
        let sleep_duration = Duration::from_secs(config.poll_interval_seconds);
        if elapsed < sleep_duration {
            tokio::time::sleep(sleep_duration - elapsed).await;
        }
    }
}
