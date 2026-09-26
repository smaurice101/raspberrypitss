<!DOCTYPE html>
<html lang="en" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
<link rel="icon" type="image/png" href="../img/qsicon.png">	
	
    <title>QuantStream AI Terminal | Real-Time Alpha Engine</title>
    <!-- Tailwind CSS -->
    <script src="https://cdn.tailwindcss.com"></script>
    <script>
        tailwind.config = {
            darkMode: 'class',
            theme: {
                extend: {
                    colors: {
                        darkbg: '#0b0f19',
                        cardbg: 'rgba(15, 23, 42, 0.75)',
                        cardborder: 'rgba(255, 255, 255, 0.08)',
                        accentblue: '#3b82f6',
                        accentgreen: '#10b981',
                        accentred: '#ef4444',
                        accentpurple: '#8b5cf6',
                        accentcyan: '#06b6d4'
                    },
                    fontFamily: {
                        mono: ['JetBrains Mono', 'Fira Code', 'Monaco', 'monospace'],
                        sans: ['Inter', 'system-ui', 'sans-serif']
                    }
                }
            }
        }
    </script>
    <!-- Chart.js -->
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <!-- Lucide Icons -->
    <script src="https://unpkg.com/lucide@latest"></script>
    <!-- Google Fonts -->
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">

    <style>
        body {
            background-color: #0b0f19;
            color: #f3f4f6;
            font-family: 'Inter', sans-serif;
            overflow-x: hidden;
        }
        .glass-panel {
            background: rgba(15, 23, 42, 0.75);
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            border: 1px solid rgba(255, 255, 255, 0.08);
        }
        .glass-panel:hover {
            border-color: rgba(255, 255, 255, 0.15);
        }
        .glow-green {
            box-shadow: 0 0 18px rgba(16, 185, 129, 0.3);
            border-color: rgba(16, 185, 129, 0.6) !important;
        }
        .glow-red {
            box-shadow: 0 0 18px rgba(239, 68, 68, 0.3);
            border-color: rgba(239, 68, 68, 0.6) !important;
        }
        .glow-cyan {
            box-shadow: 0 0 15px rgba(6, 182, 212, 0.3);
        }
        ::-webkit-scrollbar {
            width: 6px;
            height: 6px;
        }
        ::-webkit-scrollbar-track {
            background: #0b0f19;
        }
        ::-webkit-scrollbar-thumb {
            background: #1e293b;
            border-radius: 3px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: #334155;
        }
        @keyframes pulse-fast {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.3; }
        }
        .animate-pulse-fast {
            animation: pulse-fast 0.8s cubic-bezier(0.4, 0, 0.6, 1) infinite;
        }
    </style>
</head>
<body class="min-h-screen flex flex-col font-sans antialiased text-slate-200 selection:bg-accentblue selection:text-white">

    <!-- Top Navigation Header with Increased Vertical Spacing -->
<header class="glass-panel sticky top-0 z-50 border-b border-cardborder px-3 sm:px-5 py-3 sm:py-4">
        <div class="max-w-[2400px] mx-auto flex flex-col lg:flex-row lg:items-center lg:justify-center gap-4 lg:gap-6">
            
            <div class="flex items-center justify-between lg:justify-start gap-4 lg:gap-8">
                <div class="flex items-center gap-3">
                    <div>
<div class="flex items-center gap-2 flex-nowrap whitespace-nowrap">
    <img src="../img/qsicon.png" alt="QuantStream Logo" class="w-10 h-9 sm:w-14 sm:h-12 object-contain shrink-0">
    <span class="font-semibold text-white text-base sm:text-xl" style="display: inline-block; transform: scaleX(1);">QuantStream AI</span>
    <span class="text-accentcyan font-mono font-normal text-xs ml-1.5 px-1.5 py-0.5 rounded bg-cyan-500/10 border border-cyan-500/20 shrink-0">v3.0</span>
</div>
                        <p class="text-[9px] sm:text-[10px] text-slate-400 font-mono tracking-wider mt-0.5">QUANTITATIVE ALPHA PREDICTION ENGINE</p>
                    </div>
                </div>

                <div class="flex items-center gap-2 sm:gap-3">
                    <div id="statusBadge" class="flex items-center gap-1.5 sm:gap-2 px-2.5 sm:px-3 py-1.5 rounded-full bg-emerald-500/10 border border-emerald-500/30 text-emerald-400 text-[10px] sm:text-xs font-mono">
                        <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse-fast shrink-0"></span>
                        <span id="statusText" class="truncate max-w-[110px] sm:max-w-none">STREAMING WS</span>
                    </div>
                    <div class="hidden sm:flex flex-col text-right font-mono text-xs text-slate-400">
                        <span id="utcClock" class="font-semibold text-slate-200">00:00:00 UTC</span>
                        <span class="text-[10px] text-slate-500">TICK FREQ: 750ms</span>
                    </div>
                </div>
            </div>

            <div class="flex flex-wrap items-center justify-center gap-2 sm:gap-1 bg-slate-600/50 p-2 sm:p-2.5 rounded-xl border border-white/5">
                
                <!-- PROMINENT Interactive Brokers Execution Switch with Official Logo & Disk Persistence -->
                <div id="ibkrContainerBox" class="w-full sm:w-auto flex items-center justify-between sm:justify-start gap-3 px-3.5 py-2 rounded-xl bg-gradient-to-r from-slate-950 via-slate-900 to-slate-950 border-2 border-red-500/50 shadow-lg transition-all duration-300">
                    <div class="flex items-center gap-2.5">
                        <div class="flex items-center justify-center w-8 h-8 rounded-lg bg-white/5 border border-white/10 p-1 shrink-0">
                            <a href='https://www.interactivebrokers.ca/en/home.php' target=new><img src="../img/ibicon.png" alt="Interactive Brokers Logo" class="w-full h-full object-contain"></a>
                        </div>
                        <div class="flex flex-col">
                            <div class="flex items-center gap-1.5">
                                <span class="text-[10px] font-mono font-bold uppercase tracking-wider text-slate-300">Interactive Brokers</span>
                                <span class="px-1 py-0.2 rounded text-[8px] font-mono bg-red-500/20 text-red-400 border border-red-500/30">LIVE API</span>
                            </div>
                            <span id="ibkrStatusLabel" class="text-xs font-mono font-extrabold text-red-400 tracking-wide">AUTO-TRADING OFF</span>
                        </div>
                    </div>
                    <label class="relative inline-flex items-center cursor-pointer ml-2">
                        <input type="checkbox" id="ibkrToggle" class="sr-only peer">
                        <div class="w-11 h-6 bg-slate-800 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-slate-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-emerald-500"></div>
                    </label>
                </div>

                <div class="h-4 w-px bg-slate-800 hidden sm:block"></div>

                <div class="flex items-center gap-2 w-full sm:w-auto justify-center">
                    <button id="toggleStreamBtn" class="flex-1 sm:flex-none flex items-center justify-center gap-1.5 px-3.5 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-medium text-xs transition-all shadow-md">
                        <i data-lucide="pause" id="toggleStreamIcon" class="w-3.5 h-3.5"></i>
                        <span id="toggleStreamText">Pause Stream</span>
                    </button>

                    <div class="flex items-center gap-1">
                        <button id="shockCrashBtn" class="px-2.5 sm:px-3 py-2 rounded-lg bg-red-600/20 hover:bg-red-600/40 border border-red-500/30 text-red-300 font-mono text-[11px] transition-all">
                            ⚡ Flash Crash
                        </button>
                        <button id="shockRallyBtn" class="px-2.5 sm:px-3 py-2 rounded-lg bg-emerald-600/20 hover:bg-emerald-600/40 border border-emerald-500/30 text-emerald-300 font-mono text-[11px] transition-all">
                            🚀 Bull Rally
                        </button>
                    </div>
                </div>

                <div class="h-4 w-px bg-slate-800 hidden sm:block"></div>

                <div class="flex flex-wrap items-center justify-center gap-3 text-xs font-mono px-2 w-full sm:w-auto pt-1 sm:pt-0 border-t sm:border-t-0 border-slate-700/50">
                    <div class="flex items-center gap-2">
                        <label for="sliderK" class="text-slate-400">Lookahead <span class="text-slate-200 font-bold">k</span>:</label>
                        <input id="sliderK" type="range" min="1" max="10" value="3" class="w-16 accent-accentblue cursor-pointer">
                        <span id="valK" class="text-accentblue font-bold w-3">3</span>
                    </div>

                    <div class="flex items-center gap-2">
                        <label for="sliderDelta" class="text-slate-400">Threshold <span class="text-slate-200 font-bold">δ</span>:</label>
                        <input id="sliderDelta" type="range" min="0.0001" max="0.005" step="0.0001" value="0.0012" class="w-16 accent-accentcyan cursor-pointer">
                        <span id="valDelta" class="text-accentcyan font-bold w-12">0.0012</span>
                    </div>

                    <div class="flex items-center gap-2">
                        <label for="sliderM" class="text-slate-400">Lookback <span class="text-slate-200 font-bold">m</span>:</label>
                        <input id="sliderM" type="range" min="5" max="40" value="20" class="w-16 accent-accentpurple cursor-pointer">
                        <span id="valM" class="text-accentpurple font-bold w-5">20</span>
                    </div>
                </div>
            </div>

        </div>
    </header>
	
    <main class="max-w-[1800px] w-full mx-auto p-3 sm:p-5 space-y-4 flex-1">

        <section class="space-y-3">
            <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-3 px-1">
                <div class="flex items-center justify-between sm:justify-start gap-3">
                    <div class="flex items-center gap-2">
                        <i data-lucide="layout-grid" class="w-4 h-4 text-accentcyan shrink-0"></i>
                        <h2 class="text-xs sm:text-sm font-bold font-mono tracking-wider text-slate-200 uppercase">Dynamic Stock Alpha Heatmap Grid</h2>
                    </div>
                    <span id="stockCountBadge" class="px-2.5 py-0.5 rounded-full bg-slate-800 border border-slate-700 text-[10px] sm:text-[11px] font-mono text-slate-300 shrink-0">
                        10 Stocks Tracked
                    </span>
                </div>

                <div class="grid grid-cols-2 sm:flex sm:flex-wrap items-center gap-2 font-mono text-xs">
                    <button id="openAddStockModalBtn" class="col-span-2 flex items-center justify-center gap-1.5 px-3 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white font-medium transition-all shadow-md">
                        <i data-lucide="plus-circle" class="w-3.5 h-3.5"></i>
                        <span>Add Ticker</span>
                    </button>

                    <div class="flex items-center gap-1.5 bg-slate-900 border border-slate-700 rounded-lg px-2.5 py-1.5">
                        <i data-lucide="filter" class="w-3.5 h-3.5 text-slate-400 shrink-0"></i>
                        <select id="signalFilterSelect" class="bg-transparent text-slate-200 focus:outline-none cursor-pointer w-full text-xs">
                            <option value="ALL">All Signals</option>
                            <option value="BUY">BUY Only (Y=+1)</option>
                            <option value="SELL">SELL Only (Y=-1)</option>
                            <option value="HOLD">HOLD Only (Y=0)</option>
                        </select>
                    </div>

                    <div class="flex items-center gap-1.5 bg-slate-900 border border-slate-700 rounded-lg px-2.5 py-1.5">
                        <i data-lucide="arrow-up-down" class="w-3.5 h-3.5 text-slate-400 shrink-0"></i>
                        <select id="heatmapSortSelect" class="bg-transparent text-slate-200 focus:outline-none cursor-pointer w-full text-xs">
                            <option value="CONFIDENCE">Sort: Highest Confidence</option>
                            <option value="LOG_RETURN">Sort: Predicted Return</option>
                            <option value="VOLATILITY">Sort: Highest Volatility</option>
                            <option value="TICKER">Sort: Ticker A-Z</option>
                        </select>
                    </div>
                </div>
            </div>

            <div id="heatmapGrid" class="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 lg:grid-cols-10 gap-2.5"></div>
        </section>

        <section class="glass-panel rounded-2xl p-3 sm:p-4 border border-cardborder">
            <div class="flex flex-col lg:flex-row lg:items-center justify-between gap-4">
                <div class="flex items-start sm:items-center gap-3.5">
                    <div id="activeSymbolLogo" class="w-12 h-12 rounded-xl bg-blue-600/20 border border-blue-500/30 flex items-center justify-center font-bold text-lg sm:text-xl text-blue-400 font-mono shadow-md shrink-0">
                        AAPL
                    </div>
                    <div class="space-y-1 sm:space-y-0">
                        <div class="flex flex-wrap items-center gap-2 sm:gap-3">
                            <h1 id="activeSymbol" class="text-xl sm:text-2xl font-black font-mono tracking-tight text-white">AAPL</h1>
                            <span id="activeName" class="text-slate-400 text-xs font-medium">Apple Inc.</span>
                            <span id="activeSignalBadge" class="px-2.5 py-0.5 rounded-full text-[10px] sm:text-xs font-bold font-mono uppercase bg-emerald-500/20 text-emerald-400 border border-emerald-500/40">
                                BUY (Confidence 88.4%)
                            </span>
                        </div>
                        <div class="flex flex-wrap items-center gap-x-3 gap-y-1 font-mono text-[11px] sm:text-xs text-slate-400 mt-1">
                            <span>LAST: <strong id="activePrice" class="text-white">$189.45</strong></span>
                            <span>CHANGE: <strong id="activeChange" class="text-emerald-400">+1.85 (+0.98%)</strong></span>
                            <span>VOLATILITY σ_t: <strong id="activeVol" class="text-slate-200">0.00140</strong></span>
                            <span class="inline">HIGH: <span id="activeHigh" class="text-slate-300">$190.10</span></span>
                            <span class="inline">LOW: <span id="activeLow" class="text-slate-300">$188.20</span></span>
                        </div>
                    </div>
                </div>

                <div class="grid grid-cols-3 gap-2 font-mono text-xs bg-slate-900/90 p-3 rounded-xl border border-white/5 shadow-inner text-center sm:text-right">
                    <div>
                        <div class="text-slate-500 text-[9px] sm:text-[10px] truncate">PREDICTED RETURN ($r_{t,k}$)</div>
                        <div id="activeLogReturn" class="text-emerald-400 font-bold text-xs sm:text-sm mt-0.5">+0.00245</div>
                    </div>
                    <div class="border-x border-slate-800 px-1">
                        <div class="text-slate-500 text-[9px] sm:text-[10px] truncate">VOL BOUND ($\delta \cdot \sigma_t$)</div>
                        <div id="activeVolBound" class="text-amber-400 font-bold text-xs sm:text-sm mt-0.5">±0.00168</div>
                    </div>
                    <div>
                        <div class="text-slate-500 text-[9px] sm:text-[10px] truncate">TARGET $Y_t$</div>
                        <div id="activeTargetScore" class="text-emerald-400 font-bold text-xs sm:text-sm mt-0.5">+1 (BULLISH)</div>
                    </div>
                </div>
            </div>
        </section>

        <section class="grid grid-cols-1 lg:grid-cols-3 gap-4">
            <div class="lg:col-span-2 glass-panel rounded-2xl p-3 sm:p-4 border border-cardborder flex flex-col justify-between">
                <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-2 mb-3">
                    <div class="flex items-center gap-2">
                        <i data-lucide="trending-up" class="w-4 h-4 text-accentblue shrink-0"></i>
                        <h3 class="text-xs sm:text-sm font-bold font-mono text-slate-200">Price, SMA Band & Executed Signals (k-step Lookahead)</h3>
                    </div>
                    <div class="flex flex-wrap items-center gap-2.5 text-[10px] sm:text-[11px] font-mono text-slate-400">
                        <span class="flex items-center gap-1.5"><span class="w-3 h-0.5 bg-blue-500 inline-block"></span> Price ($c_t$)</span>
                        <span class="flex items-center gap-1.5"><span class="w-3 h-0.5 bg-purple-400 inline-block"></span> SMA ($m$)</span>
                        <span class="flex items-center gap-1.5"><span class="w-2.5 h-2.5 rounded-full bg-emerald-400 inline-block"></span> Buy</span>
                        <span class="flex items-center gap-1.5"><span class="w-2.5 h-2.5 rounded-full bg-red-400 inline-block"></span> Sell</span>
                    </div>
                </div>

                <div class="relative w-full h-[260px] sm:h-[320px]">
                    <canvas id="priceChart"></canvas>
                </div>
            </div>

            <div class="glass-panel rounded-2xl p-3 sm:p-4 border border-cardborder flex flex-col justify-between">
                <div class="flex items-center justify-between mb-3">
                    <div class="flex items-center gap-2">
                        <i data-lucide="activity" class="w-4 h-4 text-accentpurple shrink-0"></i>
                        <h3 class="text-xs sm:text-sm font-bold font-mono text-slate-200">Rolling Volatility (σ<sub>t</sub>) vs Boundary</h3>
                    </div>
                    <span class="text-[10px] sm:text-xs font-mono text-slate-500">m = 20</span>
                </div>

                <div class="relative w-full h-[260px] sm:h-[320px]">
                    <canvas id="volatilityChart"></canvas>
                </div>
            </div>
        </section>

        <section class="grid grid-cols-1 lg:grid-cols-3 gap-4">
            <div class="glass-panel rounded-2xl p-3 sm:p-4 border border-cardborder flex flex-col justify-between">
                <div class="flex items-center justify-between mb-2">
                    <div class="flex items-center gap-2">
                        <i data-lucide="radar" class="w-4 h-4 text-accentcyan shrink-0"></i>
                        <h3 class="text-xs sm:text-sm font-bold font-mono text-slate-200">Normalized Feature Vector X<sub>t</sub></h3>
                    </div>
                    <span class="text-[10px] font-mono text-slate-500">9 Dimensions</span>
                </div>
                <div class="relative w-full h-[240px] sm:h-[270px]">
                    <canvas id="featureRadarChart"></canvas>
                </div>
            </div>

            <div class="glass-panel rounded-2xl p-3 sm:p-4 border border-cardborder flex flex-col justify-between">
                <div class="flex items-center justify-between mb-2">
                    <div class="flex items-center gap-2">
                        <i data-lucide="bar-chart-2" class="w-4 h-4 text-accentgreen shrink-0"></i>
                        <h3 class="text-xs sm:text-sm font-bold font-mono text-slate-200">Independent Variables (x<sub>1</sub> &hellip; x<sub>9</sub>)</h3>
                    </div>
                    <span class="text-[10px] font-mono text-slate-500">[-1, +1]</span>
                </div>
                <div id="featureBarContainer" class="space-y-2 text-xs font-mono overflow-y-auto max-h-[240px] sm:max-h-[270px] pr-1"></div>
            </div>

            <div class="glass-panel rounded-2xl p-3 sm:p-4 border border-cardborder flex flex-col justify-between">
                <div class="flex items-center justify-between mb-2">
                    <div class="flex items-center gap-2">
                        <i data-lucide="gauge" class="w-4 h-4 text-amber-400 shrink-0"></i>
                        <h3 class="text-xs sm:text-sm font-bold font-mono text-slate-200">Quant Alpha Classifier Logic</h3>
                    </div>
                    <span class="text-[10px] font-mono text-slate-500">Rule</span>
                </div>

                <div class="flex flex-col items-center justify-center my-1">
                    <div class="relative w-44 h-24 sm:w-48 sm:h-26 flex items-end justify-center">
                        <canvas id="gaugeChart" class="w-full h-full"></canvas>
                        <div class="absolute bottom-0 text-center">
                            <span id="gaugeValueText" class="text-xl sm:text-2xl font-black font-mono text-emerald-400">+0.87</span>
                            <div class="text-[9px] sm:text-[10px] text-slate-400 font-mono uppercase">Normalized Alpha Score</div>
                        </div>
                    </div>
                </div>

                <div class="bg-slate-900/90 rounded-xl p-3 border border-white/5 space-y-2 text-xs font-mono">
                    <div class="flex justify-between items-center text-slate-400 text-[11px] sm:text-xs">
                        <span>Classification Rule:</span>
                        <span class="text-slate-200 font-bold">Volatility-Normalized</span>
                    </div>
					<div class="p-2 rounded bg-slate-950/80 border border-white/5 text-[10px] sm:text-[11px] leading-relaxed text-slate-300 text-center font-mono overflow-x-auto">
						Y<sub>t</sub> = sign( ln(c<sub>t+k</sub> / c<sub>t</sub>) / (&delta; &middot; &sigma;<sub>t</sub>) )
					</div>
                    <div class="flex justify-between items-center text-[11px] sm:text-xs">
                        <span class="text-slate-400">Classifier Confidence:</span>
                        <span id="confidenceVal" class="text-emerald-400 font-bold">88.4%</span>
                    </div>
                </div>
            </div>
        </section>

        <section class="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <div class="glass-panel rounded-2xl p-3 sm:p-4 border border-cardborder flex flex-col h-[280px] sm:h-[320px]">
                <div class="flex items-center justify-between mb-3">
                    <div class="flex items-center gap-2">
                        <i data-lucide="terminal" class="w-4 h-4 text-accentblue shrink-0"></i>
                        <h3 class="text-xs sm:text-sm font-bold font-mono text-slate-200">Real-Time Feature Tick Stream & Order Log</h3>
                    </div>
                    <button id="clearLogBtn" class="text-[10px] font-mono text-slate-500 hover:text-slate-300 transition-colors">Clear Log</button>
                </div>
                <div id="streamLog" class="flex-1 bg-slate-950/90 rounded-xl p-3 border border-white/5 font-mono text-[10px] sm:text-[11px] overflow-y-auto space-y-1.5 text-slate-400"></div>
            </div>

            <div class="glass-panel rounded-2xl p-3 sm:p-4 border border-cardborder flex flex-col h-[280px] sm:h-[320px]">
                <div class="flex items-center justify-between mb-3">
                    <div class="flex items-center gap-2">
                        <i data-lucide="code-2" class="w-4 h-4 text-accentpurple shrink-0"></i>
                        <h3 class="text-xs sm:text-sm font-bold font-mono text-slate-200">Go Feature Engineering Struct</h3>
                    </div>
                    <span class="text-[10px] font-mono text-accentcyan font-bold">Go v1.22</span>
                </div>
                <div class="flex-1 bg-slate-950/90 rounded-xl p-3 border border-white/5 font-mono text-[10px] sm:text-[11px] overflow-y-auto text-slate-300">
<pre class="text-slate-400 leading-relaxed"><code><span class="text-purple-400">package</span> main

<span class="text-purple-400">type</span> FinnhubQuote <span class="text-purple-400">struct</span> {
    C  <span class="text-amber-300">float64</span> `json:"c"`  <span class="text-slate-500">// Current price</span>
    D  <span class="text-amber-300">float64</span> `json:"d"`  <span class="text-slate-500">// Change</span>
    DP <span class="text-amber-300">float64</span> `json:"dp"` <span class="text-slate-500">// Percent change</span>
    H  <span class="text-amber-300">float64</span> `json:"h"`  <span class="text-slate-500">// High price</span>
    L  <span class="text-amber-300">float64</span> `json:"l"`  <span class="text-slate-500">// Low price</span>
    O  <span class="text-amber-300">float64</span> `json:"o"`  <span class="text-slate-500">// Open price</span>
    PC <span class="text-amber-300">float64</span> `json:"pc"` <span class="text-slate-500">// Previous close</span>
    T  <span class="text-amber-300">int64</span>   `json:"t"`  <span class="text-slate-500">// Unix timestamp</span>
}

<span class="text-slate-500">// ExtractRollingFeatures creates 9 independent variables (X_t)</span>
<span class="text-purple-400">func</span> ExtractRollingFeatures(history []FinnhubQuote, m <span class="text-amber-300">int</span>) []<span class="text-amber-300">float64</span> {
    curr := history[<span class="text-blue-300">len</span>(history)-1]
    x1 := math.<span class="text-blue-300">Log</span>(curr.C / curr.PC) <span class="text-slate-500">// Daily Log Return</span>
    x2 := math.<span class="text-blue-300">Log</span>(curr.C / curr.O)  <span class="text-slate-500">// Intraday Log Return</span>
    
    <span class="text-slate-500">// Compute Rolling Volatility sigma_t</span>
    sigma := CalculateRollingVolatility(history, m)
    
    <span class="text-purple-400">return</span> []<span class="text-amber-300">float64</span>{x1, x2, sigma, ...}
}</code></pre>
                </div>
            </div>
        </section>

    </main>

    <div id="addStockModal" class="fixed inset-0 bg-black/70 backdrop-blur-sm z-50 flex items-center justify-center p-4 hidden">
        <div class="glass-panel w-full max-w-md rounded-2xl p-5 sm:p-6 border border-cardborder shadow-2xl space-y-4">
            <div class="flex items-center justify-between border-b border-slate-800 pb-3">
                <div class="flex items-center gap-2">
                    <i data-lucide="plus-circle" class="w-5 h-5 text-accentcyan"></i>
                    <h3 class="font-bold text-base text-white font-mono">Add Stock Ticker to Heatmap</h3>
                </div>
                <button id="closeAddStockModalBtn" class="text-slate-400 hover:text-white transition-colors">
                    <i data-lucide="x" class="w-5 h-5"></i>
                </button>
            </div>

            <form id="addStockForm" class="space-y-4 font-mono text-xs">
                <div>
                    <label class="block text-slate-400 mb-1">Ticker Symbol (e.g. BTC, NFLX, DIS)</label>
                    <input id="newTickerInput" type="text" placeholder="NFLX" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white uppercase focus:outline-none focus:border-accentcyan">
                </div>

                <div>
                    <label class="block text-slate-400 mb-1">Company / Asset Name</label>
                    <input id="newNameInput" type="text" placeholder="Netflix Inc." required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white focus:outline-none focus:border-accentcyan">
                </div>

                <div>
                    <label class="block text-slate-400 mb-1">Base Price ($)</label>
                    <input id="newPriceInput" type="number" step="0.01" placeholder="650.00" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white focus:outline-none focus:border-accentcyan">
                </div>

                <div class="flex items-center justify-end gap-3 pt-2">
                    <button type="button" id="cancelModalBtn" class="px-4 py-2 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 font-medium">Cancel</button>
                    <button type="submit" class="px-4 py-2 rounded-lg bg-accentblue hover:bg-blue-500 text-white font-bold shadow-lg shadow-blue-500/20">Add Ticker</button>
                </div>
            </form>
        </div>
    </div>

    <script>
        let SYMBOLS = ['AAPL', 'NVDA', 'TSLA', 'SPY', 'MSFT', 'AMD', 'QQQ', 'AMZN', 'META', 'GOOGL'];

        const INITIAL_STOCK_METADATA = {
            'AAPL': { name: 'Apple Inc.', basePrice: 189.50 },
            'NVDA': { name: 'NVIDIA Corp.', basePrice: 128.20 },
            'TSLA': { name: 'Tesla Inc.', basePrice: 245.80 },
            'SPY':  { name: 'SPDR S&P 500 ETF', basePrice: 552.10 },
            'MSFT': { name: 'Microsoft Corp.', basePrice: 448.90 },
            'AMD':  { name: 'Advanced Micro Devices', basePrice: 154.30 },
            'QQQ':  { name: 'Invesco QQQ Trust', basePrice: 480.40 },
            'AMZN': { name: 'Amazon.com Inc.', basePrice: 186.60 },
            'META': { name: 'Meta Platforms Inc.', basePrice: 512.40 },
            'GOOGL':{ name: 'Alphabet Inc.', basePrice: 178.30 }
        };

        // Load persisted IBKR state from local disk (localStorage)
        const savedIbkrState = localStorage.getItem('quantstream_ibkr_autotrading');
        const initialIbkrValue = savedIbkrState === 'true';

        let state = {
            activeSymbol: 'AAPL',
            isStreaming: true,
            ibkrAutoTrading: initialIbkrValue, 
            k: 3,
            delta: 0.0012,
            m: 20,
            filterSignal: 'ALL',
            sortBy: 'CONFIDENCE',
            stocks: {}
        };

        function initStock(sym, name, base) {
            const history = [];
            let price = base;

            for (let i = 0; i < 40; i++) {
                const change = (Math.random() - 0.495) * (base * 0.003);
                price = Math.max(1, price + change);
                const high = price + Math.random() * (base * 0.002);
                const low = price - Math.random() * (base * 0.002);
                const open = price - (Math.random() - 0.5) * (base * 0.0015);

                history.push({
                    c: price,
                    d: price - base,
                    dp: ((price - base) / base) * 100,
                    h: high,
                    l: low,
                    o: open,
                    pc: base,
                    t: Date.now() - (40 - i) * 800
                });
            }

            state.stocks[sym] = {
                name: name,
                basePrice: base,
                history: history,
                signal: 0,
                confidence: 85.0,
                logReturn: 0.0,
                volatility: 0.0012,
                featureVector: [0,0,0,0,0,0,0,0,0]
            };
        }

        SYMBOLS.forEach(sym => {
            const meta = INITIAL_STOCK_METADATA[sym];
            initStock(sym, meta.name, meta.basePrice);
        });

        let priceChart, volatilityChart, featureRadarChart, gaugeChart;

        function calculateLogReturn(p2, p1) {
            if (p1 <= 0 || p2 <= 0) return 0.0;
            return Math.log(p2 / p1);
        }

        function computeQuantFeatures(sym) {
            const stock = state.stocks[sym];
            const ticks = stock.history;
            const n = ticks.length;
            if (n < state.m + 1) return;

            const curr = ticks[n - 1];
            const prev = ticks[n - 2];

            const x1 = calculateLogReturn(curr.c, curr.pc);
            const x2 = calculateLogReturn(curr.c, curr.o);
            const range = curr.h - curr.l;
            const x3 = range > 0 ? (curr.c - curr.l) / range : 0.5;
            const x4 = range > 0 ? (curr.c - curr.o) / range : 0.0;

            const lookbackTick = ticks[Math.max(0, n - state.m)];
            const x5 = calculateLogReturn(curr.c, lookbackTick.c);

            let logReturns = [];
            let sum = 0;
            for (let i = n - state.m; i < n; i++) {
                const r = calculateLogReturn(ticks[i].c, ticks[i - 1].c);
                logReturns.push(r);
                sum += r;
            }
            const mean = sum / state.m;
            let variance = 0;
            logReturns.forEach(r => variance += Math.pow(r - mean, 2));
            const sigma = Math.sqrt(variance / state.m);

            let windowHigh = -Infinity, windowLow = Infinity;
            for (let i = n - state.m; i < n; i++) {
                if (ticks[i].h > windowHigh) windowHigh = ticks[i].h;
                if (ticks[i].l < windowLow) windowLow = ticks[i].l;
            }
            const x7 = (windowHigh - windowLow) > 0 ? (curr.c - windowLow) / (windowHigh - windowLow) : 0.5;

            let smaSum = 0;
            for (let i = n - state.m; i < n; i++) smaSum += ticks[i].c;
            const sma = smaSum / state.m;
            const x8 = (curr.c - sma) / (sma + 1e-6);

            const rCurr = calculateLogReturn(curr.c, prev.c);
            const rPrev = calculateLogReturn(prev.c, ticks[Math.max(0, n - 3)].c);
            const x9 = rCurr - rPrev;

            const predictedLogReturn = (x2 * 0.4) + (x5 * 0.3) + (x9 * 0.3);
            const threshold = state.delta * sigma;

            let signal = 0;
            if (predictedLogReturn >= threshold) {
                signal = 1;
            } else if (predictedLogReturn <= -threshold) {
                signal = -1;
            }

            const confidence = Math.min(99.4, Math.max(60.0, 72 + Math.abs(predictedLogReturn / (threshold + 1e-6)) * 14));

            stock.logReturn = predictedLogReturn;
            stock.volatility = sigma;
            stock.signal = signal;
            stock.confidence = confidence;
            stock.featureVector = [x1*100, x2*100, x3, x4, x5*100, sigma*1000, x7, x8*100, x9*1000];
        }

        function renderHeatmap() {
            const grid = document.getElementById('heatmapGrid');
            grid.innerHTML = '';

            let displaySymbols = SYMBOLS.filter(sym => {
                const stock = state.stocks[sym];
                if (state.filterSignal === 'BUY') return stock.signal === 1;
                if (state.filterSignal === 'SELL') return stock.signal === -1;
                if (state.filterSignal === 'HOLD') return stock.signal === 0;
                return true;
            });

            displaySymbols.sort((a, b) => {
                const sA = state.stocks[a];
                const sB = state.stocks[b];

                if (state.sortBy === 'CONFIDENCE') return sB.confidence - sA.confidence;
                if (state.sortBy === 'LOG_RETURN') return sB.logReturn - sA.logReturn;
                if (state.sortBy === 'VOLATILITY') return sB.volatility - sA.volatility;
                if (state.sortBy === 'TICKER') return a.localeCompare(b);
                return 0;
            });

            document.getElementById('stockCountBadge').innerText = `${displaySymbols.length} / ${SYMBOLS.length} Stocks`;

            displaySymbols.forEach(sym => {
                const stock = state.stocks[sym];
                const last = stock.history[stock.history.length - 1];
                const isSelected = sym === state.activeSymbol;

                let cardStatusClass = 'border-slate-800 bg-slate-900/70 hover:border-slate-700';
                let badgeColor = 'bg-slate-700 text-slate-300 border-slate-600';
                let signalText = 'HOLD';

                if (stock.signal === 1) {
                    cardStatusClass = 'glow-green bg-emerald-950/20 border-emerald-500/40';
                    badgeColor = 'bg-emerald-500/20 text-emerald-400 border-emerald-500/40';
                    signalText = 'BUY';
                } else if (stock.signal === -1) {
                    cardStatusClass = 'glow-red bg-red-950/20 border-red-500/40';
                    badgeColor = 'bg-red-500/20 text-red-400 border-red-500/40';
                    signalText = 'SELL';
                }

                if (isSelected) {
                    cardStatusClass += ' ring-2 ring-accentblue';
                }

                const card = document.createElement('div');
                card.className = `glass-panel rounded-xl p-2.5 sm:p-3 border transition-all cursor-pointer relative group ${cardStatusClass}`;
                card.onclick = () => selectActiveStock(sym);

                const isPos = last.d >= 0;
                const changeColor = isPos ? 'text-emerald-400' : 'text-red-400';

                card.innerHTML = `
                    <div class="flex items-center justify-between mb-1">
                        <span class="font-bold font-mono text-xs sm:text-sm text-white">${sym}</span>
                        <div class="flex items-center gap-1">
                            <span class="px-1.5 py-0.5 rounded text-[8px] sm:text-[9px] font-bold font-mono border ${badgeColor}">${signalText}</span>
                            <button onclick="removeStock(event, '${sym}')" class="opacity-0 group-hover:opacity-100 text-slate-500 hover:text-red-400 transition-opacity p-0.5">
                                <i data-lucide="trash-2" class="w-3 h-3"></i>
                            </button>
                        </div>
                    </div>
                    <div class="font-mono text-sm sm:text-base font-black text-white">$${last.c.toFixed(2)}</div>
                    <div class="flex items-center justify-between font-mono text-[9px] sm:text-[10px] mt-1">
                        <span class="${changeColor} font-semibold">${isPos ? '+' : ''}${last.dp.toFixed(2)}%</span>
                        <span class="text-slate-400">σ: ${(stock.volatility * 1000).toFixed(1)}k</span>
                    </div>
                    <div class="w-full bg-slate-800 h-1 rounded-full mt-2 overflow-hidden">
                        <div class="h-full ${stock.signal === 1 ? 'bg-emerald-400' : stock.signal === -1 ? 'bg-red-400' : 'bg-slate-500'}" style="width: ${stock.confidence}%"></div>
                    </div>
                `;

                grid.appendChild(card);
            });

            lucide.createIcons();
        }

        function removeStock(event, sym) {
            event.stopPropagation();
            if (SYMBOLS.length <= 1) {
                alert("At least one ticker must remain in the heatmap.");
                return;
            }
            SYMBOLS = SYMBOLS.filter(s => s !== sym);
            delete state.stocks[sym];
            if (state.activeSymbol === sym) {
                state.activeSymbol = SYMBOLS[0];
            }
            renderHeatmap();
            updateDashboardUI();
        }

        function selectActiveStock(sym) {
            state.activeSymbol = sym;
            renderHeatmap();
            updateDashboardUI();
        }

        function updateDashboardUI() {
            const sym = state.activeSymbol;
            const stock = state.stocks[sym];
            const last = stock.history[stock.history.length - 1];

            document.getElementById('activeSymbolLogo').innerText = sym;
            document.getElementById('activeSymbol').innerText = sym;
            document.getElementById('activeName').innerText = stock.name;
            document.getElementById('activePrice').innerText = `$${last.c.toFixed(2)}`;

            const isPos = last.d >= 0;
            const changeElem = document.getElementById('activeChange');
            changeElem.innerText = `${isPos ? '+' : ''}${last.d.toFixed(2)} (${isPos ? '+' : ''}${last.dp.toFixed(2)}%)`;
            changeElem.className = isPos ? 'text-emerald-400 font-bold' : 'text-red-400 font-bold';

            document.getElementById('activeVol').innerText = stock.volatility.toFixed(5);
            document.getElementById('activeHigh').innerText = `$${last.h.toFixed(2)}`;
            document.getElementById('activeLow').innerText = `$${last.l.toFixed(2)}`;

            const logRetElem = document.getElementById('activeLogReturn');
            logRetElem.innerText = `${stock.logReturn >= 0 ? '+' : ''}${stock.logReturn.toFixed(5)}`;
            logRetElem.className = stock.logReturn >= 0 ? 'text-emerald-400 font-bold text-xs sm:text-sm' : 'text-red-400 font-bold text-xs sm:text-sm';

            const bound = state.delta * stock.volatility;
            document.getElementById('activeVolBound').innerText = `±${bound.toFixed(5)}`;

            const targetElem = document.getElementById('activeTargetScore');
            const badgeElem = document.getElementById('activeSignalBadge');

            if (stock.signal === 1) {
                targetElem.innerText = '+1 (BULLISH)';
                targetElem.className = 'text-emerald-400 font-bold text-xs sm:text-sm';
                badgeElem.innerText = `BUY (${stock.confidence.toFixed(0)}%)`;
                badgeElem.className = 'px-2.5 py-0.5 rounded-full text-[10px] sm:text-xs font-bold font-mono uppercase bg-emerald-500/20 text-emerald-400 border border-emerald-500/40';
            } else if (stock.signal === -1) {
                targetElem.innerText = '-1 (BEARISH)';
                targetElem.className = 'text-red-400 font-bold text-xs sm:text-sm';
                badgeElem.innerText = `SELL (${stock.confidence.toFixed(0)}%)`;
                badgeElem.className = 'px-2.5 py-0.5 rounded-full text-[10px] sm:text-xs font-bold font-mono uppercase bg-red-500/20 text-red-400 border border-red-500/40';
            } else {
                targetElem.innerText = '0 (NEUTRAL)';
                targetElem.className = 'text-slate-400 font-bold text-xs sm:text-sm';
                badgeElem.innerText = `HOLD (${stock.confidence.toFixed(0)}%)`;
                badgeElem.className = 'px-2.5 py-0.5 rounded-full text-[10px] sm:text-xs font-bold font-mono uppercase bg-slate-800 text-slate-300 border border-slate-700';
            }

            document.getElementById('confidenceVal').innerText = `${stock.confidence.toFixed(1)}%`;

            renderFeatureBars(stock.featureVector);
            updateCharts();
        }

        function renderFeatureBars(fv) {
            const container = document.getElementById('featureBarContainer');
            container.innerHTML = '';

            const names = [
                'Daily Log Return (x1)',
                'Intraday Log Return (x2)',
                'Stochastic Position (x3)',
                'Candle Body Ratio (x4)',
                'Cumulative Return m-ticks (x5)',
                'Rolling Volatility σ_t (x6)',
                'Price Range Position (x7)',
                'SMA Offset Distance (x8)',
                'Momentum Acceleration (x9)'
            ];

            fv.forEach((val, idx) => {
                const normVal = Math.max(-1, Math.min(1, val));
                const widthPct = Math.abs(normVal) * 100;
                const isPos = normVal >= 0;
                const barColor = isPos ? 'bg-emerald-500' : 'bg-red-500';

                const row = document.createElement('div');
                row.className = 'space-y-1';
                row.innerHTML = `
                    <div class="flex justify-between text-[11px]">
                        <span class="text-slate-400">${names[idx]}</span>
                        <span class="text-slate-200 font-mono">${val.toFixed(3)}</span>
                    </div>
                    <div class="w-full bg-slate-900 h-1.5 rounded-full overflow-hidden flex items-center">
                        <div class="h-full ${barColor} transition-all duration-300" style="width: ${widthPct}%"></div>
                    </div>
                `;
                container.appendChild(row);
            });
        }

        function initCharts() {
            const ctxPrice = document.getElementById('priceChart').getContext('2d');
            priceChart = new Chart(ctxPrice, {
                type: 'line',
                data: {
                    labels: [],
                    datasets: [
                        {
                            label: 'Price',
                            data: [],
                            borderColor: '#3b82f6',
                            borderWidth: 2,
                            tension: 0.2,
                            pointRadius: 0
                        },
                        {
                            label: 'SMA',
                            data: [],
                            borderColor: '#a855f7',
                            borderWidth: 1.5,
                            borderDash: [3, 3],
                            pointRadius: 0
                        },
                        {
                            label: 'Buy Signal',
                            data: [],
                            borderColor: '#10b981',
                            backgroundColor: '#10b981',
                            pointStyle: 'triangle',
                            pointRadius: 6,
                            pointHoverRadius: 8,
                            showLine: false
                        },
                        {
                            label: 'Sell Signal',
                            data: [],
                            borderColor: '#ef4444',
                            backgroundColor: '#ef4444',
                            pointStyle: 'triangle',
                            pointRotation: 180,
                            pointRadius: 6,
                            pointHoverRadius: 8,
                            showLine: false
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    animation: false,
                    scales: {
                        x: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#64748b', font: { family: 'JetBrains Mono', size: 9 }, maxTicksLimit: 6 } },
                        y: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#64748b', font: { family: 'JetBrains Mono', size: 9 } } }
                    },
                    plugins: { legend: { display: false } }
                }
            });

            const ctxVol = document.getElementById('volatilityChart').getContext('2d');
            volatilityChart = new Chart(ctxVol, {
                type: 'line',
                data: {
                    labels: [],
                    datasets: [
                        {
                            label: 'Rolling σ_t',
                            data: [],
                            borderColor: '#8b5cf6',
                            backgroundColor: 'rgba(139, 92, 246, 0.12)',
                            fill: true,
                            borderWidth: 1.5,
                            pointRadius: 0
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    animation: false,
                    scales: {
                        x: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#64748b', font: { family: 'JetBrains Mono', size: 9 }, maxTicksLimit: 6 } },
                        y: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#64748b', font: { family: 'JetBrains Mono', size: 9 } } }
                    },
                    plugins: { legend: { display: false } }
                }
            });

            const ctxRadar = document.getElementById('featureRadarChart').getContext('2d');
            featureRadarChart = new Chart(ctxRadar, {
                type: 'radar',
                data: {
                    labels: ['X1 LogR', 'X2 Intra', 'X3 Stoch', 'X4 Body', 'X5 CumR', 'X6 Vol', 'X7 Range', 'X8 SMA', 'X9 Accel'],
                    datasets: [{
                        label: 'Feature Vector X_t',
                        data: [0,0,0,0,0,0,0,0,0],
                        backgroundColor: 'rgba(6, 182, 212, 0.25)',
                        borderColor: '#06b6d4',
                        borderWidth: 2,
                        pointBackgroundColor: '#06b6d4'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    animation: { duration: 250 },
                    scales: {
                        r: {
                            angleLines: { color: 'rgba(255, 255, 255, 0.1)' },
                            grid: { color: 'rgba(255, 255, 255, 0.08)' },
                            pointLabels: { color: '#94a3b8', font: { family: 'JetBrains Mono', size: 8 } },
                            ticks: { display: false }
                        }
                    },
                    plugins: { legend: { display: false } }
                }
            });

            const ctxGauge = document.getElementById('gaugeChart').getContext('2d');
            gaugeChart = new Chart(ctxGauge, {
                type: 'doughnut',
                data: {
                    datasets: [{
                        data: [50, 50],
                        backgroundColor: ['#10b981', '#1e293b'],
                        borderWidth: 0
                    }]
                },
                options: {
                    rotation: -90,
                    circumference: 180,
                    cutout: '80%',
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { tooltip: { enabled: false } }
                }
            });
        }

        function updateCharts() {
            const sym = state.activeSymbol;
            const stock = state.stocks[sym];
            const ticks = stock.history;

            const labels = ticks.map(t => new Date(t.t).toLocaleTimeString([], { hour12: false, minute:'2-digit', second:'2-digit' }));
            const prices = ticks.map(t => t.c);

            const sma = prices.map((_, idx) => {
                if (idx < state.m) return prices[idx];
                let sum = 0;
                for (let i = idx - state.m + 1; i <= idx; i++) sum += prices[i];
                return sum / state.m;
            });

            const buySignals = [];
            const sellSignals = [];

            ticks.forEach((t, i) => {
                let sig = 0;
                if (i >= state.m) {
                    const prevC = ticks[Math.max(0, i - 1)].c;
                    const openC = t.o;
                    const x2 = calculateLogReturn(t.c, openC);
                    const lookbackTick = ticks[Math.max(0, i - state.m)];
                    const x5 = calculateLogReturn(t.c, lookbackTick.c);
                    const rCurr = calculateLogReturn(t.c, prevC);
                    const rPrev = calculateLogReturn(prevC, ticks[Math.max(0, i - 3)].c);
                    const x9 = rCurr - rPrev;
                    
                    let logReturns = [];
                    let sum = 0;
                    for (let j = i - state.m + 1; j <= i; j++) {
                        const r = calculateLogReturn(ticks[j].c, ticks[j - 1].c);
                        logReturns.push(r);
                        sum += r;
                    }
                    const mean = sum / state.m;
                    let variance = 0;
                    logReturns.forEach(r => variance += Math.pow(r - mean, 2));
                    const sigma = Math.sqrt(variance / state.m);

                    const predLogR = (x2 * 0.4) + (x5 * 0.3) + (x9 * 0.3);
                    const thresh = state.delta * sigma;

                    if (predLogR >= thresh) sig = 1;
                    else if (predLogR <= -thresh) sig = -1;
                }

                if (sig === 1) {
                    buySignals.push(t.c);
                    sellSignals.push(null);
                } else if (sig === -1) {
                    buySignals.push(null);
                    sellSignals.push(t.c);
                } else {
                    buySignals.push(null);
                    sellSignals.push(null);
                }
            });

            priceChart.data.labels = labels;
            priceChart.data.datasets[0].data = prices;
            priceChart.data.datasets[1].data = sma;
            priceChart.data.datasets[2].data = buySignals;
            priceChart.data.datasets[3].data = sellSignals;
            priceChart.update();

            volatilityChart.data.labels = labels;
            volatilityChart.data.datasets[0].data = ticks.map((_, i) => {
                if (i < state.m) return 0;
                let sum = 0;
                for (let j = i - state.m + 1; j <= i; j++) {
                    sum += Math.abs(calculateLogReturn(ticks[j].c, ticks[j-1].c));
                }
                return sum / state.m;
            });
            volatilityChart.update();

            featureRadarChart.data.datasets[0].data = stock.featureVector.map(v => Math.max(-1, Math.min(1, v)));
            featureRadarChart.update();

            const gaugeScore = Math.max(0, Math.min(100, (stock.logReturn / (state.delta * stock.volatility + 1e-6) + 1) * 50));
            gaugeChart.data.datasets[0].data = [gaugeScore, 100 - gaugeScore];
            gaugeChart.data.datasets[0].backgroundColor = stock.signal === 1 ? ['#10b981', '#1e293b'] : stock.signal === -1 ? ['#ef4444', '#1e293b'] : ['#64748b', '#1e293b'];
            gaugeChart.update();

            document.getElementById('gaugeValueText').innerText = `${stock.logReturn >= 0 ? '+' : ''}${(stock.logReturn * 1000).toFixed(2)}`;
            document.getElementById('gaugeValueText').className = stock.signal === 1 ? 'text-xl sm:text-2xl font-black font-mono text-emerald-400' : stock.signal === -1 ? 'text-xl sm:text-2xl font-black font-mono text-red-400' : 'text-xl sm:text-2xl font-black font-mono text-slate-400';
        }

        function appendStreamLog(sym, tick, Y) {
            const logContainer = document.getElementById('streamLog');
            const entry = document.createElement('div');
            entry.className = 'flex items-center gap-2 border-b border-white/5 pb-1';
            
            const timeStr = new Date(tick.t).toLocaleTimeString([], { hour12: false, minute:'2-digit', second:'2-digit', fractionalSecondDigits: 3 });
            
            let yBadge = '<span class="text-slate-500">[HOLD]</span>';
            if (Y === 1) {
                const execTag = state.ibkrAutoTrading ? '<span class="text-amber-300 bg-amber-500/20 px-1 rounded ml-1 font-bold">[IBKR BUY]</span>' : '<span class="text-slate-500 ml-1">[DISARMED]</span>';
                yBadge = `<span class="text-emerald-400 font-bold">[BUY]</span>${execTag}`;
            } else if (Y === -1) {
                const execTag = state.ibkrAutoTrading ? '<span class="text-amber-300 bg-amber-500/20 px-1 rounded ml-1 font-bold">[IBKR SELL]</span>' : '<span class="text-slate-500 ml-1">[DISARMED]</span>';
                yBadge = `<span class="text-red-400 font-bold">[SELL]</span>${execTag}`;
            }

            entry.innerHTML = `
                <span class="text-slate-500 shrink-0">${timeStr}</span>
                <span class="text-cyan-400 font-bold shrink-0">${sym}</span>
                <span class="text-slate-300 truncate">c:${tick.c.toFixed(2)}</span>
                ${yBadge}
            `;

            logContainer.prepend(entry);
            if (logContainer.children.length > 50) logContainer.removeChild(logContainer.lastChild);
        }

        function simulateIncomingTicks() {
            if (!state.isStreaming) return;

            SYMBOLS.forEach(sym => {
                const stock = state.stocks[sym];
                const last = stock.history[stock.history.length - 1];

                const shock = (Math.random() - 0.495) * (last.c * 0.0025);
                const newPrice = Math.max(1, last.c + shock);

                const newTick = {
                    c: newPrice,
                    d: newPrice - stock.basePrice,
                    dp: ((newPrice - stock.basePrice) / stock.basePrice) * 100,
                    h: Math.max(last.h, newPrice),
                    l: Math.min(last.l, newPrice),
                    o: last.o,
                    pc: stock.basePrice,
                    t: Date.now()
                };

                stock.history.push(newTick);
                if (stock.history.length > 60) stock.history.shift();

                computeQuantFeatures(sym);

                if (sym === state.activeSymbol) {
                    appendStreamLog(sym, newTick, stock.signal);
                }
            });

            renderHeatmap();
            updateDashboardUI();
        }

        // Apply visual styling based on initial disk state
        function applyIbkrVisuals(isActive) {
            const containerBox = document.getElementById('ibkrContainerBox');
            const label = document.getElementById('ibkrStatusLabel');
            const toggle = document.getElementById('ibkrToggle');

            toggle.checked = isActive;

            if (isActive) {
                containerBox.className = 'w-full sm:w-auto flex items-center justify-between sm:justify-start gap-3 px-3.5 py-2 rounded-xl bg-gradient-to-r from-emerald-950/60 via-slate-900 to-emerald-950/60 border-2 border-emerald-500 shadow-lg shadow-emerald-950/50 transition-all duration-300';
                label.innerText = 'AUTO-TRADING ACTIVE';
                label.className = 'text-xs font-mono font-extrabold text-emerald-400 tracking-wide';
            } else {
                containerBox.className = 'w-full sm:w-auto flex items-center justify-between sm:justify-start gap-3 px-3.5 py-2 rounded-xl bg-gradient-to-r from-slate-950 via-slate-900 to-slate-950 border-2 border-red-500/50 shadow-lg transition-all duration-300';
                label.innerText = 'AUTO-TRADING OFF';
                label.className = 'text-xs font-mono font-extrabold text-red-400 tracking-wide';
            }
        }

        // Interactive Brokers Toggle Switch Listener + Disk Storage (localStorage)
        document.getElementById('ibkrToggle').addEventListener('change', (e) => {
            state.ibkrAutoTrading = e.target.checked;
            
            // Write ON or OFF preference directly to browser disk
            localStorage.setItem('quantstream_ibkr_autotrading', state.ibkrAutoTrading);

            applyIbkrVisuals(state.ibkrAutoTrading);
        });

        document.getElementById('openAddStockModalBtn').addEventListener('click', () => {
            document.getElementById('addStockModal').classList.remove('hidden');
        });

        document.getElementById('closeAddStockModalBtn').addEventListener('click', () => {
            document.getElementById('addStockModal').classList.add('hidden');
        });

        document.getElementById('cancelModalBtn').addEventListener('click', () => {
            document.getElementById('addStockModal').classList.add('hidden');
        });

        document.getElementById('addStockForm').addEventListener('submit', (e) => {
            e.preventDefault();
            const ticker = document.getElementById('newTickerInput').value.trim().toUpperCase();
            const name = document.getElementById('newNameInput').value.trim();
            const price = parseFloat(document.getElementById('newPriceInput').value);

            if (ticker && name && price > 0) {
                if (!SYMBOLS.includes(ticker)) {
                    SYMBOLS.push(ticker);
                    initStock(ticker, name, price);
                    computeQuantFeatures(ticker);
                    selectActiveStock(ticker);
                } else {
                    alert("Ticker already exists in heatmap.");
                }
                document.getElementById('addStockModal').classList.add('hidden');
                document.getElementById('addStockForm').reset();
            }
        });

        document.getElementById('signalFilterSelect').addEventListener('change', (e) => {
            state.filterSignal = e.target.value;
            renderHeatmap();
        });

        document.getElementById('heatmapSortSelect').addEventListener('change', (e) => {
            state.sortBy = e.target.value;
            renderHeatmap();
        });

        document.getElementById('toggleStreamBtn').addEventListener('click', () => {
            state.isStreaming = !state.isStreaming;
            const text = document.getElementById('toggleStreamText');
            const icon = document.getElementById('toggleStreamIcon');
            const badge = document.getElementById('statusBadge');
            const statusText = document.getElementById('statusText');

            if (state.isStreaming) {
                text.innerText = 'Pause Stream';
                icon.setAttribute('data-lucide', 'pause');
                badge.className = 'flex items-center gap-1.5 sm:gap-2 px-2.5 sm:px-3 py-1.5 rounded-full bg-emerald-500/10 border border-emerald-500/30 text-emerald-400 text-[10px] sm:text-xs font-mono';
                statusText.innerText = 'STREAMING WS';
            } else {
                text.innerText = 'Resume Stream';
                icon.setAttribute('data-lucide', 'play');
                badge.className = 'flex items-center gap-1.5 sm:gap-2 px-2.5 sm:px-3 py-1.5 rounded-full bg-amber-500/10 border border-amber-500/30 text-amber-400 text-[10px] sm:text-xs font-mono';
                statusText.innerText = 'PAUSED';
            }
            lucide.createIcons();
        });

        document.getElementById('shockCrashBtn').addEventListener('click', () => {
            SYMBOLS.forEach(sym => {
                const stock = state.stocks[sym];
                const last = stock.history[stock.history.length - 1];
                last.c -= last.c * 0.025;
                last.l = Math.min(last.l, last.c);
                computeQuantFeatures(sym);
            });
            renderHeatmap();
            updateDashboardUI();
        });

        document.getElementById('shockRallyBtn').addEventListener('click', () => {
            SYMBOLS.forEach(sym => {
                const stock = state.stocks[sym];
                const last = stock.history[stock.history.length - 1];
                last.c += last.c * 0.025;
                last.h = Math.max(last.h, last.c);
                computeQuantFeatures(sym);
            });
            renderHeatmap();
            updateDashboardUI();
        });

        document.getElementById('sliderK').addEventListener('input', (e) => {
            state.k = parseInt(e.target.value);
            document.getElementById('valK').innerText = state.k;
            SYMBOLS.forEach(sym => computeQuantFeatures(sym));
            renderHeatmap();
            updateDashboardUI();
        });

        document.getElementById('sliderDelta').addEventListener('input', (e) => {
            state.delta = parseFloat(e.target.value);
            document.getElementById('valDelta').innerText = state.delta.toFixed(4);
            SYMBOLS.forEach(sym => computeQuantFeatures(sym));
            renderHeatmap();
            updateDashboardUI();
        });

        document.getElementById('sliderM').addEventListener('input', (e) => {
            state.m = parseInt(e.target.value);
            document.getElementById('valM').innerText = state.m;
            SYMBOLS.forEach(sym => computeQuantFeatures(sym));
            renderHeatmap();
            updateDashboardUI();
        });

        document.getElementById('clearLogBtn').addEventListener('click', () => {
            document.getElementById('streamLog').innerHTML = '';
        });

        setInterval(() => {
            const now = new Date();
            document.getElementById('utcClock').innerText = now.toUTCString().split(' ')[4] + ' UTC';
        }, 1000);

        window.onload = function() {
            lucide.createIcons();
            initCharts();

            // Apply disk-persisted state visually on boot
            applyIbkrVisuals(state.ibkrAutoTrading);

            SYMBOLS.forEach(sym => computeQuantFeatures(sym));

            renderHeatmap();
            updateDashboardUI();

            setInterval(simulateIncomingTicks, 750);
        };
		
// Interactive Brokers Toggle Switch Listener + Local Disk File Writer
// Interactive Brokers Toggle Switch Listener (Simple Browser Download Approach)
document.getElementById('ibkrToggle').addEventListener('change', (e) => {
    state.ibkrAutoTrading = e.target.checked;
    
    // Save preference in browser storage
    localStorage.setItem('quantstream_ibkr_autotrading', state.ibkrAutoTrading);
    applyIbkrVisuals(state.ibkrAutoTrading);

    // Create and download ON.txt or OFF.txt instantly
    const fileName = state.ibkrAutoTrading ? 'ON.txt' : 'OFF.txt';
    const fileContent = `Interactive Brokers Auto-Trading: ${state.ibkrAutoTrading ? 'ACTIVE' : 'OFF'}\nTimestamp: ${new Date().toISOString()}`;

    const blob = new Blob([fileContent], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = fileName;
    
    document.body.appendChild(a);
    a.click();
    
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
});
		
    </script>
<!-- Quantitative Trading Disclaimer -->
<section class="max-w-[1800px] mx-auto px-3 sm:px-5 pb-5">
        <div class="glass-panel rounded-xl p-3 sm:p-4 border border-cardborder text-xs sm:text-sm font-mono text-slate-300 leading-relaxed text-center">
            <span class="text-amber-400 font-bold uppercase">Disclaimer:</span> 
            QuantStream AI is an advanced quantitative multi-tick alpha prediction engine designed for analytical, research, and simulation purposes only. Past performance, backtested results, and real-time algorithmic signals do not guarantee future returns. Algorithmic trading involves substantial risk of loss and is not suitable for every investor. Otics Advanced Analytics, Inc. assumes no responsibility for financial losses incurred using this terminal.
        </div>
    </section>
	
<footer class="glass-panel border-t border-cardborder mt-8 py-6 px-4">
        <div class="max-w-[1800px] mx-auto flex flex-col sm:flex-row items-center justify-between gap-4 font-mono text-xs text-slate-400 text-center sm:text-left">
            <div class="flex items-center gap-2 justify-center">
                <span class="text-white font-semibold"><a href='https://www.otics.ca' target="_blank">Otics Advanced Analytics, Inc.</a></span>
                <span class="text-slate-600">|</span>
                <span>All Rights Reserved</span>
            </div>
            <div class="flex items-center gap-6 justify-center">
                <span class="text-slate-500">QuantStream AI Engine v3.0</span>
                <div class="flex items-center gap-1.5 text-emerald-400">
                    <span class="w-1.5 h-1.5 rounded-full bg-emerald-400"></span>
                    <span>Systems Operational</span>
                </div>
            </div>
        </div>
    </footer>	
</body>
</html>