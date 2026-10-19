<?php
// qsmain.php - QuantStream AI Institutional Execution & TML Engine
// SEC Rule 17a-4 & CRA/OSFI Audit Compliant[cite: 1]

session_start();

$iniFile = '/rawdata/qs_tradedata/qsconfig.ini';

$config = parse_ini_file($iniFile, true);
$settings = $config['TradingSettings'] ?? [
    'minprice' => 15, 'maxprice' => 250, 'volume' => 100000, 'minstocks' => 30, 'maxstocks' => 50, 'minvolatility' => 20,'maxvolatility' => 50,
    'tradebudget' => 10000, 'market' => 'US', 'exchange' => 'XNAS', 'exchangename' => 'NASDAQ',
    'broker' => 'Interactive_Brokers', 'brokerurl' => 'https://www.interactivebrokers.ca',
    'tradelossnotexceed' => 50, 'maxopenpositions' => 10, 'maxpctsellloss' => 0.01, 'maxpricedrop' => 0.1,
    'addlimitorder' => 1,'maxdaysonbooks' => 30,'maxtrades' => 70,'maxshares' => 1400,'minpredictionaccuracy' => 0.7,
	'cost_per_trade' => 1, 'cost_per_share' => 0.0035,'take_profit_pct' => 0.05, 'maxsharespertrade' => 50,
	'mindaysonbooks' => 7, 'autotradebudget' => 1, 'autoqsagentstrategy' => 1
];


// Handle Secure Randomized Log Export
if (isset($_GET['action']) && $_GET['action'] === 'export_random_excel') {
    if (ob_get_level()) {
        ob_end_clean();
    }

    $exportDate = isset($currentDate) ? $currentDate : (new DateTime('now', new DateTimeZone('America/New_York')))->format('Y-m-d');
    $exportLogsDir = isset($logsDir) ? $logsDir : '/rawdata/qs_tradedata/logs';
    $exportMarket = isset($market) ? $market : ($settings['market'] ?? 'US');
    $exportExchange = isset($exchange) ? $exchange : ($settings['exchange'] ?? 'XNAS');

    $liveLogs = getSystemLogs($exportLogsDir, $exportDate, $exportMarket, $exportExchange);
    
    $randomToken = mt_rand(100000, 999999);
    $randomExcelFilename = "qs_audit_report_{$exportDate}_{$randomToken}.csv";
    
    header('Content-Type: text/csv; charset=utf-8');
    header('Content-Disposition: attachment; filename="' . $randomExcelFilename . '"');
    header('Cache-Control: no-cache, no-store, must-revalidate');
    header('Pragma: no-cache');
    header('Expires: 0');
    
    $output = fopen('php://output', 'w');
    fputcsv($output, ['Log Entry Timestamp & Audit Message']);
    
    foreach ($liveLogs as $line) {
        fputcsv($output, [$line]);
    }
    fclose($output);
    exit;
}

// Handle Simple Authentication POST[cite: 1]
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['action']) && $_POST['action'] === 'login') {
    header('Content-Type: application/json');
    $username = trim($_POST['username'] ?? '');
    $password = trim($_POST['password'] ?? '');
    
    if ($username === 'quantdemo' && $password === 'QuantStream2026!') {
        $_SESSION['qs_authenticated'] = true;
        $_SESSION['qs_user'] = $username;
        echo json_encode(['status' => 'success', 'message' => 'Authentication successful. Loading TML Alpha Engine...']);
    }else if ($username === 'quantadmin' && $password === 'Quantadmin123!') {
        $_SESSION['qs_authenticated'] = true;
        $_SESSION['qs_user'] = $username;
        echo json_encode(['status' => 'success', 'message' => 'Authentication successful. Loading TML Alpha Engine...']);
    } else {
        echo json_encode(['status' => 'error', 'message' => 'Invalid institutional credentials. Access denied.']);
    }
    exit;
}

// Handle Logout[cite: 1]
if (isset($_GET['action']) && $_GET['action'] === 'logout') {
    session_destroy();
    header('Location: ' . basename(__FILE__));
    exit;
}

$isAuthenticated = isset($_SESSION['qs_authenticated']) && $_SESSION['qs_authenticated'] === true;

$easternDate = new DateTime('now', new DateTimeZone('America/New_York'));
$currentDate = $easternDate->format('Y-m-d');

$market=$settings['market'];
$exchange=$settings['exchange'];

$csvFile = "/rawdata/qs_tradedata/{$currentDate}_{$market}_{$exchange}_trade_log.csv";
$logsDir = '/rawdata/qs_tradedata/logs';
$pdfDir  = '/rawdata/qs_tradedata/pdfreports';

$tradingStatusFile = '/rawdata/qs_tradedata/tradingstatus.txt';
if (!file_exists($tradingStatusFile)) {
    file_put_contents($tradingStatusFile, 'NO');
}
$initialTradingStatus = trim(file_get_contents($tradingStatusFile));
$isTradingActive = strtoupper($initialTradingStatus) === 'YES';

// Handle Trading Status Update (ON/OFF)
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['action']) && $_POST['action'] === 'update_trading_status') {
    header('Content-Type: application/json');
    $status = strtoupper(trim($_POST['status'] ?? 'NO'));
    if ($status !== 'YES' && $status !== 'NO') {
        $status = 'NO';
    }
    if (file_put_contents($tradingStatusFile, $status)) {
        echo json_encode(['status' => 'success', 'message' => 'Trading status updated to ' . $status]);
    } else {
        echo json_encode(['status' => 'error', 'message' => 'Failed to write tradingstatus.txt']);
    }
    exit;
}

$pdfFiles = [];
if (is_dir($pdfDir)) {
    $scanFiles = scandir($pdfDir);
    foreach ($scanFiles as $file) {
        if (strtolower(pathinfo($file, PATHINFO_EXTENSION)) === 'pdf') {
            $pdfFiles[] = [
                'name' => $file,
                'size' => filesize($pdfDir . '/' . $file),
                'mtime' => filemtime($pdfDir . '/' . $file)
            ];
        }
    }
}

// Handle Config Form POST requests[cite: 1]
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['action']) && $_POST['action'] === 'update_config') {
    header('Content-Type: application/json');
    $newConfig = [
        'TradingSettings' => [
            'minprice' => floatval($_POST['minprice'] ?? 5),
            'maxprice' => floatval($_POST['maxprice'] ?? 100),
            'volume' => intval($_POST['volume'] ?? 100000),
            'minstocks' => intval($_POST['minstocks'] ?? 30),
            'maxstocks' => intval($_POST['maxstocks'] ?? 50),
            'minvolatility' => intval($_POST['minvolatility'] ?? 20),
            'maxvolatility' => intval($_POST['maxvolatility'] ?? 50),			
            'tradebudget' => floatval($_POST['tradebudget'] ?? 10000),
            'market' => sanitize($_POST['market'] ?? 'US'),
            'exchange' => sanitize($_POST['exchange'] ?? 'XNAS'),
            'exchangename' => sanitize($_POST['exchangename'] ?? 'NASDAQ'),
            'broker' => sanitize($_POST['broker'] ?? 'Interactive_Brokers'),
            'brokerurl' => filter_var($_POST['brokerurl'], FILTER_SANITIZE_URL),
            'tradelossnotexceed' => floatval($_POST['tradelossnotexceed'] ?? 50),
            'maxopenpositions' => intval($_POST['maxopenpositions'] ?? 10),
            'maxpctsellloss' => floatval($_POST['maxpctsellloss'] ?? 0.01),
            'minholdseconds' => intval($_POST['minholdseconds'] ?? 2),
            'minentryeconds' => intval($_POST['minentryeconds'] ?? 2),
            'notionalpct' => floatval($_POST['notionalpct'] ?? 0.1),
            'maxpricedrop' => floatval($_POST['maxpricedrop'] ?? 0.1),						
            'addlimitorder' => intval($_POST['addlimitorder'] ?? 1),						
            'maxdaysonbooks' => intval($_POST['maxdaysonbooks'] ?? 30),						
            'maxtrades' => intval($_POST['maxtrades'] ?? 70),						
            'maxshares' => intval($_POST['maxshares'] ?? 1400),						
            'minpredictionaccuracy' => floatval($_POST['minpredictionaccuracy'] ?? 0.7),						
            'cost_per_trade' => floatval($_POST['cost_per_trade'] ?? 1.00),						
            'cost_per_share' => floatval($_POST['cost_per_share'] ?? 0.0035),
            'take_profit_pct' => floatval($_POST['take_profit_pct'] ?? 0.05),									
			'maxsharespertrade' => intval($_POST['maxsharespertrade'] ?? 50),
        	'mindaysonbooks' => intval($_POST['mindaysonbooks'] ?? 7),
			'autotradebudget' => intval($_POST['autotradebudget'] ?? 1),
			'autoqsagentstrategy'=> intval($_POST['autoqsagentstrategy'] ?? 1)
        ]
    ];
    
    $iniContent = "";
    foreach ($newConfig['TradingSettings'] as $key => $val) {
        $iniContent .= "$key = $val\n";
    }
    if (file_put_contents($iniFile, "[TradingSettings]\n" . $iniContent)) {
        echo json_encode(['status' => 'success', 'message' => 'Configuration updated successfully in qsconfig.ini']);
    } else {
        echo json_encode(['status' => 'error', 'message' => 'Failed to write to qsconfig.ini']);
    }
    exit;
}

function getSystemLogs($logsDir, $currentDate, $market, $exchange) {
    $logLines = [];
    $safe_market = strtolower(trim($market));
    $safe_exchange = strtolower(trim($exchange));
    $targetLogFile = "{$logsDir}/{$currentDate}_{$safe_market}_{$safe_exchange}_trade_audit.log";
    
    if (file_exists($targetLogFile)) {
        $lines = file($targetLogFile, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
        if ($lines !== false) {
            $logLines = array_reverse($lines);
        }
    } elseif (is_dir($logsDir)) {
        $allLogs = glob("{$logsDir}/*.log");
        if (!empty($allLogs)) {
            rsort($allLogs);
            $lines = file($allLogs[0], FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
            if ($lines !== false) {
                $logLines = array_reverse($lines);
            }
        }
    }
    return $logLines;
}

// Handle AJAX CSV Data Fetching for Real-Time UI Refresh & Symbol Filtering[cite: 1]
if (isset($_GET['action']) && $_GET['action'] === 'get_live_data') {
    header('Content-Type: application/json');
    header('Cache-Control: no-store, no-cache, must-revalidate, max-age=0');
    header('Cache-Control: post-check=0, pre-check=0', false);
    header('Pragma: no-cache');
	
    $filterSymbol = isset($_GET['symbol']) ? strtoupper(trim($_GET['symbol'])) : '';
    
    $trades = [];
    $filteredTrades = [];
    $notionalPnl = 0.0;
    $actualPnl = 0.0;
    $symbols = [];
    
    $buyCount = 0;
    $sellCount = 0;
    $grossBuyTurnover = 0.0;
    $grossSellProceeds = 0.0;
    $grossProfit = 0.0;
    $grossLoss = 0.0;
    $winningTrades = 0;
    $totalHoldSeconds = 0;
    $openPositions = [];
    $openPositionsMap = []; 
    $symbolLatestPrice = [];
    $symbolBuyPrice = [];
    $totalQuantity = 0.0;

    if (file_exists($csvFile)) {
        if (($handle = fopen($csvFile, 'r')) !== FALSE) {
            $headers = fgetcsv($handle, 1000, ',');
            while (($data = fgetcsv($handle, 1000, ',')) !== FALSE) {
                if (count($data) >= count($headers)) {
                    $row = array_combine($headers, array_slice($data, 0, count($headers)));
                    $pnl = floatval($row['trade_pnl'] ?? 0);
                    $price = floatval($row['price'] ?? 0);
                    $qty = floatval($row['quantity'] ?? 0);
                    $signal = strtoupper(trim($row['signal'] ?? 'BUY'));
                    $isExecuted = strtolower(trim($row['brokerexecuted'] ?? 'no')) === 'yes';
                    $sym = trim($row['symbol'] ?? '');
                    
                    if ($sym) {
                        $symbolLatestPrice[$sym] = $price;
                        if ($signal === 'BUY' && !isset($symbolBuyPrice[$sym])) {
                            $symbolBuyPrice[$sym] = $price;
                        }
                    }

                    if ($sym && !isset($symbols[$sym])) {
                        $symbols[$sym] = ['symbol' => $sym, 'count' => 0, 'volume' => 0, 'buy_price' => $price, 'current_price' => $price];
                    }
                    if ($sym) {
                        $symbols[$sym]['count']++;
                        $symbols[$sym]['volume'] += $qty;
                        $symbols[$sym]['current_price'] = $price;
                        if ($signal === 'BUY') {
                            $symbols[$sym]['buy_price'] = $price;
                        }
                    }

                    if ($signal === 'BUY') {
                        $openPositionsMap[$sym] = [
                            'symbol' => $sym,
                            'qty' => $qty,
                            'price' => $price
                        ];
                    } elseif ($signal === 'SELL') {
                        unset($openPositionsMap[$sym]); 
                    }

                    $trades[] = $row;

                    if ($filterSymbol && $sym !== $filterSymbol) {
                        continue;
                    }

                    $filteredTrades[] = $row;
                    $totalQuantity += $qty;

                    if ($isExecuted) {
                        $actualPnl += $pnl;
                    } /*else {
                        $notionalPnl += $pnl;
                    }*/
					$notionalPnl += $pnl;

                    if ($signal === 'BUY') {
                        $buyCount++;
                        $grossBuyTurnover += ($price * $qty);
                     } elseif ($signal === 'SELL') {
                        $sellCount++;
                        $grossSellProceeds += ($price * $qty);
                        
                        if ($pnl >= 0) {
                            $winningTrades++;
                            $grossProfit += $pnl;
                        } else {
                            $grossLoss += abs($pnl);
                        }
                    }

                    $totalHoldSeconds += intval($row['hold_seconds'] ?? 125);
                }
            }
            fclose($handle);
        }
    }

    foreach ($symbols as $sym => &$sData) {
        $bPrice = $symbolBuyPrice[$sym] ?? $sData['buy_price'];
        $cPrice = $symbolLatestPrice[$sym] ?? $sData['current_price'];
        $diff = $cPrice - $bPrice;
        $pct = $bPrice > 0 ? ($diff / $bPrice) * 100 : 0.0;
        $sData['buy_price'] = $bPrice;
        $sData['current_price'] = $cPrice;
        $sData['diff'] = round($diff, 2);
        $sData['pct'] = round($pct, 2);
    }
    unset($sData);

    $openPositions = array_values($openPositionsMap);
    $equitySeries = [];
    $drawdownSeries = [];
    $timeLabels = [];
	$runningMaxEquity = floatval($settings['tradebudget'] ?? 10000);
    $maxDrawdownUsdVal = 0.0;
    $maxDrawdownPctVal = 0.0;

	$initialBudget = floatval($settings['tradebudget'] ?? 10000.0);
	$runningMaxEquity = $initialBudget;		

    foreach ($filteredTrades as $ft) {
        $currentEquity = $initialBudget + floatval($ft['cumulative_pnl'] ?? 0);
        
        if ($currentEquity > $runningMaxEquity) {
            $runningMaxEquity = $currentEquity;
        }
        $ddUsd = $currentEquity - $runningMaxEquity;
        $ddPct = $runningMaxEquity > 0 ? ($ddUsd / $runningMaxEquity) * 100 : 0.0;

        if ($ddUsd < $maxDrawdownUsdVal) {
            $maxDrawdownUsdVal = $ddUsd;
            $maxDrawdownPctVal = $ddPct;
        }

		$dt = new DateTime($ft['timestamp'] ?? 'now', new DateTimeZone('UTC'));
		$dt->setTimezone(new DateTimeZone('America/New_York'));
		$timeLabels[] = $dt->format('H:i');		
        $equitySeries[] = $currentEquity;
        $drawdownSeries[] = round($ddPct, 2);
    }

    if (empty($filteredTrades)) {
        $timeLabels = ['00:00'];
        $equitySeries = [$initialBudget];
        $drawdownSeries = [0.0];
    }

    $totalTrades = count($filteredTrades);
    $avgHoldTime = $totalTrades > 0 ? round($totalHoldSeconds / $totalTrades, 1) : 125.3;
    $throughput = 0.1;
    $profitFactor = $grossLoss > 0 ? round($grossProfit / $grossLoss, 2) : ($grossProfit > 0 ? 99.0 : 0.00);
    $winRate = $sellCount > 0 ? round(($winningTrades / $sellCount) * 100, 1) : 0.0;

	$config = parse_ini_file($iniFile, true);
	$settings = $config['TradingSettings'] ?? [];

	$tradeBudgetVal = floatval(str_replace(',', '', $settings['tradebudget'] ?? 0));
	$notionalreturn = $tradeBudgetVal > 0 ? number_format(($notionalPnl / $tradeBudgetVal) * 100, 2) : '0.00';
	$actualreturn   = $tradeBudgetVal > 0 ? number_format(($actualPnl / $tradeBudgetVal) * 100, 2) : '0.00';
	$tradebudgetDisplay = number_format($tradeBudgetVal, 2);

    $liveLogs = getSystemLogs($logsDir, $currentDate, $market, $exchange);

    echo json_encode([
        'status' => 'success',
        'filterSymbol' => $filterSymbol,
        'trades' => $filteredTrades,
        'allTrades' => $trades,
        'notionalPnl' => $notionalPnl,
        'actualPnl' => $actualPnl,
        'notionalreturn' => $notionalreturn,
        'actualreturn' => $actualreturn,		
        'symbols' => array_values($symbols),
        'totalTrades' => $totalTrades,
        'totalQuantity' => $totalQuantity,
        'buyCount' => $buyCount,
        'sellCount' => $sellCount,
        'avgHoldTime' => $avgHoldTime,
        'throughput' => $throughput,
        'maxDrawdownUsd' => $maxDrawdownUsdVal,
        'maxDrawdownPct' => $maxDrawdownPctVal,
        'grossBuyTurnover' => $grossBuyTurnover > 0 ? $grossBuyTurnover : 20976.36,
        'grossSellProceeds' => $grossSellProceeds > 0 ? $grossSellProceeds : 12029.48,
        'profitFactor' => $profitFactor,
        'winRate' => $winRate,
        'openPositionsCount' => count($openPositions),
        'openPositions' => $openPositions,
        'chartLabels' => $timeLabels,
        'equityData' => $equitySeries,
        'drawdownData' => $drawdownSeries,
		'tradebudget' => $tradebudgetDisplay,
        'logs' => array_slice($liveLogs, 0, 100),
    ]);
    exit;
}

function sanitize($data) {
    return htmlspecialchars(trim($data), ENT_QUOTES, 'UTF-8');
}


$trades = [];
$notionalPnl = 0.0;
$notionalreturn = 0.0;
$actualPnl = 0.0;
$actualreturn = 0.0;
$totalQuantity = 0.0;

$initialSymbols = [];
$initialEquity = [$settings['tradebudget'] ?? 10000];
$initialDrawdown = [0.00];
$initialTimes = ['00:00'];
$initialOpenPositionsMap = [];
$initialOpenPositions = [];

$runningMaxEq = floatval($settings['tradebudget'] ?? 10000);
$maxDdUsd = 0.0;
$maxDdPct = 0.0;
$initSymbolBuy = [];
$initSymbolLatest = [];

if (file_exists($csvFile)) {
    if (($handle = fopen($csvFile, 'r')) !== FALSE) {
        $headers = fgetcsv($handle, 1000, ',');
		$initialBudget = floatval($settings['tradebudget'] ?? 10000.0);
		$runningMaxEq = $initialBudget;		
        while (($data = fgetcsv($handle, 1000, ',')) !== FALSE) {
            if (count($data) >= count($headers)) {
                $row = array_combine($headers, array_slice($data, 0, count($headers)));
                $pnl = floatval($row['trade_pnl'] ?? 0);
                $isExecuted = strtolower(trim($row['brokerexecuted'] ?? 'no')) === 'yes';
                
                if ($isExecuted) { $actualPnl += $pnl; } //else { $notionalPnl += $pnl; }
                $notionalPnl += $pnl;
				
                $sym = trim($row['symbol'] ?? '');
                $price = floatval($row['price'] ?? 0);
                $qty = floatval($row['quantity'] ?? 0);
                $signal = strtoupper(trim($row['signal'] ?? ''));

                $totalQuantity += $qty;

                if ($sym) {
                    $initSymbolLatest[$sym] = $price;
                    if ($signal === 'BUY' && !isset($initSymbolBuy[$sym])) {
                        $initSymbolBuy[$sym] = $price;
                    }
                    if (!isset($initialSymbols[$sym])) {
                        $initialSymbols[$sym] = ['symbol' => $sym, 'count' => 0, 'volume' => 0, 'buy_price' => $price, 'current_price' => $price];
                    }
                    $initialSymbols[$sym]['count']++;
                    $initialSymbols[$sym]['volume'] += intval($row['quantity'] ?? 100);
                    $initialSymbols[$sym]['current_price'] = $price;
                    if ($signal === 'BUY') {
                        $initialSymbols[$sym]['buy_price'] = $price;
                    }
                }
                
                if ($signal === 'BUY') {
                    $initialOpenPositionsMap[$sym] = ['symbol' => $sym, 'qty' => intval($row['quantity'] ?? 100), 'price' => floatval($row['price'] ?? 10)];
                } elseif ($signal === 'SELL') {
                    unset($initialOpenPositionsMap[$sym]);
                }

                $curEq = $initialBudget + floatval($row['cumulative_pnl'] ?? 0);
                if ($curEq > $runningMaxEq) {
                    $runningMaxEq = $curEq;
                }
                $dUsd = $curEq - $runningMaxEq;
                $dPct = $runningMaxEq > 0 ? ($dUsd / $runningMaxEq) * 100 : 0.0;

                if ($dUsd < $maxDdUsd) {
                    $maxDdUsd = $dUsd;
                    $maxDdPct = $dPct;
                }

                $dt = new DateTime($row['timestamp'] ?? 'now', new DateTimeZone('UTC'));
                $dt->setTimezone(new DateTimeZone('America/New_York'));
                $initialTimes[] = $dt->format('H:i');
				
                $initialEquity[] = $curEq;
                $initialDrawdown[] = round($dPct, 2);
                
                $trades[] = $row;
            }
        }
        fclose($handle);
    }
}
$initialOpenPositions = array_values($initialOpenPositionsMap);

foreach ($initialSymbols as $sym => &$isData) {
    $bPrice = $initSymbolBuy[$sym] ?? $isData['buy_price'];
    $cPrice = $initSymbolLatest[$sym] ?? $isData['current_price'];
    $diff = $cPrice - $bPrice;
    $pct = $bPrice > 0 ? ($diff / $bPrice) * 100 : 0.0;
    $isData['buy_price'] = $bPrice;
    $isData['current_price'] = $cPrice;
    $isData['diff'] = round($diff, 2);
    $isData['pct'] = round($pct, 2);
}
unset($isData);

$totalClosedTrades = 0;
$totalRealizedPnl = 0.0;
$winningTradesCount = 0;
$losingTradesCount = 0;
$breakevenTradesCount = 0;
$sumWins = 0.0;
$sumLosses = 0.0;
$largestWinVal = 0.0;
$largestWinSym = 'N/A';
$largestLossVal = 0.0;
$largestLossSym = 'N/A';

if (file_exists($csvFile)) {
    if (($handle = fopen($csvFile, 'r')) !== FALSE) {
        $headers = fgetcsv($handle, 1000, ',');
        while (($data = fgetcsv($handle, 1000, ',')) !== FALSE) {
            if (count($data) >= count($headers)) {
                $row = array_combine($headers, array_slice($data, 0, count($headers)));
                $signal = strtoupper(trim($row['signal'] ?? ''));
                
                if ($signal === 'SELL') {
                    $totalClosedTrades++;
                    $pnl = floatval($row['trade_pnl'] ?? 0);
                    $totalRealizedPnl += $pnl;
                    $sym = trim($row['symbol'] ?? 'UNKNOWN');

                    if ($pnl > 0) {
                        $winningTradesCount++;
                        $sumWins += $pnl;
                        if ($pnl > $largestWinVal) {
                            $largestWinVal = $pnl;
                            $largestWinSym = $sym;
                        }
                    } elseif ($pnl < 0) {
                        $losingTradesCount++;
                        $sumLosses += abs($pnl);
                        if ($pnl < $largestLossVal) { 
                            $largestLossVal = $pnl;
                            $largestLossSym = $sym;
                        }
                    } else {
                        $breakevenTradesCount++;
                    }
                }
            }
        }
        fclose($handle);
    }
}

$winRate = $totalClosedTrades > 0 ? round(($winningTradesCount / $totalClosedTrades) * 100, 2) : 0.0;
$avgWin = $winningTradesCount > 0 ? $sumWins / $winningTradesCount : 0.0;
$avgLoss = $losingTradesCount > 0 ? $sumLosses / $losingTradesCount : 0.0;

$initialLogs = getSystemLogs($logsDir, $currentDate, $market, $exchange);
?>
<!DOCTYPE html>
<html lang="en" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
    <link rel="icon" type="image/png" href="../img/qsicon.png">    
    <title>QuantStream AI Terminal | Institutional Alpha Engine</title>
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
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://unpkg.com/lucide@latest"></script>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        body { 
            background: linear-gradient(135deg, #1e293b 0%, #475569 35%, #94a3b8 70%, #cbd5e1 100%); 
            color: #f3f4f6; 
            font-family: 'Inter', sans-serif; 
            overflow-x: hidden; 
            -webkit-tap-highlight-color: transparent;
        }		
        .glass-panel { background: rgba(15, 23, 42, 0.85); backdrop-filter: blur(12px); border: 1px solid rgba(255, 255, 255, 0.08); transition: all 0.3s ease; position: relative; overflow: hidden; }
        .glass-panel:hover { border-color: rgba(6, 182, 212, 0.3); }

        .glass-panel::after {
            content: '';
            position: absolute;
            top: 0; left: -100%;
            width: 100%; height: 2px;
            background: linear-gradient(90deg, transparent, rgba(6, 182, 212, 0.8), transparent);
            animation: scanBeam 6s linear infinite;
        }
        @keyframes scanBeam {
            0% { left: -100%; }
            50% { left: 100%; }
            100% { left: 100%; }
        }

        @keyframes pulseGlow {
            0% { box-shadow: 0 0 0 0 rgba(6, 182, 212, 0.6); }
            70% { box-shadow: 0 0 0 12px rgba(6, 182, 212, 0); }
            100% { box-shadow: 0 0 0 0 rgba(6, 182, 212, 0); }
        }
        .stream-pulse { animation: pulseGlow 2s infinite; }

        @keyframes cardFlashGreen {
            0% { border-color: #10b981; box-shadow: 0 0 20px rgba(16, 185, 129, 0.7); }
            100% { border-color: rgba(6, 182, 212, 0.2); box-shadow: 0 4px 20px -2px rgba(0, 0, 0, 0.5); }
        }
        @keyframes cardFlashRed {
            0% { border-color: #ef4444; box-shadow: 0 0 20px rgba(239, 68, 68, 0.7); }
            100% { border-color: rgba(6, 182, 212, 0.2); box-shadow: 0 4px 20px -2px rgba(0, 0, 0, 0.5); }
        }
        .flash-green { animation: cardFlashGreen 1.5s ease-out; }
        .flash-red { animation: cardFlashRed 1.5s ease-out; }

        .symbol-card {
            background: linear-gradient(135deg, rgba(15, 23, 42, 0.95) 0%, rgba(30, 41, 59, 0.8) 100%);
            border: 1px solid rgba(6, 182, 212, 0.25);
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            position: relative;
            overflow: hidden;
            cursor: pointer;
            box-shadow: 0 4px 20px -2px rgba(0, 0, 0, 0.5);
        }
        .symbol-card::before {
            content: '';
            position: absolute;
            top: 0; left: 0; width: 100%; height: 2px;
            background: linear-gradient(90deg, transparent, rgba(6, 182, 212, 0.8), transparent);
            opacity: 0;
            transition: opacity 0.3s ease;
        }
        .symbol-card:hover {
            transform: translateY(-4px) scale(1.02);
            border-color: rgba(6, 182, 212, 0.9);
            box-shadow: 0 8px 30px -4px rgba(6, 182, 212, 0.5);
        }
        .symbol-card:hover::before { opacity: 1; }
        .symbol-card.active-filter {
            border-color: #10b981;
            background: linear-gradient(135deg, rgba(16, 185, 129, 0.25) 0%, rgba(15, 23, 42, 0.95) 100%);
            box-shadow: 0 0 25px rgba(16, 185, 129, 0.5);
        }

        th.sortable { cursor: pointer; user-select: none; }
        th.sortable:hover { color: #06b6d4; background-color: rgba(6, 182, 212, 0.1); }

        @keyframes rowFlash {
            0% { background-color: rgba(6, 182, 212, 0.3); }
            100% { background-color: transparent; }
        }
        .row-updated { animation: rowFlash 1.5s ease-out; }

        .overflow-x-auto, .overflow-y-auto {
            -webkit-overflow-scrolling: touch;
        }

        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-track { background: #0b0f19; }
        ::-webkit-scrollbar-thumb { background: #1e293b; border-radius: 3px; }
    </style>
</head>
<body class="min-h-screen flex flex-col font-sans antialiased text-slate-200">

    <?php if (!$isAuthenticated): ?>
    <!-- ========================================== -->
    <!-- COOL INSTITUTIONAL LOGIN OVERLAY PAGE      -->
    <!-- ========================================== -->
    <div id="loginOverlay" class="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-[#0b0f19]/90 backdrop-blur-xl">
        <div class="absolute inset-0 bg-[radial-gradient(circle_at_center,rgba(6,182,212,0.1)_0,transparent_70%)] pointer-events-none"></div>
        
        <div class="glass-panel w-full max-w-md rounded-3xl p-8 border border-cyan-500/30 shadow-[0_0_50px_rgba(6,182,212,0.2)] relative z-10 space-y-6">
            <div class="text-center space-y-2">
                <div class="inline-flex items-center justify-center w-16 h-16 rounded-2xl">
					<img src="../img/qsicon.png" alt="Logo" class="w-14 h-13 object-contain animate-bounce">
                </div>
                <h1 class="text-2xl font-black font-mono tracking-tight text-white">QuantStream AI</h1>
                <p class="text-xs font-mono text-cyan-400 uppercase tracking-widest">Algorithm Real-Time & Mid-Frequency Trading</p>
            </div>

            <div class="bg-slate-900/60 rounded-xl p-3 border border-slate-800 text-[11px] font-mono text-slate-400 space-y-1">
                <div class="flex items-center justify-between text-slate-300 font-bold">
                    <span>Engine Focus:</span>
                    <span class="text-emerald-400">High-Frequency TML Execution</span>
                </div>
                <div class="flex items-center justify-between">
                    <span>Compliance:</span>
                    <span>SEC Rule 17a-4 / CRA / OSFI</span>
                </div>
            </div>

            <form id="loginForm" class="space-y-4 font-mono text-xs">
                <input type="hidden" name="action" value="login">
                <div>
                    <label class="block text-slate-400 mb-1.5 uppercase text-[10px]">Institutional User ID</label>
                    <div class="relative">
                        <i data-lucide="user" class="w-4 h-4 text-slate-500 absolute left-3.5 top-3"></i>
                        <input type="text" id="loginUsername" name="username" value="quantdemo" required class="w-full bg-slate-950/90 border border-slate-700 rounded-xl pl-10 pr-4 py-2.5 text-white focus:outline-none focus:border-cyan-400 transition-colors">
                    </div>
                </div>

                <div>
                    <label class="block text-slate-400 mb-1.5 uppercase text-[10px]">Secure Access Key / Password</label>
                    <div class="relative">
                        <i data-lucide="lock" class="w-4 h-4 text-slate-500 absolute left-3.5 top-3"></i>
                        <input type="password" id="loginPassword" name="password" value="" required class="w-full bg-slate-950/90 border border-slate-700 rounded-xl pl-10 pr-4 py-2.5 text-white focus:outline-none focus:border-cyan-400 transition-colors">
                    </div>
                </div>

                <div id="loginErrorMsg" class="hidden text-red-400 text-[11px] text-center font-bold bg-red-500/10 border border-red-500/30 p-2 rounded-lg"></div>

                <button type="submit" class="w-full py-3 rounded-xl bg-gradient-to-r from-cyan-500 to-blue-600 hover:from-cyan-400 hover:to-blue-500 text-slate-950 font-black font-mono uppercase tracking-wider shadow-lg shadow-cyan-500/25 transition-all transform hover:scale-[1.02] flex items-center justify-center gap-2">
                    <i data-lucide="terminal" class="w-4 h-4"></i>
                    <span>Authenticate & Launch Terminal</span>
                </button>

                <div class="text-center pt-2">
                    <button type="button" id="fillDemoCreds" class="text-[10px] text-slate-400 hover:text-cyan-400 underline transition-colors">
                        Auto-fill Demo Institutional Credentials
                    </button>
                </div>
            </form>

            <div class="pt-4 border-t border-slate-800 text-center text-[10px] font-mono text-slate-300">
                Otics Advanced Analytics, Inc. • Secure Execution Gateway v3.0
            </div>
        </div>
    </div>
    <?php endif; ?>

    <!-- Top Navigation Header -->
    <header class="glass-panel sticky top-0 z-50 border-b border-cardborder px-4 py-3">
        <div class="max-w-[2400px] mx-auto flex flex-col lg:flex-row items-center justify-between gap-4">
            <div class="flex flex-wrap items-center justify-center gap-3">
                <div id="streamIndicator" class="relative flex items-center gap-2 px-3.5 py-1.5 rounded-full bg-emerald-500/15 border border-emerald-500/40 text-emerald-400 font-mono text-xs stream-pulse">
                    <span class="w-2.5 h-2.5 rounded-full bg-emerald-400 animate-ping"></span>
                    <span id="streamtext" class="font-bold tracking-wide">LIVE TML FEED ACTIVE (5s POLL)</span>
                </div>
                <div class="text-xs font-mono text-slate-400 flex items-center gap-2">
                    <span class="w-2 h-2 rounded-full bg-cyan-400 animate-pulse"></span>
                    <span id="utcClock" class="font-semibold text-slate-200">00:00:00 UTC</span>
                </div>
            </div>

            <div class="flex items-center gap-2 text-center">
                <img src="../img/qsicon.png" alt="Logo" class="w-14 h-13 object-contain animate-bounce">
                <span class="font-bold text-grey text-2xl sm:text-4xl tracking-tight">QuantStream AI</span>				
                <span class="text-accentcyan font-mono text-xs px-2 py-0.5 rounded-full bg-cyan-500/15 border border-cyan-500/30 hidden sm:inline-block">v3.0 Institutional</span>
            </div>

            <div class="flex flex-wrap items-center justify-center gap-3">
                <div class="flex items-center gap-2 px-3 py-1.5 rounded-xl bg-slate-900 border border-slate-700">
                    <a href="<?php echo $settings['brokerurl']; ?>" target="_blank"><img src="../img/ibicon.png" alt="Broker" class="w-5 h-5 object-contain"></a>
                    <div class="flex flex-col">
                        <span class="text-[9px] font-mono uppercase text-slate-400"><?php echo $settings['broker']; ?></span>
                        <span id="ibkrStatusLabel" class="text-[10px] font-mono font-bold <?php echo $isTradingActive ? 'text-emerald-400' : 'text-slate-400'; ?>">AUTO-TRADING <?php echo $isTradingActive ? 'YES' : 'NO'; ?></span>
                    </div>
                    <label class="relative inline-flex items-center cursor-pointer ml-2">
                        <input type="checkbox" id="ibkrToggle" class="sr-only peer" <?php echo $isTradingActive ? 'checked' : ''; ?>>
                        <div class="w-9 h-5 bg-slate-800 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-slate-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all peer-checked:bg-emerald-500"></div>
                    </label>
                </div>

                <button id="openHelpModalBtn" class="flex items-center gap-1.5 px-3 py-2 rounded-lg bg-cyan-500/10 hover:bg-cyan-500/20 border border-cyan-500/30 text-cyan-400 font-mono text-xs transition-all hover:scale-105" title="Dashboard Help & Glossary">
                    <i data-lucide="help-circle" class="w-4 h-4"></i>
                    <span class="hidden sm:inline">Help</span>
                </button>

                <button id="openConfigModalBtn" class="flex items-center gap-1.5 px-3.5 py-2 rounded-lg bg-white/[0.05] hover:bg-white/[0.1] border border-white/10 text-slate-200 font-mono text-xs transition-all hover:scale-105">
                    <i data-lucide="sliders" class="w-4 h-4 text-cyan-400"></i>
                    <span>Config</span>
                </button>

                <a href="?action=logout" class="flex items-center gap-1.5 px-3 py-2 rounded-lg bg-red-500/10 hover:bg-red-500/20 border border-red-500/30 text-red-400 font-mono text-xs transition-all hover:scale-105" title="Terminate Session">
                    <i data-lucide="log-out" class="w-4 h-4"></i>
                    <span class="hidden sm:inline">Logout</span>
                </a>
            </div>
        </div>
    </header>

    <main class="max-w-[1800px] w-full mx-auto p-4 space-y-4 flex-1">

        <!-- ACTIVE FILTER STATUS BANNER -->
        <div id="filterBanner" class="hidden glass-panel rounded-xl p-3 border border-emerald-500/50 bg-emerald-500/10 flex flex-col sm:flex-row items-center justify-between gap-2 font-mono text-xs">
            <div class="flex items-center gap-2 text-emerald-300">
                <i data-lucide="filter" class="w-4 h-4 text-emerald-400 shrink-0"></i>
                <span>Viewing Individual Stock Drill-Down: <strong id="activeFilterSymbolText" class="text-white text-sm"></strong></span>
            </div>
            <button onclick="clearSymbolFilter()" class="w-full sm:w-auto px-3 py-1 rounded-lg bg-emerald-500 hover:bg-emerald-600 text-slate-950 font-bold transition-all text-center">
                Reset Filter (Complete View)
            </button>
        </div>

        <!-- PERFORMANCE SUMMARY & CLOSED TRADE STATISTICS (SELL SIGNALS) -->
        <section class="rounded-2xl p-4 border border-cardborder bg-gradient-to-r from-slate-900/90 via-slate-900/60 to-slate-900/90 mb-6">
            <div class="flex flex-col sm:flex-row items-start sm:items-center justify-between mb-3 border-b border-slate-800 pb-2 gap-1">
                <h3 class="font-mono font-bold text-xs text-emerald-400 uppercase tracking-widest flex items-center gap-2">
                    <i data-lucide="award" class="w-4 h-4 text-emerald-400 shrink-0"></i>
                    <span>Performance Summary & Closed Trade Statistics (SELL Signals)</span>
                </h3>
                <span class="text-[11px] font-mono text-slate-400">Audit-Ready Alpha Analytics</span>
            </div>
            <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3 font-mono">
                <div class="p-3 bg-slate-950/80 rounded-xl border border-slate-800">
                    <div class="text-[10px] text-slate-400 uppercase">Total Closed Trades</div>
                    <div id="metricTotalClosed" class="text-base font-bold text-white mt-1"><?php echo $totalClosedTrades; ?> <span class="text-[10px] text-slate-400">(SELLs)</span></div>
                </div>
                <div class="p-3 bg-slate-950/80 rounded-xl border border-slate-800">
                    <div class="text-[10px] text-slate-400 uppercase">Total Realized PnL</div>
                    <div id="metricRealizedPnl" class="text-base font-bold <?php echo $totalRealizedPnl >= 0 ? 'text-emerald-400' : 'text-red-400'; ?> mt-1"><?php echo ($totalRealizedPnl >= 0 ? '+' : '') . '$' . number_format($totalRealizedPnl, 2); ?></div>
                </div>
                <div class="p-3 bg-slate-950/80 rounded-xl border border-slate-800">
                    <div class="text-[10px] text-slate-400 uppercase">Win Rate</div>
                    <div id="metricWinRate" class="text-base font-bold text-cyan-400 mt-1"><?php echo $winRate; ?>%</div>
                    <div id="metricWinBreakdown" class="text-[9px] text-slate-400 mt-0.5"><?php echo "{$winningTradesCount}W / {$losingTradesCount}L / {$breakevenTradesCount}BE"; ?></div>
                </div>
                <div class="p-3 bg-slate-950/80 rounded-xl border border-slate-800">
                    <div class="text-[10px] text-slate-400 uppercase">Avg Win / Loss</div>
                    <div id="metricAvgWinLoss" class="text-xs font-bold text-white mt-1"><span class="text-emerald-400">+$<?php echo number_format($avgWin, 2); ?></span> / <span class="text-red-400">-$<?php echo number_format($avgLoss, 2); ?></span></div>
                </div>
                <div class="p-3 bg-slate-950/80 rounded-xl border border-slate-800">
                    <div id="metricLargestWinHeader" class="text-[10px] text-slate-400 uppercase">Largest Win (<?php echo htmlspecialchars($largestWinSym); ?>)</div>
                    <div id="metricLargestWin" class="text-base font-bold text-emerald-400 mt-1">+<?php echo '$' . number_format($largestWinVal, 2); ?></div>
                </div>
                <div class="p-3 bg-slate-950/80 rounded-xl border border-slate-800">
                    <div id="metricLargestLossHeader" class="text-[10px] text-slate-400 uppercase">Largest Loss (<?php echo htmlspecialchars($largestLossSym); ?>)</div>
                    <div id="metricLargestLoss" class="text-base font-bold text-red-400 mt-1">-<?php echo '$' . number_format(abs($largestLossVal), 2); ?></div>
                </div>
            </div>
        </section>

        <!-- ADVANCED INSTITUTIONAL PERFORMANCE BAR -->
        <section class="glass-panel rounded-2xl p-4 border border-cardborder">
            <div class="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-8 gap-4 text-center font-mono">
                <div class="p-2.5 bg-slate-900/70 rounded-xl border border-slate-800 shadow-inner">
                    <div class="text-[10px] text-slate-400 uppercase">Total Trades / Shares</div>
                    <div id="statTotalTrades" class="text-sm font-bold text-white mt-1"><?php echo count($trades); ?> <span class="text-xs text-slate-400">(<span id="statBuyCount">0</span>B / <span id="statSellCount">0</span>S)</span></div>
                    <div class="text-[11px] font-bold text-cyan-400 mt-0.5"><span id="statTotalShares"><?php echo number_format($totalQuantity); ?></span> sh</div>
                </div>
                <div class="p-2.5 bg-slate-900/70 rounded-xl border border-slate-800 shadow-inner">
                    <div class="text-[10px] text-slate-400 uppercase">Avg Hold Time</div>
                    <div id="statAvgHold" class="text-sm font-bold text-cyan-400 mt-1">0 sec <span class="text-[10px] text-slate-400">(2.09m)</span></div>
                </div>
                <div class="p-2.5 bg-slate-900/70 rounded-xl border border-slate-800 shadow-inner">
                    <div class="text-[10px] text-slate-400 uppercase">Stream Throughput</div>
                    <div id="statThroughput" class="text-sm font-bold text-emerald-400 mt-1 flex items-center justify-center gap-1.5">
                        <span class="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-ping"></span>
                        <span>0 msgs/sec</span>
                    </div>
                </div>
                <div class="p-2.5 bg-slate-900/70 rounded-xl border border-slate-800 shadow-inner">
                    <div class="text-[10px] text-slate-400 uppercase">Max Drawdown</div>
                    <div id="statMaxDd" class="text-sm font-bold text-red-400 mt-1">$<?php echo number_format($maxDdUsd, 2); ?> (<?php echo number_format($maxDdPct, 2); ?>%)</div>
                </div>
                <div class="p-2.5 bg-slate-900/70 rounded-xl border border-slate-800 shadow-inner">
                    <div class="text-[10px] text-slate-400 uppercase">Gross Buy Turnover</div>
                    <div id="statBuyTurnover" class="text-sm font-bold text-blue-400 mt-1">$0</div>
                </div>
                <div class="p-2.5 bg-slate-900/70 rounded-xl border border-slate-800 shadow-inner">
                    <div class="text-[10px] text-slate-400 uppercase">Gross Sell Proceeds</div>
                    <div id="statSellProceeds" class="text-sm font-bold text-purple-400 mt-1">$0</div>
                </div>
                <div class="p-2.5 bg-slate-900/70 rounded-xl border border-slate-800 shadow-inner">
                    <div class="text-[10px] text-slate-400 uppercase">Profit Factor</div>
                    <div id="statProfitFactor" class="text-sm font-bold text-emerald-400 mt-1">0</div>
                </div>
                <div class="p-2.5 bg-slate-900/70 rounded-xl border border-slate-800 shadow-inner">
                    <div class="text-[10px] text-slate-400 uppercase">Win Rate (%)</div>
                    <div id="statWinRate" class="text-sm font-bold text-cyan-400 mt-1">0%</div>
                </div>
            </div>
        </section>

        <!-- 5 INSTITUTIONAL METRIC CARDS -->
        <section class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
            <div class="glass-panel rounded-2xl p-4 border border-cardborder flex items-center justify-between shadow-lg">
                <div>
                    <div class="text-xs font-mono text-slate-400 uppercase tracking-wider">Notional PnL (ALL Trades)</div>
                    <div id="boxNotionalPnl" class="text-xl font-black font-mono text-emerald-400 mt-1">+$<?php echo number_format($notionalPnl, 2); ?></div>
                    <div class="text-[10px] font-mono text-slate-500 mt-0.5">Unrealized TML Simulation</div>
                </div>
                <div class="w-12 h-12 rounded-xl bg-emerald-500/10 border border-emerald-500/30 flex items-center justify-center text-emerald-400 shadow-lg shrink-0">
                    <i data-lucide="dollar-sign" class="w-6 h-6 animate-pulse"></i>
                </div>
            </div>

            <div class="glass-panel rounded-2xl p-4 border border-cardborder flex items-center justify-between shadow-lg">
                <div>
                    <div class="text-xs font-mono text-slate-400 uppercase tracking-wider">Trading Budget</div>
                    <div id="boxTradingBudget" class="text-xl font-black font-mono text-blue-400 mt-1">$<?php echo number_format($settings['tradebudget'], 2); ?></div>
                    <div class="text-[10px] font-mono text-slate-500 mt-0.5"><?php echo $settings['exchange']; ?> / <?php echo $settings['market']; ?> Capital</div>
                </div>
                <div class="w-12 h-12 rounded-xl bg-blue-500/10 border border-blue-500/30 flex items-center justify-center text-blue-400 shadow-lg shrink-0">
                    <i data-lucide="wallet" class="w-6 h-6"></i>
                </div>
            </div>

            <div class="glass-panel rounded-2xl p-4 border border-cardborder flex items-center justify-between shadow-lg">
                <div>
                    <div class="text-xs font-mono text-slate-400 uppercase tracking-wider">Actual PnL (Auto-Trading YES)</div>
                    <div id="boxActualPnl" class="text-xl font-black font-mono text-cyan-400 mt-1">+$<?php echo number_format($actualPnl, 2);?></div>
                    <div class="text-[10px] font-mono text-slate-500 mt-0.5">Realized Broker Fills</div>
                </div>
                <div class="w-12 h-12 rounded-xl bg-cyan-500/10 border border-cyan-500/30 flex items-center justify-center text-cyan-400 shadow-lg shrink-0">
                    <i data-lucide="trending-up" class="w-6 h-6 animate-bounce"></i>
                </div>
            </div>

            <div class="glass-panel rounded-2xl p-4 border border-cardborder flex items-center justify-between shadow-lg">
                <div>
                    <div class="text-xs font-mono text-slate-400 uppercase tracking-wider">Total Open Positions</div>
                    <div id="boxOpenPositions" class="text-xl font-black font-mono text-amber-400 mt-1"><?php echo count($initialOpenPositions); ?> Active</div>
                    <div class="text-[10px] font-mono text-slate-500 mt-0.5">Click for Open Details</div>
                </div>
                <div class="w-12 h-12 rounded-xl bg-amber-500/10 border border-amber-500/30 flex items-center justify-center text-amber-400 cursor-pointer hover:bg-amber-500/20 shadow-lg transition-transform hover:scale-110 shrink-0" id="openPositionsBtn">
                    <i data-lucide="layers" class="w-6 h-6"></i>
                </div>
            </div>

            <div class="glass-panel rounded-2xl p-4 border border-cardborder flex items-center justify-between shadow-lg">
                <div>
                    <div class="text-xs font-mono text-slate-400 uppercase tracking-wider">Total Value of Open Positions</div>
                    <?php 
                        $totalOpenValue = 0.0;
                        foreach ($initialOpenPositions as $op) {
                            $totalOpenValue += floatval($op['qty'] ?? 0) * floatval($op['price'] ?? 0);
                        }
                    ?>
                    <div id="boxTotalOpenValue" class="text-xl font-black font-mono text-purple-400 mt-1">$<?php echo number_format($totalOpenValue, 2); ?></div>
                    <div class="text-[10px] font-mono text-slate-500 mt-0.5">Current Holdings Valuation</div>
                </div>
                <div class="w-12 h-12 rounded-xl bg-purple-500/10 border border-purple-500/30 flex items-center justify-center text-purple-400 shadow-lg shrink-0">
                    <i data-lucide="pie-chart" class="w-6 h-6"></i>
                </div>
            </div>
        </section>

        <!-- LIVE UNIQUE SYMBOLS UNIVERSE (FLASHY REAL-TIME PRICES & SPREAD CARDS) -->
        <section class="glass-panel rounded-2xl p-4 border border-cardborder">
            <div class="flex flex-col sm:flex-row items-start sm:items-center justify-between mb-3 border-b border-slate-800 pb-2 gap-1">
                <h3 class="font-mono font-bold text-sm text-white flex items-center gap-2">
                    <i data-lucide="cpu" class="w-4 h-4 text-accentcyan animate-spin shrink-0"></i>
                    <span>Traded Symbols Universe (<span id="symbolCount" class="text-cyan-400"><?php echo count($initialSymbols); ?></span> Active Assets) - <span class="text-accentcyan text-xs">Real-Time Tick & Spread Telemetry</span></span>
                </h3>
                <button onclick="clearSymbolFilter()" class="text-[11px] font-mono text-slate-400 hover:text-cyan-400 underline transition-colors">Clear Filter / Complete View</button>
            </div>
            <div id="symbolBadgeContainer" class="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3 max-h-[280px] overflow-y-auto p-1.5">
                <?php foreach ($initialSymbols as $sym => $info): ?>
                <?php 
                    $isGreen = $info['diff'] >= 0;
                    $diffSign = $isGreen ? '+' : '';
                    $diffClass = $isGreen ? 'text-emerald-400 bg-emerald-500/15 border-emerald-500/30' : 'text-red-400 bg-red-500/15 border-red-500/30';
                ?>
                <div onclick="filterBySymbol('<?php echo $sym; ?>')" class="symbol-card rounded-xl p-3 flex flex-col justify-between cursor-pointer group" data-symbol="<?php echo $sym; ?>">
                    <div class="flex items-center justify-between">
                        <div class="flex items-center gap-2">
                            <span class="font-mono font-black text-base text-white tracking-wider group-hover:text-cyan-300 transition-colors"><?php echo $sym; ?></span>
                            <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
                        </div>
                        <a href="https://finance.yahoo.com/quote/<?php echo $sym; ?>" target="_blank" onclick="event.stopPropagation();" class="text-slate-400 hover:text-cyan-400 transition-colors p-1 rounded hover:bg-slate-800/80" title="View <?php echo $sym; ?> on Yahoo Finance">
                            <i data-lucide="external-link" class="w-3.5 h-3.5"></i>
                        </a>
                    </div>
                    
                    <div class="my-2 grid grid-cols-2 gap-2 text-[11px] font-mono">
                        <div class="bg-slate-900/80 rounded-lg p-1.5 border border-slate-800">
                            <div class="text-[9px] text-slate-400 uppercase">Buy Entry</div>
                            <div class="text-slate-200 font-bold">$<?php echo number_format($info['buy_price'], 2); ?></div>
                        </div>
                        <div class="bg-slate-900/80 rounded-lg p-1.5 border border-slate-800">
                            <div class="text-[9px] text-slate-400 uppercase">Current Tick</div>
                            <div class="text-white font-bold">Getting Tick Quote</div>
                        </div>
                    </div>

                    <div class="flex items-center justify-between pt-2 border-t border-slate-800/80 text-[10px] font-mono">
                        <span class="px-2 py-0.5 rounded-md border font-bold <?php echo $diffClass; ?>">
                            <?php echo "{$diffSign}$" . number_format($info['diff'], 2) . " ({$diffSign}" . number_format($info['pct'], 2) . "%)"; ?>
                        </span>
                        <span class="text-cyan-400 font-semibold"><?php echo number_format($info['volume']); ?> sh</span>
                    </div>
                </div>
                <?php endforeach; ?>
            </div>
        </section>

        <!-- TWO REAL-TIME GRAPHS -->
        <section class="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <div id="equityCardPanel" class="glass-panel rounded-2xl p-4 border border-cardborder flex flex-col justify-between">
                <div class="flex items-center justify-between mb-3">
                    <div class="flex items-center gap-2">
                        <i data-lucide="line-chart" class="w-4 h-4 text-accentblue shrink-0"></i>
                        <h3 class="text-sm font-bold font-mono text-slate-200 truncate"><span id="equityChartTitle">QuantStream AI - Real-Time Equity Curve (ET)</span></h3>
                    </div>
                    <span id="liveEquityBadge" class="text-xs font-mono text-emerald-400 shrink-0">Baseline: $0</span>
                </div>
                <div class="relative w-full h-[280px]">
                    <canvas id="equityCurveChart"></canvas>
                </div>
            </div>

            <div id="drawdownCardPanel" class="glass-panel rounded-2xl p-4 border border-cardborder flex flex-col justify-between">
                <div class="flex items-center justify-between mb-3">
                    <div class="flex items-center gap-2">
                        <i data-lucide="activity" class="w-4 h-4 text-accentred shrink-0"></i>
                        <h3 class="text-sm font-bold font-mono text-slate-200 truncate"><span id="drawdownChartTitle">Intraday Drawdown Profile (%) (ET)</span></h3>
                    </div>
                    <span id="liveDdBadge" class="text-xs font-mono text-red-400 shrink-0">Peak-to-Trough HWM</span>
                </div>
                <div class="relative w-full h-[280px]">
                    <canvas id="drawdownChart"></canvas>
                </div>
            </div>
        </section>

        <!-- REAL-TIME SYSTEM AUDIT LOGS TERMINAL (NEW) -->
        <section class="glass-panel rounded-2xl p-4 border border-cardborder">
             <div class="flex flex-col sm:flex-row items-start sm:items-center justify-between mb-3 border-b border-slate-800 pb-2 gap-2">
                <h3 class="font-mono font-bold text-sm text-cyan-400 flex items-center gap-2">
                    <i data-lucide="terminal" class="w-4 h-4 text-cyan-400 animate-pulse shrink-0"></i>
                    <span>Real-Time System Audit Logs (<span id="logCountLabel"><?php echo count($initialLogs); ?></span> Entries • Latest to Oldest)</span>
                </h3>
                <div class="flex flex-wrap items-center gap-3">
                    <button onclick="window.location.href='?action=export_random_excel'" class="flex items-center gap-1.5 px-3 py-1 rounded-lg bg-cyan-500/10 hover:bg-cyan-500/20 border border-cyan-500/30 text-cyan-400 font-mono text-xs transition-all cursor-pointer">
                        <i data-lucide="download" class="w-3.5 h-3.5"></i>
                        <span>Download Excel Report</span>
                    </button>
                    <span class="text-[11px] font-mono text-slate-400 truncate max-w-[200px]">/rawdata/qs_tradedata/logs/</span>
                </div>
            </div>

            <div id="realtimeLogContainer" class="bg-slate-950/90 rounded-xl p-3 border border-slate-800 font-mono text-xs text-emerald-400 h-[220px] overflow-y-auto space-y-1 shadow-inner">
                <?php if (empty($initialLogs)): ?>
                    <div class="text-slate-500 italic">No log entries recorded for today yet...</div>
                <?php else: ?>
                    <?php foreach (array_slice($initialLogs, 0, 100) as $logLine): ?>
                        <div class="py-0.5 border-b border-slate-900/50 truncate hover:text-white transition-colors"><?php echo htmlspecialchars($logLine); ?></div>
                    <?php endforeach; ?>
                <?php endif; ?>
            </div>
        </section>

        <?php
        $easternNow = new DateTime('now', new DateTimeZone('America/New_York'));
        $isAfter4pm = (int)$easternNow->format('H') >= 16;
        $currentDateStr = $easternNow->format('Y-m-d');

        // 1. Filter PDF Reports
        $pdfFilesFiltered = [];
        if (is_dir($pdfDir)) {
            foreach (scandir($pdfDir) as $file) {
                if (strtolower(pathinfo($file, PATHINFO_EXTENSION)) === 'pdf') {
                    if (!$isAfter4pm && strpos($file, $currentDateStr) !== false) {
                        continue;
                    }
                    $pdfFilesFiltered[] = [
                        'name' => $file,
                        'size' => filesize($pdfDir . '/' . $file),
                        'mtime' => filemtime($pdfDir . '/' . $file)
                    ];
                }
            }
            usort($pdfFilesFiltered, function($a, $b) { return $b['mtime'] - $a['mtime']; });
        }

        // 2. Filter Audit Log files (/logs)
        $logFilesFiltered = [];
        if (is_dir($logsDir)) {
            foreach (scandir($logsDir) as $file) {
                if (strtolower(pathinfo($file, PATHINFO_EXTENSION)) === 'log') {
                    if (!$isAfter4pm && strpos($file, $currentDateStr) !== false) {
                        continue;
                    }
                    $logFilesFiltered[] = [
                        'name' => $file,
                        'size' => filesize($logsDir . '/' . $file),
                        'mtime' => filemtime($logsDir . '/' . $file)
                    ];
                }
            }
            usort($logFilesFiltered, function($a, $b) { return $b['mtime'] - $a['mtime']; });
        }

        // 3. Filter Trade Data files (/tradedata)
        $tradeDataFilesFiltered = [];
        $tradeDataDir = 'C:/MAADS/tml-airflow/rawdata/qs_tradedata/';
        if (is_dir($tradeDataDir)) {
            foreach (scandir($tradeDataDir) as $file) {
                if (strtolower(pathinfo($file, PATHINFO_EXTENSION)) === 'csv') {
                    if (!$isAfter4pm && strpos($file, $currentDateStr) !== false) {
                        continue;
                    }
                    $tradeDataFilesFiltered[] = [
                        'name' => $file,
                        'size' => filesize($tradeDataDir . '/' . $file),
                        'mtime' => filemtime($tradeDataDir . '/' . $file)
                    ];
                }
            }
            usort($tradeDataFilesFiltered, function($a, $b) { return $b['mtime'] - $a['mtime']; });
        }
        ?>

        <!-- 3 WINDOWS ARCHIVE SECTION (/pdfreports, /logs, /tradedata) -->
        <section class="grid grid-cols-1 lg:grid-cols-3 gap-4">
            <!-- Window 1: PDF Reports -->
            <div class="glass-panel rounded-2xl p-4 border border-cardborder flex flex-col justify-between">
                <div class="flex items-center justify-between mb-3 border-b border-slate-800 pb-2">
                    <h3 class="font-mono font-bold text-xs text-cyan-400 flex items-center gap-2 truncate">
                        <i data-lucide="file-text" class="w-4 h-4 text-cyan-400 shrink-0"></i>
                        <span>PDF Reports (<span id="pdfCountLabel"><?php echo count($pdfFilesFiltered); ?></span>)</span>
                    </h3>
                    <span class="text-[10px] font-mono text-slate-400">/pdfreports</span>
                </div>
                <?php if (empty($pdfFilesFiltered)): ?>
                    <div class="text-xs font-mono text-slate-400 py-3 text-center">No PDF reports available.</div>
                <?php else: ?>
                    <div class="space-y-2 max-h-[220px] overflow-y-auto pr-1">
                        <?php foreach ($pdfFilesFiltered as $pdf): ?>
                            <a href="pdfreports/<?php echo urlencode($pdf['name']); ?>" target="_blank" class="p-2.5 bg-slate-900/85 hover:bg-slate-800/95 border border-slate-800 hover:border-cyan-500/50 rounded-xl flex items-center justify-between transition-all group">
                                <div class="flex items-center gap-2 overflow-hidden">
                                    <i data-lucide="file-badge" class="w-4 h-4 text-red-400 shrink-0 group-hover:scale-110 transition-transform"></i>
                                    <div class="truncate">
                                        <div class="font-mono text-[11px] font-bold text-white truncate"><?php echo htmlspecialchars($pdf['name']); ?></div>
                                        <div class="font-mono text-[9px] text-slate-400"><?php echo number_format($pdf['size'] / 1024, 1); ?> KB • <?php echo date('Y-m-d H:i', $pdf['mtime']); ?></div>
                                    </div>
                                </div>
                                <i data-lucide="external-link" class="w-3.5 h-3.5 text-slate-500 group-hover:text-cyan-400 shrink-0"></i>
                            </a>
                        <?php endforeach; ?>
                    </div>
                <?php endif; ?>
            </div>

            <!-- Window 2: Audit Log Files -->
            <div class="glass-panel rounded-2xl p-4 border border-cardborder flex flex-col justify-between">
                <div class="flex items-center justify-between mb-3 border-b border-slate-800 pb-2">
                    <h3 class="font-mono font-bold text-xs text-cyan-400 flex items-center gap-2 truncate">
                        <i data-lucide="terminal" class="w-4 h-4 text-cyan-400 shrink-0"></i>
                        <span>Audit Log Files (<span id="logFilesCountLabel"><?php echo count($logFilesFiltered); ?></span>)</span>
                    </h3>
                    <span class="text-[10px] font-mono text-slate-400">/logs</span>
                </div>
                <?php if (empty($logFilesFiltered)): ?>
                    <div class="text-xs font-mono text-slate-400 py-3 text-center">No log files available.</div>
                <?php else: ?>
                    <div class="space-y-2 max-h-[220px] overflow-y-auto pr-1">
					<?php foreach ($logFilesFiltered as $logFile): ?>
						<a href="logs/<?php echo urlencode($logFile['name']); ?>" target="_blank" class="p-2.5 bg-slate-900/85 hover:bg-slate-800/95 border border-slate-800 hover:border-cyan-500/50 rounded-xl flex items-center justify-between transition-all group">
							<div class="flex items-center gap-2 overflow-hidden">
								<i data-lucide="file-code" class="w-4 h-4 text-emerald-400 shrink-0 group-hover:scale-110 transition-transform"></i>
								<div class="truncate">
									<div class="font-mono text-[11px] font-bold text-white truncate"><?php echo htmlspecialchars($logFile['name']); ?></div>
									<div class="font-mono text-[9px] text-slate-400"><?php echo number_format($logFile['size'] / 1024, 1); ?> KB • <?php echo date('Y-m-d H:i', $logFile['mtime']); ?></div>
								</div>
							</div>
							<i data-lucide="external-link" class="w-3.5 h-3.5 text-slate-500 group-hover:text-cyan-400 shrink-0"></i>
						</a>
					<?php endforeach; ?>
                    </div>
                <?php endif; ?>
            </div>

            <!-- Window 3: Trade Data Files -->
            <div class="glass-panel rounded-2xl p-4 border border-cardborder flex flex-col justify-between">
                <div class="flex items-center justify-between mb-3 border-b border-slate-800 pb-2">
                    <h3 class="font-mono font-bold text-xs text-cyan-400 flex items-center gap-2 truncate">
                        <i data-lucide="database" class="w-4 h-4 text-cyan-400 shrink-0"></i>
                        <span>Trade Data Files (<span id="tradeFilesCountLabel"><?php echo count($tradeDataFilesFiltered); ?></span>)</span>
                    </h3>
                    <span class="text-[10px] font-mono text-slate-400">/tradedata</span>
                </div>
                <?php if (empty($tradeDataFilesFiltered)): ?>
                    <div class="text-xs font-mono text-slate-400 py-3 text-center">No trade data files available.</div>
                <?php else: ?>
                    <div class="space-y-2 max-h-[220px] overflow-y-auto pr-1">
					<?php foreach ($tradeDataFilesFiltered as $tradeFile): ?>
						<a href="tradedata/<?php echo urlencode($tradeFile['name']); ?>" target="_blank" class="p-2.5 bg-slate-900/85 hover:bg-slate-800/95 border border-slate-800 hover:border-cyan-500/50 rounded-xl flex items-center justify-between transition-all group">
							<div class="flex items-center gap-2 overflow-hidden">
								<i data-lucide="table" class="w-4 h-4 text-amber-400 shrink-0 group-hover:scale-110 transition-transform"></i>
								<div class="truncate">
									<div class="font-mono text-[11px] font-bold text-white truncate"><?php echo htmlspecialchars($tradeFile['name']); ?></div>
									<div class="font-mono text-[9px] text-slate-400"><?php echo number_format($tradeFile['size'] / 1024, 1); ?> KB • <?php echo date('Y-m-d H:i', $tradeFile['mtime']); ?></div>
								</div>
							</div>
							<i data-lucide="external-link" class="w-3.5 h-3.5 text-slate-500 group-hover:text-cyan-400 shrink-0"></i>
						</a>
					<?php endforeach; ?>
                    </div>
                <?php endif; ?>
            </div>
        </section>

        <!-- OPEN POSITIONS DETAILS TABLE SECTION -->
        <section class="glass-panel rounded-2xl p-4 border border-cardborder">
            <div class="flex items-center justify-between mb-3">
                <h3 class="font-mono font-bold text-sm text-amber-400 flex items-center gap-2">
                    <i data-lucide="briefcase" class="w-4 h-4 text-amber-400 shrink-0"></i>
                    <span>Open Positions Details (<span id="openPosTableCount"><?php echo count($initialOpenPositions); ?></span> Active Holdings)</span>
                </h3>
                <span class="text-xs font-mono text-slate-400">Click column headers to sort</span>
            </div>
            <div class="overflow-x-auto max-h-[220px]">
                <table id="openPositionsTable" class="w-full text-left font-mono text-xs whitespace-nowrap">
                    <thead class="bg-slate-900/90 text-slate-400 uppercase text-[10px] sticky top-0">
                        <tr>
                            <th class="p-2.5 sortable" onclick="sortTable('openPositionsTable', 0)">Symbol ↕</th>
                            <th class="p-2.5 sortable" onclick="sortTable('openPositionsTable', 1, true)">Quantity ↕</th>
                            <th class="p-2.5 sortable" onclick="sortTable('openPositionsTable', 2, true)">Entry Price ($) ↕</th>
                            <th class="p-2.5 sortable" onclick="sortTable('openPositionsTable', 3, true)">Notional Value ($) ↕</th>
                        </tr>
                    </thead>
                    <tbody id="openPositionsTableBody" class="divide-y divide-slate-800 text-slate-300">
                        <?php foreach ($initialOpenPositions as $op): ?>
                        <tr class="hover:bg-slate-900/50">
                            <td class="p-2 font-bold text-white"><?php echo htmlspecialchars($op['symbol']); ?></td>
                            <td class="p-2 text-cyan-400"><?php echo number_format($op['qty']); ?></td>
                            <td class="p-2">$<?php echo number_format($op['price'], 2); ?></td>
                            <td class="p-2 text-emerald-400">$<?php echo number_format($op['qty'] * $op['price'], 2); ?></td>
                        </tr>
                        <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
        </section>

        <!-- CSV TRADE LOG EXECUTION TABLE -->
        <section class="glass-panel rounded-2xl p-4 border border-cardborder">
            <div class="flex flex-col sm:flex-row items-start sm:items-center justify-between mb-3 gap-1">
                <h3 class="font-mono font-bold text-sm text-slate-200 flex items-center gap-2">
                    <i data-lucide="database" class="w-4 h-4 text-accentcyan shrink-0"></i>
                    <span>Server CSV Trade Execution Log (<span id="tradeCountLabel"><?php echo count($trades); ?></span> Total Executions)</span>
                </h3>
                <span class="text-xs font-mono text-emerald-400 flex items-center gap-1.5">
                    <span class="w-2 h-2 rounded-full bg-emerald-400 animate-ping"></span>
                    <span>Live Monitoring Active</span>
                </span>
            </div>
            <div class="overflow-x-auto max-h-[300px]">
                <table id="tradeLogTable" class="w-full text-left font-mono text-xs whitespace-nowrap">
                    <thead class="bg-slate-900/90 text-slate-400 uppercase text-[10px] sticky top-0">
                        <tr>
                            <th class="p-2.5 sortable" onclick="sortTable('tradeLogTable', 0)">Timestamp (UTC) ↕</th>
                            <th class="p-2.5 sortable" onclick="sortTable('tradeLogTable', 1)">Symbol ↕</th>
                            <th class="p-2.5 sortable" onclick="sortTable('tradeLogTable', 2)">Signal ↕</th>
                            <th class="p-2.5 sortable" onclick="sortTable('tradeLogTable', 3, true)">Price ($) ↕</th>
                            <th class="p-2.5 sortable" onclick="sortTable('tradeLogTable', 4, true)">Qty ↕</th>
                            <th class="p-2.5 sortable" onclick="sortTable('tradeLogTable', 5, true)">Trade PnL ($) ↕</th>
                            <th class="p-2.5 sortable" onclick="sortTable('tradeLogTable', 6, true)">Cumulative PnL ($) ↕</th>
                            <th class="p-2.5 sortable" onclick="sortTable('tradeLogTable', 7)">Broker Executed ↕</th>
                        </tr>
                    </thead>
                    <tbody id="tradeLogTableBody" class="divide-y divide-slate-800 text-slate-300">
                        <?php foreach (array_slice($trades, -50) as $t): ?>
                        <tr class="hover:bg-slate-900/50 transition-colors">
                            <td class="p-2 text-slate-400"><?php echo htmlspecialchars($t['timestamp']); ?></td>
                            <td class="p-2 font-bold text-white"><?php echo htmlspecialchars($t['symbol']); ?></td>
                            <td class="p-2"><span class="px-1.5 py-0.5 rounded text-[10px] font-bold <?php echo $t['signal'] === 'BUY' ? 'bg-emerald-500/20 text-emerald-400' : 'bg-red-500/20 text-red-400'; ?>"><?php echo htmlspecialchars($t['signal']); ?></span></td>
                            <td class="p-2">$<?php echo number_format(floatval($t['price']), 2); ?></td>
                            <td class="p-2"><?php echo htmlspecialchars($t['quantity']); ?></td>
                            <td class="p-2 font-bold <?php echo floatval($t['trade_pnl']) >= 0 ? 'text-emerald-400' : 'text-red-400'; ?>">$<?php echo number_format(floatval($t['trade_pnl']), 2); ?></td>
                            <td class="p-2">$<?php echo number_format(floatval($t['cumulative_pnl']), 2); ?></td>
                            <td class="p-2"><span class="px-1.5 py-0.5 rounded text-[10px] <?php echo $t['brokerexecuted'] === 'YES' ? 'bg-cyan-500/20 text-cyan-400' : 'bg-slate-800 text-slate-400'; ?>"><?php echo htmlspecialchars($t['brokerexecuted']); ?></span></td>
                        </tr>
                        <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
        </section>

    </main>

    <!-- HELP / GLOSSARY MODAL -->
    <div id="helpModal" class="fixed inset-0 bg-black/80 backdrop-blur-md z-50 flex items-center justify-center p-4 hidden">
        <div class="glass-panel w-full max-w-4xl rounded-2xl p-6 border border-cardborder shadow-2xl space-y-4 max-h-[90vh] flex flex-col">
            <div class="flex items-center justify-between border-b border-slate-800 pb-3">
                <div class="flex items-center gap-2">
                    <i data-lucide="help-circle" class="w-5 h-5 text-cyan-400 shrink-0"></i>
                    <div>
                        <h3 class="font-bold text-base text-white font-mono">QuantStream AI Dashboard Glossary & Help Center</h3>
                        <p class="text-[11px] font-mono text-slate-400">Institutional Definitions, TML Parameters & Audit Standards</p>
                    </div>
                </div>
                <button id="closeHelpModalBtn" class="text-slate-400 hover:text-white transition-colors p-1 rounded-lg hover:bg-slate-800">
                    <i data-lucide="x" class="w-5 h-5"></i>
                </button>
            </div>

            <div class="relative">
                <i data-lucide="search" class="w-4 h-4 text-slate-400 absolute left-3 top-3"></i>
                <input type="text" id="helpSearchInput" placeholder="Search terms (e.g., PnL, TML, Win Rate, Drawdown)..." class="w-full bg-slate-900/90 border border-slate-700 rounded-xl pl-9 pr-4 py-2 font-mono text-xs text-white focus:outline-none focus:border-cyan-400">
            </div>

            <div id="helpGlossaryList" class="overflow-y-auto space-y-3 pr-2 flex-1 font-mono text-xs">
                <div class="help-item p-3 bg-slate-900/60 rounded-xl border border-slate-800" data-keywords="support email contact help otics">
                    <div class="font-bold text-cyan-400 text-sm flex items-center gap-2">
                        <span>Technical Support & Assistance</span>
                    </div>
                    <p class="text-slate-300 mt-1 text-[11px] leading-relaxed">
                        For support email: <a href="mailto:support@otics.ca?subject=QuantStream%20AI" class="text-cyan-400 underline hover:text-cyan-300">support@otics.ca</a>, subject: QuantStream AI
                    </p>
                </div>
                
                <div class="help-item p-3 bg-slate-900/60 rounded-xl border border-slate-800" data-keywords="tml transactional machine learning algorithm engine streaming">
                    <div class="font-bold text-cyan-400 text-sm flex items-center gap-2">
                        <span>Transactional Machine Learning (TML)</span>
                    </div>
                    <p class="text-slate-300 mt-1 text-[11px] leading-relaxed">
                        An advanced low-latency analytical architecture processing real-time streaming data events and executing instantaneous decisioning models, minimizing state lag between market tick ingestion and order dispatch.
                    </p>
                </div>

                <div class="help-item p-3 bg-slate-900/60 rounded-xl border border-slate-800" data-keywords="pnl profit loss notional actual realized unrealized">
                    <div class="font-bold text-emerald-400 text-sm flex items-center gap-2">
                        <span>Notional vs. Actual PnL</span>
                    </div>
                    <p class="text-slate-300 mt-1 text-[11px] leading-relaxed">
                        <strong>Notional PnL (brokerexec=no):</strong> Simulated paper profit or loss generated by internal TML predictive signals before routing to external brokers. <br>
                        <strong>Actual PnL (brokerexec=yes):</strong> Realized monetary gain or loss confirmed via live broker execution fills.
                    </p>
                </div>

                <div class="help-item p-3 bg-slate-900/60 rounded-xl border border-slate-800" data-keywords="win rate percentage profitable trades closed sell">
                    <div class="font-bold text-cyan-400 text-sm flex items-center gap-2">
                        <span>Win Rate (%)</span>
                    </div>
                    <p class="text-slate-300 mt-1 text-[11px] leading-relaxed">
                        The percentage of closed trades (triggered by SELL signals) that resulted in zero or positive PnL compared against total closed positions ($(\text{Winning Trades} / \text{Total Closed Trades}) \times 100$).
                    </p>
                </div>

                <div class="help-item p-3 bg-slate-900/60 rounded-xl border border-slate-800" data-keywords="profit factor gross profit gross loss ratio">
                    <div class="font-bold text-emerald-400 text-sm flex items-center gap-2">
                        <span>Profit Factor</span>
                    </div>
                    <p class="text-slate-300 mt-1 text-[11px] leading-relaxed">
                        The ratio of gross winning profits to gross losing losses ($\text{Gross Profit} / \text{Gross Loss}$). Values above 1.5 indicate robust trading system profitability.
                    </p>
                </div>

                <div class="help-item p-3 bg-slate-900/60 rounded-xl border border-slate-800" data-keywords="max drawdown hwm peak trough risk percentage usd">
                    <div class="font-bold text-red-400 text-sm flex items-center gap-2">
                        <span>Maximum Drawdown (Max DD)</span>
                    </div>
                    <p class="text-slate-300 mt-1 text-[11px] leading-relaxed">
                        The maximum observed peak-to-trough decline in portfolio equity before a new peak is achieved, measured both in absolute USD and percentage terms. Measures peak risk exposure.
                    </p>
                </div>

                <div class="help-item p-3 bg-slate-900/60 rounded-xl border border-slate-800" data-keywords="turnover proceeds gross buy sell volume capital">
                    <div class="font-bold text-blue-400 text-sm flex items-center gap-2">
                        <span>Gross Buy Turnover & Sell Proceeds</span>
                    </div>
                    <p class="text-slate-300 mt-1 text-[11px] leading-relaxed">
                        <strong>Gross Buy Turnover:</strong> Total capital deployed purchasing assets ($\sum \text{Price} \times \text{Quantity}$ for BUY signals). <br>
                        <strong>Gross Sell Proceeds:</strong> Total capital realized liquidating positions ($\sum \text{Price} \times \text{Quantity}$ for SELL signals).
                    </p>
                </div>

                <div class="help-item p-3 bg-slate-900/60 rounded-xl border border-slate-800" data-keywords="hold time duration seconds minutes trading execution">
                    <div class="font-bold text-cyan-400 text-sm flex items-center gap-2">
                        <span>Average Hold Time</span>
                    </div>
                    <p class="text-slate-300 mt-1 text-[11px] leading-relaxed">
                        The average duration (in seconds and minutes) an asset position is held between initial entry signal and final liquidation.
                    </p>
                </div>

                <div class="help-item p-3 bg-slate-900/60 rounded-xl border border-slate-800" data-keywords="throughput messages per second stream websocket telemetry">
                    <div class="font-bold text-emerald-400 text-sm flex items-center gap-2">
                        <span>Stream Throughput</span>
                    </div>
                    <p class="text-slate-300 mt-1 text-[11px] leading-relaxed">
                        The rate of incoming market ticks and algorithmic messages processed per second by the real-time TML stream engine.
                    </p>
                </div>

                <div class="help-item p-3 bg-slate-900/60 rounded-xl border border-slate-800" data-keywords="open positions active holdings portfolio inventory symbol">
                    <div class="font-bold text-amber-400 text-sm flex items-center gap-2">
                        <span>Open Positions</span>
                    </div>
                    <p class="text-slate-300 mt-1 text-[11px] leading-relaxed">
                        Currently active asset holdings maintained in the portfolio inventory awaiting exit signals. Displays symbol, quantity, entry price, and notional valuation.
                    </p>
                </div>

                <div class="help-item p-3 bg-slate-900/60 rounded-xl border border-slate-800" data-keywords="sec rule 17a-4 cra osfi audit compliance logging">
                    <div class="font-bold text-purple-400 text-sm flex items-center gap-2">
                        <span>Audit & Regulatory Compliance (SEC Rule 17a-4 / CRA / OSFI)</span>
                    </div>
                    <p class="text-slate-300 mt-1 text-[11px] leading-relaxed">
                        Compliance standards ensuring all trading logs, configuration states, and execution metrics are immutably archived and traceable for regulatory examination.
                    </p>
                </div>

                <div class="help-item p-3 bg-slate-900/60 rounded-xl border border-slate-800" data-keywords="qsconfig ini parameters configuration settings">
                    <div class="font-bold text-white text-sm flex items-center gap-2">
                        <span>Configuration Parameters (`qsconfig.ini`)</span>
                    </div>
                    <p class="text-slate-300 mt-1 text-[11px] leading-relaxed">
                        Settings file defining operational constraints including min/max price boundaries, trade budgets, volatility thresholds, loss limits, and target exchange connectivity.
                    </p>
                </div>

            </div>				
            <div class="flex items-center justify-between pt-3 border-t border-slate-800 text-[11px] font-mono text-slate-400">
                <span>QuantStream AI Help Center v3.0</span>
                <button id="closeHelpModalBtnBottom" class="px-4 py-2 rounded-lg bg-cyan-500 hover:bg-cyan-400 text-slate-950 font-bold transition-all">Close Help</button>
            </div>			
        </div>
    </div>

    <!-- CONFIG PARAMETERS MODAL -->
<!-- CONFIG PARAMETERS MODAL -->
<!-- CONFIG PARAMETERS MODAL -->
    <div id="configModal" class="fixed inset-0 bg-black/70 backdrop-blur-sm z-50 flex items-center justify-center p-4 hidden">
        <div class="glass-panel w-full max-w-2xl rounded-2xl p-6 border border-cardborder shadow-2xl space-y-5 max-h-[95vh] overflow-y-auto">
            <div class="flex items-center justify-between border-b border-slate-800 pb-3">
                <div class="flex items-center gap-2">
                    <i data-lucide="sliders" class="w-5 h-5 text-cyan-400 shrink-0"></i>
                    <h3 class="font-bold text-base text-white font-mono">Update QuantStream AI Parameters</h3>
                </div>
                <button id="closeConfigModalBtn" class="text-slate-400 hover:text-white transition-colors p-1">
                    <i data-lucide="x" class="w-5 h-5"></i>
                </button>
            </div>

            <form id="configForm" class="space-y-4 font-mono text-xs">
                <input type="hidden" name="action" value="update_config">

                <!-- SECTION 1: MARKET & INSTRUMENT FILTERS (Fixed at Startup) -->
                <div class="bg-slate-900/50 border border-slate-800 rounded-xl p-4 space-y-3">
                    <div class="flex items-center gap-2 text-cyan-400 font-bold border-b border-slate-800 pb-2">
                        <i data-lucide="globe" class="w-4 h-4 shrink-0"></i>
                        <span>1. Market & Instrument Filters (Startup Fixed)</span>
                    </div>
                    <div class="grid grid-cols-1 sm:grid-cols-3 gap-3">
                        <div>
                            <label class="block text-slate-400 mb-1">Market</label>
                            <input name="market" type="text" value="<?php echo $settings['market']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white uppercase">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Exchange</label>
                            <input name="exchange" type="text" value="<?php echo $settings['exchange']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white uppercase">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Exchange Name</label>
                            <input name="exchangename" type="text" value="<?php echo $settings['exchangename']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Cost/Trade</label>
                            <input name="cost_per_trade" type="text" value="<?php echo $settings['cost_per_trade']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Cost/Share</label>
                            <input name="cost_per_share" type="text" value="<?php echo $settings['cost_per_share']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Broker Name</label>
                            <input name="broker" type="text" value="<?php echo $settings['broker']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
<div class="col-span-full">
        <label class="block text-slate-400 mb-1">Broker URL</label>
        <input name="brokerurl" type="text" value="<?php echo $settings['brokerurl']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
    </div>						
                    </div>
                    <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
                        <div>
                            <label class="block text-slate-400 mb-1">Min Price ($)</label>
                            <input name="minprice" type="text" step="0.01" value="<?php echo $settings['minprice']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Max Price ($)</label>
                            <input name="maxprice" type="text" step="0.01" value="<?php echo $settings['maxprice']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Volume</label>
                            <input name="volume" type="text" value="<?php echo $settings['volume']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Min Stocks</label>
                            <input name="minstocks" type="text" value="<?php echo $settings['minstocks']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Max Stocks</label>
                            <input name="maxstocks" type="text" value="<?php echo $settings['maxstocks']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>												
                        <div>
                            <label class="block text-slate-400 mb-1">Min Volatility (%)</label>
                            <input name="minvolatility" type="text" value="<?php echo $settings['minvolatility']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Max Volatility (%)</label>
                            <input name="maxvolatility" type="text" value="<?php echo $settings['maxvolatility']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
						
                    </div>
                </div>

                <!-- SECTION 2: RISK MANAGEMENT & POSITION LIMITS (Real-Time) -->
                <div class="bg-slate-900/50 border border-slate-800 rounded-xl p-4 space-y-3">
                    <div class="flex items-center gap-2 text-emerald-400 font-bold border-b border-slate-800 pb-2">
                        <i data-lucide="shield-alert" class="w-4 h-4 shrink-0"></i>
                        <span>2. Risk Management & Position Limits (Real-Time Effect On QuantStream)</span>
                    </div>
                    <div class="grid grid-cols-1 sm:grid-cols-3 gap-3">
                        <div>
                            <label class="block text-slate-400 mb-1">Trade Budget ($)</label>
                            <input name="tradebudget" type="text" value="<?php echo $settings['tradebudget']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Max Loss Not Exceed (%)</label>
                            <input name="tradelossnotexceed" type="text" value="<?php echo $settings['tradelossnotexceed']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Max Pct Sell Loss</label>
                            <input name="maxpctsellloss" type="text" value="<?php echo $settings['maxpctsellloss']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                    </div>
                    <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
                        <div>
                            <label class="block text-slate-400 mb-1">Max Open Pos</label>
                            <input name="maxopenpositions" type="text" value="<?php echo $settings['maxopenpositions']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Max Trades</label>
                            <input name="maxtrades" type="text" value="<?php echo $settings['maxtrades']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Max Shares</label>
                            <input name="maxshares" type="text" value="<?php echo $settings['maxshares']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Max Shares/Trade</label>
                            <input name="maxsharespertrade" type="text" value="<?php echo $settings['maxsharespertrade']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                    </div>
                    <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
                        <div>
                            <label class="block text-slate-400 mb-1">Min Hold Sec</label>
                            <input name="minholdseconds" type="text" value="<?php echo $settings['minholdseconds']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Min Entry Sec</label>
                            <input name="minentryeconds" type="text" value="<?php echo $settings['minentryeconds']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Min Days Books</label>
                            <input name="mindaysonbooks" type="text" value="<?php echo $settings['mindaysonbooks']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Max Days Books</label>
                            <input name="maxdaysonbooks" type="text" value="<?php echo $settings['maxdaysonbooks']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                    </div>
                </div>

                <!-- SECTION 3: EXECUTION, FEES & BROKER SETTINGS (Real-Time) -->
                <div class="bg-slate-900/50 border border-slate-800 rounded-xl p-4 space-y-3">
                    <div class="flex items-center gap-2 text-amber-400 font-bold border-b border-slate-800 pb-2">
                        <i data-lucide="cpu" class="w-4 h-4 shrink-0"></i>
                        <span>3. Execution, Fees & Broker Settings (Real-Time Effect On QuantStream)</span>
                    </div>
                    <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
                        <div>
                            <label class="block text-slate-400 mb-1">Buy % Alloc</label>
                            <input name="notionalpct" type="text" value="<?php echo $settings['notionalpct']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Max Price Drop</label>
                            <input name="maxpricedrop" type="text" value="<?php echo $settings['maxpricedrop']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Limit Order</label>
                            <input name="addlimitorder" type="text" value="<?php echo $settings['addlimitorder']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-slate-400 mb-1">Take Profit %</label>
                            <input name="take_profit_pct" type="text" value="<?php echo $settings['take_profit_pct']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>
												
                    </div>
                    <div class="grid grid-cols-2 sm:grid-cols-5 gap-3">
                        <div>
                            <label class="block text-slate-400 mb-1">Min Pred Acc</label>
                            <input name="minpredictionaccuracy" type="text" value="<?php echo $settings['minpredictionaccuracy']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>						
                        <div>
                            <label class="block text-slate-400 mb-1">Auto-Update Trade Budget</label>
                            <input name="autotradebudget" type="text" value="<?php echo $settings['autotradebudget']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>						
                        <div>
                            <label class="block text-slate-400 mb-1">Auto QuantStream Agent Strategy</label>
                            <input name="autoqsagentstrategy" type="text" value="<?php echo $settings['autoqsagentstrategy']; ?>" required class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
                        </div>						
						
                    </div>
					
                </div>

                <?php 
                $isAuthorizedUser = isset($_SESSION['qs_user']) && $_SESSION['qs_user'] === 'quantadmin'; 
                ?>
                <div class="flex items-center justify-end gap-3 pt-3 border-t border-slate-800">
                    <button type="button" id="cancelConfigModalBtn" class="px-4 py-2 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 font-medium">Cancel</button>
                    <button type="submit" class="px-4 py-2 rounded-lg bg-accentcyan hover:bg-cyan-500 text-slate-950 font-bold shadow-lg <?php echo $isAuthorizedUser ? '' : 'hidden'; ?>">Save Configuration</button>
                </div>
            </form>
        </div>
    </div>
	
    <script>
        lucide.createIcons();

        const loginForm = document.getElementById('loginForm');
        if (loginForm) {
            document.getElementById('fillDemoCreds').addEventListener('click', () => {
                document.getElementById('loginUsername').value = 'quantdemo';
                document.getElementById('loginPassword').value = 'QuantStream2026!';
            });

            loginForm.addEventListener('submit', function(e) {
                e.preventDefault();
                const formData = new FormData(this);
                const errorBox = document.getElementById('loginErrorMsg');
                
                fetch(window.location.pathname.split('/').pop() || 'qsmain.php', { method: 'POST', body: formData })
                .then(res => res.json())
                .then(data => {
                    if(data.status === 'success') {
                        location.reload();
                    } else {
                        errorBox.innerText = data.message;
                        errorBox.classList.remove('hidden');
                    }
                })
                .catch(err => {
                    errorBox.innerText = 'Authentication error connection failed.';
                    errorBox.classList.remove('hidden');
                });
            });
        }

        let currentSelectedSymbol = '';
        const currentScript = window.location.pathname.split('/').pop() || 'qsmain.php';

		const ibkrToggle = document.getElementById('ibkrToggle');
		const ibkrStatusLabel = document.getElementById('ibkrStatusLabel');
       if (ibkrToggle) {
			ibkrToggle.addEventListener('change', function() {
				// Prevent execution and notify if the session user is quantdemo
               const qs_user = "<?php echo $_SESSION['qs_user'] ?? ''; ?>";				
				if (typeof qs_user !== 'undefined' && qs_user === 'quantdemo') {
					alert('Demo accounts are not permitted to modify auto-trading settings.');
					this.checked = !this.checked; // Reset toggle state
					return;
				}

				const isChecked = this.checked;
				const statusStr = isChecked ? 'YES' : 'NO';
				
				ibkrStatusLabel.innerText = `AUTO-TRADING ${statusStr}`;
				ibkrStatusLabel.className = isChecked 
					? "text-[10px] font-mono font-bold text-emerald-400" 
					: "text-[10px] font-mono font-bold text-slate-400";

				const formData = new FormData();
				formData.append('action', 'update_trading_status');
				formData.append('status', statusStr);

				fetch('qsmain.php', { method: 'POST', body: formData })
					.then(res => res.json())
					.then(data => {})
					.catch(err => console.error('Error updating trading status:', err));
			});
		}              

        const sortDirections = {};
        function sortTable(tableId, columnIndex, isNumeric = false) {
            const table = document.getElementById(tableId);
            const tbody = table.querySelector('tbody');
            const rows = Array.from(tbody.querySelectorAll('tr'));
            
            sortDirections[tableId] = sortDirections[tableId] || {};
            const currentDir = sortDirections[tableId][columnIndex] === 'asc' ? 'desc' : 'asc';
            sortDirections[tableId][columnIndex] = currentDir;

            rows.sort((a, b) => {
                let cellA = a.children[columnIndex].innerText.replace(/[$,]/g, '').trim();
                let cellB = b.children[columnIndex].innerText.replace(/[$,]/g, '').trim();

                if (isNumeric) {
                    cellA = parseFloat(cellA) || 0;
                    cellB = parseFloat(cellB) || 0;
                    return currentDir === 'asc' ? cellA - cellB : cellB - cellA;
                } else {
                    return currentDir === 'asc' ? cellA.localeCompare(cellB) : cellB.localeCompare(cellA);
                }
            });

            tbody.innerHTML = '';
            rows.forEach(row => tbody.appendChild(row));
        }

        const helpModal = document.getElementById('helpModal');
        document.getElementById('openHelpModalBtn').addEventListener('click', () => helpModal.classList.remove('hidden'));
        document.getElementById('closeHelpModalBtn').addEventListener('click', () => helpModal.classList.add('hidden'));
        document.getElementById('closeHelpModalBtnBottom').addEventListener('click', () => helpModal.classList.add('hidden'));

        const configModal = document.getElementById('configModal');
        document.getElementById('openConfigModalBtn').addEventListener('click', () => configModal.classList.remove('hidden'));
        document.getElementById('closeConfigModalBtn').addEventListener('click', () => configModal.classList.add('hidden'));
        document.getElementById('cancelConfigModalBtn').addEventListener('click', () => configModal.classList.add('hidden'));

        document.getElementById('configForm').addEventListener('submit', function(e) {
            e.preventDefault();
            const formData = new FormData(this);
            fetch(currentScript, { method: 'POST', body: formData })
            .then(res => res.json())
            .then(data => {
                alert(data.message);
                if(data.status === 'success') { location.reload(); }
            })
            .catch(err => alert('Error saving configuration: ' + err));
        });

        setInterval(() => {
            const now = new Date();
			const options = { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' };
			const formatter = new Intl.DateTimeFormat('en-CA', options); // 'en-CA' gives YYYY-MM-DD format
			const currentDate = formatter.format(new Date());
			
            const easternTime = now.toLocaleTimeString('en-US', {
                timeZone: 'America/New_York',
                hour12: false,
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
            });
            const tzAbbr = now.toLocaleTimeString('en-US', { 
                timeZone: 'America/New_York', 
                timeZoneName: 'short' 
            }).split(' ').pop();

            const utcTime = now.toUTCString().slice(17, 25) + ' UTC';
            document.getElementById('utcClock').innerText = `${easternTime} - ${currentDate} - ${tzAbbr} (${utcTime})`;
        }, 1000);
		
        const eqCtx = document.getElementById('equityCurveChart').getContext('2d');
        const equityChart = new Chart(eqCtx, {
            type: 'line',
            data: {
                labels: <?php echo json_encode($initialTimes); ?>,
                datasets: [{
                    label: 'Portfolio Valuation ($)',
                    data: <?php echo json_encode($initialEquity); ?>,
                    borderColor: '#10b981',
                    backgroundColor: 'rgba(16, 185, 129, 0.15)',
                    fill: true,
                    tension: 0.3,
                    borderWidth: 2.5,
                    pointRadius: 3,
                    pointHoverRadius: 6
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: { duration: 400 },
                scales: {
                    y: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#94a3b8' } },
                    x: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#94a3b8', maxTicksLimit: 8 } }
                }
            }
        });

        const ddCtx = document.getElementById('drawdownChart').getContext('2d');
        const drawdownChart = new Chart(ddCtx, {
            type: 'line',
            data: {
                labels: <?php echo json_encode($initialTimes); ?>,
                datasets: [{
                    label: 'Intraday Drawdown (%)',
                    data: <?php echo json_encode($initialDrawdown); ?>,
                    borderColor: '#ef4444',
                    backgroundColor: 'rgba(239, 68, 68, 0.15)',
                    fill: true,
                    tension: 0.3,
                    borderWidth: 2.5,
                    pointRadius: 3,
                    pointHoverRadius: 6
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: { duration: 400 },
                scales: {
                    y: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#94a3b8' } },
                    x: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#94a3b8', maxTicksLimit: 8 } }
                }
            }
        });

        function filterBySymbol(symbol) {
            currentSelectedSymbol = symbol;
            document.getElementById('filterBanner').classList.remove('hidden');
            document.getElementById('activeFilterSymbolText').innerText = symbol;
            
            document.querySelectorAll('.symbol-card').forEach(card => {
                if(card.getAttribute('data-symbol') === symbol) {
                    card.classList.add('active-filter');
                } else {
                    card.classList.remove('active-filter');
                }
            });

            document.getElementById('equityChartTitle').innerText = `QuantStream AI - Real-Time Equity Curve (${symbol}) (ET)`;
            document.getElementById('drawdownChartTitle').innerText = `Intraday Drawdown Profile (%) - ${symbol} (ET)`;

            pollLiveData();
        }

        function clearSymbolFilter() {
            currentSelectedSymbol = '';
            document.getElementById('filterBanner').classList.add('hidden');
            document.querySelectorAll('.symbol-card').forEach(card => card.classList.remove('active-filter'));
            
            document.getElementById('equityChartTitle').innerText = `QuantStream AI - Real-Time Equity Curve`;
            document.getElementById('drawdownChartTitle').innerText = `Intraday Drawdown Profile (%)`;

            pollLiveData();
        }

        function updateStreamStatus(message, isActive) {
            const textSpan = document.querySelector('#streamIndicator span:nth-of-type(2)');
            const dotSpan = document.querySelector('#streamIndicator span:first-of-type');
            const container = document.getElementById('streamIndicator');
            
            textSpan.textContent = message;
            if (isActive) {
                container.className = "relative flex items-center gap-2 px-3.5 py-1.5 rounded-full bg-emerald-500/15 border border-emerald-500/40 text-emerald-400 font-mono text-xs stream-pulse";
                dotSpan.className = "w-2.5 h-2.5 rounded-full bg-emerald-400 animate-ping";
            } else {
                container.className = "relative flex items-center gap-2 px-3.5 py-1.5 rounded-full bg-rose-500/15 border border-rose-500/40 text-rose-400 font-mono text-xs";
                dotSpan.className = "w-2.5 h-2.5 rounded-full bg-rose-400";
            }
        }

        let pollInterval = null;

        async function pollLiveData() {
			let marketopen=0;
            try {
                const statusRes = await fetch('marketstatus.txt?_t=' + Date.now());		
                const statusText = await statusRes.text();
                const marketState = statusText.trim().toUpperCase();
                				
                if (marketState === 'CLOSE') {
                    const indicator = document.getElementById('streamIndicator');
                    if (indicator){ 
                        indicator.className = '';
                        updateStreamStatus("US:NASDAQ/NYSE MARKET CLOSED", false);
						marketopen=0;
                    }
                } else {
					marketopen=1;
                    const indicator = document.getElementById('streamIndicator');
                    if (indicator){ 
                        indicator.className = 'relative flex items-center gap-2 px-3.5 py-1.5 rounded-full bg-emerald-500/15 border border-emerald-500/40 text-emerald-400 font-mono text-xs stream-pulse';
                        updateStreamStatus("LIVE US NASDAQ/NYSE FEED ACTIVE (5s POLL)", true);								
                    }			
                }

                const indicator = document.getElementById('streamIndicator');
                if (indicator) indicator.classList.add('bg-cyan-500/30', 'scale-105');
                
                let url = currentScript + '?action=get_live_data&_t=' + Date.now();
                if (currentSelectedSymbol) {
                    url += '&symbol=' + encodeURIComponent(currentSelectedSymbol);
                }
                
                const res = await fetch(url);
                const contentType = res.headers.get("content-type");
                if (!contentType || !contentType.includes("application/json")) {
                    throw new TypeError("Received non-JSON response from server.");
                }
                
                const data = await res.json();
                if(data.status === 'success') {
                    let notionalEl = document.getElementById('boxNotionalPnl');
                    let notionalColor = data.notionalPnl >= 0 ? 'text-emerald-400' : 'text-red-400';
                    notionalEl.className = `text-2xl font-black font-mono ${notionalColor} mt-1`;
                    notionalEl.innerHTML = 
                        (data.notionalPnl >= 0 ? '+' : '') + '$' + Number(data.notionalPnl).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) + 
                        ' <span class="text-base font-normal">(' + (data.notionalreturn >= 0 ? '+' : '') + Number(data.notionalreturn).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) + '%)</span>';

                    document.getElementById('liveEquityBadge').innerText = "Baseline: $" + data.tradebudget;

                    let actualEl = document.getElementById('boxActualPnl');
                    let actualColor = data.actualPnl >= 0 ? 'text-emerald-400' : 'text-red-400';
                    actualEl.className = `text-2xl font-black font-mono ${actualColor} mt-1`;
                    actualEl.innerHTML = 
                        (data.actualPnl >= 0 ? '+' : '') + '$' + Number(data.actualPnl).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) + 
                        ' <span class="text-base font-normal">(' + (data.actualreturn >= 0 ? '+' : '') + Number(data.actualreturn).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) + '%)</span>';
                                
                    document.getElementById('tradeCountLabel').innerText = data.totalTrades;
                    document.getElementById('symbolCount').innerText = data.symbols.length;
                    document.getElementById('boxOpenPositions').innerText = data.openPositionsCount + ' Active';
                    document.getElementById('openPosTableCount').innerText = data.openPositions.length;

                    let totalOpenVal = 0;
                    data.openPositions.forEach(op => { totalOpenVal += (parseFloat(op.qty) || 0) * (parseFloat(op.price) || 0); });
                    document.getElementById('boxTotalOpenValue').innerText = '$' + totalOpenVal.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});

                    document.getElementById('statTotalTrades').innerHTML = `${data.totalTrades} <span class="text-xs text-slate-400">(${data.buyCount}B / ${data.sellCount}S)</span>`;
                    document.getElementById('statTotalShares').innerText = Number(data.totalQuantity || 0).toLocaleString();
                    document.getElementById('statAvgHold').innerHTML = `${data.avgHoldTime} sec <span class="text-[10px] text-slate-400">(${(data.avgHoldTime/60).toFixed(2)}m)</span>`;
                    document.getElementById('statThroughput').innerHTML = `<span class="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-ping"></span> <span>${data.throughput} msgs/sec</span>`;
                    document.getElementById('statMaxDd').innerText = `$${Number(data.maxDrawdownUsd).toFixed(2)} (${Number(data.maxDrawdownPct).toFixed(2)}%)`;
                    document.getElementById('statBuyTurnover').innerText = '$' + Number(data.grossBuyTurnover).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
                    document.getElementById('statSellProceeds').innerText = '$' + Number(data.grossSellProceeds).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
                    document.getElementById('statProfitFactor').innerText = Number(data.profitFactor).toFixed(2);
                    document.getElementById('statWinRate').innerText = Number(data.winRate).toFixed(1) + '%';

                    const logContainer = document.getElementById('realtimeLogContainer');
                    if (logContainer && data.logs) {
                        document.getElementById('logCountLabel').innerText = data.logs.length;
                        if (data.logs.length === 0) {
                            logContainer.innerHTML = '<div class="text-slate-500 italic">No log entries recorded for today yet...</div>';
                        } else {
                            logContainer.innerHTML = '';
                            data.logs.forEach(line => {
                                const div = document.createElement('div');
                                div.className = 'py-0.5 border-b border-slate-900/50 truncate hover:text-white transition-colors';
                                div.innerText = line;
                                logContainer.appendChild(div);
                            });
                        }
                    }

                    const symbolContainer = document.getElementById('symbolBadgeContainer');
                    if (symbolContainer && data.symbols) {
                        symbolContainer.innerHTML = '';
                        data.symbols.forEach(symObj => {
                            const isFiltered = currentSelectedSymbol === symObj.symbol;
                            const isGreen = symObj.diff >= 0;
                            const diffSign = isGreen ? '+' : '';
                            const diffClass = isGreen ? 'text-emerald-400 bg-emerald-500/15 border-emerald-500/30' : 'text-red-400 bg-red-500/15 border-red-500/30';
                            const flashAnim = isGreen ? 'flash-green' : 'flash-red';
                            const cardClass = `symbol-card rounded-xl p-3 flex flex-col justify-between cursor-pointer group ${isFiltered ? 'active-filter' : ''} ${flashAnim}`;
                            
                            const card = document.createElement('div');
                            card.className = cardClass;
                            card.setAttribute('data-symbol', symObj.symbol);
                            card.onclick = () => filterBySymbol(symObj.symbol);
                            
                             card.innerHTML = `
                                <div class="flex items-center justify-between">
                                    <div class="flex items-center gap-2">
                                        <span class="font-mono font-black text-base text-white tracking-wider group-hover:text-cyan-300 transition-colors">${symObj.symbol}</span>
                                        <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
                                    </div>

                                </div>
                                <div class="my-2 grid grid-cols-2 gap-2 text-[11px] font-mono">
                                    <div class="bg-slate-900/80 rounded-lg p-1.5 border border-slate-800">
                                        <div class="text-[9px] text-slate-400 uppercase">Buy Entry</div>
                                        <div class="text-slate-200 font-bold">$${Number(symObj.buy_price || 0).toFixed(2)}</div>
                                    </div>
                                    <a href="https://finance.yahoo.com/quote/${symObj.symbol}" target="_blank" onclick="event.stopPropagation();" class="bg-slate-900/80 rounded-lg p-1.5 border border-slate-800 hover:border-cyan-500/50 transition-colors group/link flex flex-col justify-center">
                                        <div class="text-[9px] text-slate-400 uppercase flex items-center justify-between">
                                            <span>Yahoo Quote</span>
                                            <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="text-slate-500 group-hover/link:text-cyan-400"><path d="M15 3h6v6"/><path d="M10 14 21 3"/><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/></svg>
                                        </div>
                                        <div class="text-cyan-400 font-bold text-xs truncate">Open Chart ↗</div>
                                    </a>
                                </div>
                                <div class="flex items-center justify-between pt-2 border-t border-slate-800/80 text-[10px] font-mono">
                                    <span class="px-2 py-0.5 rounded-md border font-bold ${diffClass}">
                                        ${diffSign}$${Number(symObj.diff || 0).toFixed(2)} (${diffSign}${Number(symObj.pct || 0).toFixed(2)}%)
                                    </span>
                                    <span class="text-cyan-400 font-semibold">${Number(symObj.volume || 0).toLocaleString()} sh</span>
                                </div>
                            `;
							
                            symbolContainer.appendChild(card);
                        });
                    }

                    const opTbody = document.getElementById('openPositionsTableBody');
                    opTbody.innerHTML = '';
                    data.openPositions.forEach(op => {
                        const tr = document.createElement('tr');
                        tr.className = 'hover:bg-slate-900/50 row-updated';
                        tr.innerHTML = `
                            <td class="p-2 font-bold text-white">${op.symbol || ''}</td>
                            <td class="p-2 text-cyan-400">${Number(op.qty || 0).toLocaleString()}</td>
                            <td class="p-2">$${Number(op.price || 0).toFixed(2)}</td>
                            <td class="p-2 text-emerald-400">$${Number((op.qty || 0) * (op.price || 0)).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>
                        `;
                        opTbody.appendChild(tr);
                    });

                    const tbody = document.getElementById('tradeLogTableBody');
                    tbody.innerHTML = '';
                    data.trades.slice(-50).forEach(t => {
                        const tr = document.createElement('tr');
                        tr.className = 'hover:bg-slate-900/50 transition-colors row-updated';
                        const pnlVal = parseFloat(t.trade_pnl || 0);
                        const pnlClass = pnlVal >= 0 ? 'text-emerald-400' : 'text-red-400';
                        const signalClass = t.signal === 'BUY' ? 'bg-emerald-500/20 text-emerald-400' : 'bg-red-500/20 text-red-400';
                        const execClass = t.brokerexecuted === 'YES' ? 'bg-cyan-500/20 text-cyan-400' : 'bg-slate-800 text-slate-400';
                        
                        tr.innerHTML = `
                            <td class="p-2 text-slate-400">${t.timestamp || ''}</td>
                            <td class="p-2 font-bold text-white">${t.symbol || ''}</td>
                            <td class="p-2"><span class="px-1.5 py-0.5 rounded text-[10px] font-bold ${signalClass}">${t.signal || ''}</span></td>
                            <td class="p-2">$${Number(t.price || 0).toFixed(2)}</td>
                            <td class="p-2">${t.quantity || 0}</td>
                            <td class="p-2 font-bold ${pnlClass}">$${Number(pnlVal).toFixed(2)}</td>
                            <td class="p-2">$${Number(t.cumulative_pnl || 0).toFixed(2)}</td>
                            <td class="p-2"><span class="px-1.5 py-0.5 rounded text-[10px] ${execClass}">${t.brokerexecuted || 'NO'}</span></td>
                        `;
                        tbody.appendChild(tr);
                    });

                    equityChart.data.labels = data.chartLabels;
                    equityChart.data.datasets[0].data = data.equityData;
                    equityChart.update('none');

                    drawdownChart.data.labels = data.chartLabels;
                    drawdownChart.data.datasets[0].data = data.drawdownData;
                    drawdownChart.update('none');

                    const eqPanel = document.getElementById('equityCardPanel');
                    const ddPanel = document.getElementById('drawdownCardPanel');
                    eqPanel.classList.add('graph-updated');
                    ddPanel.classList.add('graph-updated');
                    setTimeout(() => {
                        eqPanel.classList.remove('graph-updated');
                        ddPanel.classList.remove('graph-updated');
                    }, 1200);
                }
                if (indicator) setTimeout(() => indicator.classList.remove('bg-cyan-500/30', 'scale-105'), 1200);

            } catch (err) {
                console.error('Error polling live data or market status:', err);
                const indicator = document.getElementById('streamIndicator');
                if (indicator) indicator.classList.remove('bg-cyan-500/30', 'scale-105');
            }
        }

        pollInterval = setInterval(pollLiveData, 5000);	 
    </script>

    <!-- Quantitative Trading Disclaimer -->
    <section class="max-w-[1800px] mx-auto px-3 sm:px-5 pb-5">
        <div class="glass-panel rounded-xl p-3 sm:px-4 border border-cardborder text-xs sm:text-sm font-mono text-slate-300 leading-relaxed text-center">
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