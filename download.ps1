# ==============================================
# NYC Taxi Data Downloader
# ==============================================

# 🔧 НАСТРОЙКА: Укажите года для загрузки
$Years = @(2010,2011,2012,2013,2014,2015,2016, 2017, 2018,1019,2020,2021,2022,2023,2024,2025)
#$Years = @(2010)

# Базовая директория для данных
$BaseDir = "E:\NYCTaxi"
# Или можно задать конкретный путь:
# $BaseDir = "E:\NYCTaxi"
# Или в текущей директории:
# $BaseDir = ".\NYCTaxi"

# Базовый URL
$BaseUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# ==============================================
# Основной цикл
# ==============================================
Write-Host "`n🚕 Starting NYC Taxi data download...`n" -ForegroundColor Green

foreach ($Year in $Years) {
    # Создать директорию для года
    $DataDir = Join-Path $BaseDir $Year
    New-Item -ItemType Directory -Force -Path $DataDir | Out-Null
    Set-Location $DataDir
    
    Write-Host "📅 Processing year: $Year" -ForegroundColor Cyan
    Write-Host "📁 Directory: $DataDir`n" -ForegroundColor Cyan
    
    # Скачать все месяцы
    1..12 | ForEach-Object {
        $Month = $_.ToString("00")
        $FileName = "yellow_tripdata_$Year-$Month.parquet"
        $FileUrl = "$BaseUrl/$FileName"
        
        Write-Host "📦 Downloading $FileName..." -ForegroundColor Yellow
        
        try {
            Invoke-WebRequest -Uri $FileUrl -OutFile $FileName -UseBasicParsing
            
            # Показать размер
            $FileSize = (Get-Item $FileName).Length / 1MB
            Write-Host "   ✅ Downloaded: $FileName ($([math]::Round($FileSize, 2)) MB)" -ForegroundColor Green
        }
        catch {
            Write-Host "   ❌ Error downloading $FileName : $_" -ForegroundColor Red
        }
    }
    
    # Скачать справочник зон (только один раз для каждого года)
    if (-not (Test-Path "taxi_zone_lookup.csv")) {
        Write-Host "`n📍 Downloading Taxi Zone Lookup..." -ForegroundColor Yellow
        
        try {
            Invoke-WebRequest -Uri "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv" -OutFile "taxi_zone_lookup.csv" -UseBasicParsing
            Write-Host "   ✅ Downloaded: taxi_zone_lookup.csv" -ForegroundColor Green
        }
        catch {
            Write-Host "   ❌ Error downloading zone lookup: $_" -ForegroundColor Red
        }
    }
    
    # Статистика по году
    Write-Host "`n📊 Summary for $Year :" -ForegroundColor Cyan
    $FileCount = (Get-ChildItem -Filter "*.parquet").Count
    $TotalSize = (Get-ChildItem -Filter "*.parquet" | Measure-Object -Property Length -Sum).Sum / 1GB
    
    Write-Host "   Total parquet files: $FileCount" -ForegroundColor White
    Write-Host "   Total size: $([math]::Round($TotalSize, 2)) GB" -ForegroundColor White
    Write-Host "✅ Year $Year complete!`n" -ForegroundColor Green
    Write-Host "========================================`n"
}

Write-Host "🎉 All downloads complete!" -ForegroundColor Green
Write-Host "📁 Data saved to: $BaseDir" -ForegroundColor Green
