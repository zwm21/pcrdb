@echo off
chcp 65001 >nul

rem 用法: daily_sync.bat [qsdk|bsdk]
rem   不带参数: 运行后在交互中选择渠道 (默认渠道服)
rem   带参数:   跳过渠道选择, 直接采集指定渠道

set "CHANNEL=%~1"

echo============================================================
if /i "%CHANNEL%"=="" (
    echo 正在执行每日同步组合任务... ^(运行后可选择渠道^)
) else (
    echo 正在执行每日同步组合任务... ^(渠道: %CHANNEL%^)
)
echo 工作目录: %cd%
echo============================================================

if /i "%CHANNEL%"=="" (
    python cli.py task daily_sync
) else (
    python cli.py --channel %CHANNEL% task daily_sync --args channel=%CHANNEL%
)

echo.
echo============================================================
echo 任务执行完毕，按任意键退出...
pause >nul
