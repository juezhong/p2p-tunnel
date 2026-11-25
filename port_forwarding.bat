@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem ---- 参数快查：这些分支不走提权 ----
if /i "%~1"=="--elevated" goto after_cli
if /i "%~1"=="-h" goto usage
if /i "%~1"=="--help" goto usage
if /i "%~1"=="-l" goto list_only
if /i "%~1"=="--list" goto list_only
if /i "%~1"=="-p" goto ports_only
if /i "%~1"=="--ports" goto ports_only
if not "%~1"=="" goto bad_arg

:after_cli

rem ================= 自动提权 =================
net session >nul 2>&1
if errorlevel 1 (
    if /i "%~1"=="--elevated" (
        echo [警告] 已经提权，但检测不到管理员权限（Server 服务可能被关）。
        echo        继续执行，如果 netsh 报错就是权限问题。
        echo.
    ) else (
        echo 需要管理员权限，正在申请 UAC 提权...
        powershell -NoProfile -Command "Start-Process -FilePath '%~f0' -ArgumentList '--elevated' -Verb RunAs"
        if errorlevel 1 (
            echo 提权被取消或失败。
            pause
        )
        exit /b
    )
)

rem ================= 主菜单 =================
:menu
cls
echo ============================================================
echo   当前转发规则:
netsh interface portproxy show v4tov4 2>nul
echo ============================================================
echo   [1] 添加转发规则
echo   [2] 删除转发规则（列表选择）
echo   [3] 只看规则详情
echo   [4] 查看本机监听端口
echo   [q] 退出
echo.
set "CH="
set /p "CH=请选择: "
set "CH=!CH: =!"
if /i "!CH!"=="1" goto add
if /i "!CH!"=="2" goto del
if /i "!CH!"=="3" goto show
if /i "!CH!"=="4" goto ports
if /i "!CH!"=="q" goto quit
if defined CH set "mtry=0"
if not defined CH set /a mtry+=1
if not defined CH if !mtry! GEQ 6 goto menu_eof
echo.
echo [提示] 只能输入 1 / 2 / 3 / 4 / q
pause
goto menu

rem ================= 1. 添加 =================
:add
cls
echo === 添加转发规则 ===
echo 一共 4 步。任意一步输入 q 回车可返回主菜单。
echo.
set "try=0"

:add_lip
set "lip="
set /p "lip=1/4 监听 IP（本机用哪个地址接连接；直接回车取 0.0.0.0 即所有网卡；q 返回）: "
set "lip=!lip: =!"
if /i "!lip!"=="q" goto menu
call :clean_ip "!lip!"
set "lip=!cv!"
if not defined lip set "lip=0.0.0.0"
call :is_ip "!lip!"
if not "!ok!"=="1" (
    echo.
    echo    [错误] IP 格式不对: [!lip!]
    echo           只用英文半角的数字和点，四段，每段 0-255
    goto add_lip
)

:add_lport
set "lport="
set /p "lport=2/4 监听端口（本机对外开放哪个端口；q 返回）: "
set "lport=!lport: =!"
if /i "!lport!"=="q" goto menu
call :clean_port "!lport!"
set "lport=!cv!"
if defined lport goto add_lport_ok
set /a try+=1
if !try! GEQ 4 goto add_eof
goto add_lport
:add_lport_ok
set "try=0"
call :is_port "!lport!"
if not "!ok!"=="1" (
    echo.
    echo    [错误] 端口必须是 1-65535 的数字，读到的值: [!lport!]
    goto add_lport
)
set "lport=!q!"

:add_tip
set "tip="
set /p "tip=3/4 目标 IP（填一个 IPv4 地址：本机收到的连接会被转发到这台机器，服务跑在它上面；q 返回）: "
set "tip=!tip: =!"
if /i "!tip!"=="q" goto menu
call :clean_ip "!tip!"
set "tip=!cv!"
if defined tip goto add_tip_ok
set /a try+=1
if !try! GEQ 4 goto add_eof
goto add_tip
:add_tip_ok
set "try=0"
call :is_ip "!tip!"
if not "!ok!"=="1" (
    echo.
    echo    [错误] IP 格式不对: [!tip!]
    echo           只用英文半角的数字和点，四段，每段 0-255
    goto add_tip
)

:add_tport
set "tport="
set /p "tport=4/4 目标端口（3/4 那台机器上服务的端口；q 返回）: "
set "tport=!tport: =!"
if /i "!tport!"=="q" goto menu
call :clean_port "!tport!"
set "tport=!cv!"
if defined tport goto add_tport_ok
set /a try+=1
if !try! GEQ 4 goto add_eof
goto add_tport
:add_tport_ok
set "try=0"
call :is_port "!tport!"
if not "!ok!"=="1" (
    echo.
    echo    [错误] 端口必须是 1-65535 的数字，读到的值: [!tport!]
    goto add_tport
)
set "tport=!q!"

call :collect_rules
call :verify_rule "!lip!" "!lport!"
if "!found!"=="1" (
    echo.
    echo [提示] 规则 !lip!:!lport! 已经存在，直接添加会失败。
    echo        要改目标地址，请先回主菜单选 2 删掉旧规则。
    pause
    goto menu
)

call :port_listening "!lip!" "!lport!"
if "!lis!"=="1" (
    echo.
    echo [警告] 本机 !lip!:!lport! 已经有程序在监听，这条转发规则很可能建立失败。
    echo        可以选 4 看是哪个进程占用，或在任务管理器里按 PID 查。
    set "go="
    set /p "go=仍要尝试添加? [y/N]: "
    set "go=!go: =!"
    if /i not "!go!"=="y" goto menu
)

echo.
echo 执行命令:
echo   netsh interface portproxy add v4tov4 listenaddress=!lip! listenport=!lport! connectaddress=!tip! connectport=!tport!
echo.
netsh interface portproxy add v4tov4 listenaddress=!lip! listenport=!lport! connectaddress=!tip! connectport=!tport!
set "rc=!errorlevel!"
echo.
call :collect_rules
call :verify_rule "!lip!" "!lport!"
if "!found!"=="1" (
    echo [完成] 规则已添加: !lip!:!lport!  --  !tip!:!tport!
) else (
    echo [失败] netsh 返回码 !rc!，规则列表里没有出现 !lip!:!lport!
    echo        上面 netsh 打印的那几行原文请发给我。
)
echo.
pause
goto menu

rem ================= 2. 删除（菜单选择） =================
:del
cls
echo === 删除转发规则 ===
echo.
call :collect_rules
if !n! EQU 0 (
    echo 当前没有转发规则。
    pause
    goto menu
)
echo   序号   监听地址              端口    目标地址              端口
echo   ----   --------------------  -----   --------------------  -----
for /l %%i in (1,1,!n!) do call :print_rule %%i
echo.
set "sel="
set /p "sel=要删除第几条? [回车或 q 返回]: "
set "sel=!sel: =!"
if "!sel!"=="" goto menu
if /i "!sel!"=="q" goto menu
call :is_port "!sel!"
if not "!ok!"=="1" (
    echo [错误] 序号无效: !sel!
    pause
    goto del
)
set "sel=!q!"
if !sel! GTR !n! (
    echo [错误] 没有第 !sel! 条规则。
    pause
    goto del
)
call :get_rule !sel!
echo.
echo 即将删除:  !GA!:!GB!  --  !GC!:!GD!
set "y="
set /p "y=确认删除? [y/N]: "
set "y=!y: =!"
if /i not "!y!"=="y" (
    echo 已取消。
    pause
    goto menu
)
netsh interface portproxy delete v4tov4 listenaddress=!GA! listenport=!GB!
if errorlevel 1 (
    echo.
    echo [失败] 删除出错，请看上面 netsh 的输出。
) else (
    echo.
    echo [完成] 已删除 !GA!:!GB!
)
echo.
pause
goto menu

rem ================= 3. 查看规则 =================
:show
cls
echo === 当前转发规则 ===
echo.
netsh interface portproxy show v4tov4
echo.
pause
goto menu

rem ================= 4. 查看监听端口 =================
:ports
cls
call :load_listening
if !Ln! EQU 0 (
    echo 没有正在监听的端口。
    echo.
    pause
    goto menu
)
set "per=18"
set /a pages=(Ln+per-1)/per
set "page=0"

:ports_show
cls
echo === 本机 TCP 监听端口（共 !Ln! 条） ===
echo.
if !page! EQU 0 (
    echo   列说明:
    echo     本地地址 = 本机正在监听的 地址:端口
    echo     外部地址 = 已经连上来的对方地址，0.0.0.0:0 表示还没有连接
    echo     状态     = LISTENING 表示正在监听
    echo     PID      = 占用这个端口的进程号，可用任务管理器 - 详细信息 里按 PID 查
    echo.
)
echo   本地地址                 外部地址                 状态        PID
echo   ------------------------ ------------------------ ----------- ------
set /a s=page*per+1
set /a e=s+per-1
if !e! GTR !Ln! set "e=!Ln!"
for /l %%i in (!s!,1,!e!) do echo   !L_%%i!
echo.
set /a pg=page+1
echo   第 !pg! / !pages! 页    n/p 或 j/k 翻页（按一下就行，不用回车），q 返回
call :read_key
if /i "!K!"=="q" goto ports_done
if /i "!K!"=="p" (
    if !page! GTR 0 set /a page-=1
    goto ports_show
)
if /i "!K!"=="n" (
    set /a page+=1
    if !page! GEQ !pages! set /a page=pages-1
)
goto ports_show

:ports_done
echo.
call :collect_rules
call :conflicts
echo.
pause
goto menu

rem 读一个按键（不用回车），结果放 K: p 上一页 / n 下一页 / q 返回 / x 其它
:read_key
choice /c npjqk /n >nul
set "k_rc=!errorlevel!"
set "K=x"
if "!k_rc!"=="1" set "K=n"
if "!k_rc!"=="2" set "K=p"
if "!k_rc!"=="3" set "K=n"
if "!k_rc!"=="4" set "K=q"
if "!k_rc!"=="5" set "K=p"
exit /b

rem 收集监听行到 L_1..L_n，总数放 Ln
:load_listening
set "Ln=0"
for /f "usebackq delims=" %%L in (`netstat -ano ^| findstr /C:"LISTENING"`) do (
    set /a Ln+=1
    set "L_!Ln!=%%L"
)
exit /b

rem ================= 退出 =================
:quit
echo 退出。
exit /b

:menu_eof
echo.
echo [错误] 连续多次读到空输入，输入流可能已经结束。
echo        请直接双击 port_forwarding.bat，或在 cmd 窗口里运行它；
echo        不要在管道、重定向或非交互环境里跑。
echo.
pause
exit /b

:add_eof
echo.
echo [错误] 连续多次读到空输入，输入流可能已经结束。
echo        请直接双击 port_forwarding.bat，或在 cmd 窗口里运行它；
echo        不要在管道、重定向或非交互环境里跑。
echo.
pause
goto menu

:usage
cls
goto usage_body

:bad_arg
cls
echo [错误] 不认识的参数: %~1
echo.
:usage_body
echo 用法: port_forwarding.bat [选项]
echo.
echo   port_forwarding.bat        打开交互菜单（需要管理员权限，会自动申请 UAC）
echo   port_forwarding.bat -l     列出已有转发规则（不提权）
echo   port_forwarding.bat -p     列出本机正在监听的端口（不提权）
echo   port_forwarding.bat -h     显示这段帮助
echo.
echo 例: port_forwarding.bat -l
echo.
call :cli_pause
exit /b

:list_only
cls
echo === 当前转发规则 ===
echo.
netsh interface portproxy show v4tov4
echo.
call :cli_pause
exit /b

:ports_only
cls
echo === 本机 TCP 监听端口 ===
echo.
echo   本地地址                 外部地址                 状态        PID
echo   ------------------------ ------------------------ ----------- ------
call :load_listening
if !Ln! EQU 0 echo   （没有正在监听的端口）
for /l %%i in (1,1,!Ln!) do echo   !L_%%i!
echo.
call :cli_pause
exit /b

rem 双击运行时窗口会闪退，补一次 pause；从 cmd 窗口里运行则不打扰
:cli_pause
echo "%cmdcmdline%" | findstr /i /c:"/c" >nul
if not errorlevel 1 pause
exit /b

rem ================= 子程序 =================

rem 校验端口: 纯数字且 1-65535，结果放 ok，规范化后的值放 q
:is_port
set "ok=0"
set "q="
set "v=%~1"
if "!v!"=="" exit /b
set "r=!v!"
set "num="
set "nd=0"

:is_port_loop
if "!r!"=="" goto is_port_tail
set "c=!r:~0,1!"
set "r=!r:~1!"
set "d=0"
for %%z in (0 1 2 3 4 5 6 7 8 9) do if "!c!"=="%%z" set "d=1"
if "!d!"=="0" exit /b
set "num=!num!!c!"
set /a nd+=1
if !nd! GTR 5 exit /b
goto is_port_loop

:is_port_tail
if "!num!"=="" exit /b
if !num! LSS 1 exit /b
if !num! GTR 65535 exit /b
set "q=!num!"
set "ok=1"
exit /b

rem 校验 IPv4，结果放 ok
:is_ip
set "ok=0"
set "v=%~1"
if "!v!"=="" exit /b
set "r=!v!"
set "octet="
set "dots=0"
set "nd=0"

:is_ip_loop
if "!r!"=="" goto is_ip_tail
set "c=!r:~0,1!"
set "r=!r:~1!"
if not "!c!"=="." goto is_ip_digit
if "!octet!"=="" exit /b
if !nd! GTR 3 exit /b
if !octet! GTR 255 exit /b
set /a dots+=1
if !dots! GTR 3 exit /b
set "octet="
set "nd=0"
goto is_ip_loop

:is_ip_digit
set "d=0"
for %%z in (0 1 2 3 4 5 6 7 8 9) do if "!c!"=="%%z" set "d=1"
if "!d!"=="0" exit /b
set "octet=!octet!!c!"
set /a nd+=1
if !nd! GTR 3 exit /b
goto is_ip_loop

:is_ip_tail
if "!octet!"=="" exit /b
if !nd! GTR 3 exit /b
if !octet! GTR 255 exit /b
if !dots! NEQ 3 exit /b
set "ok=1"
exit /b

rem 只保留数字和点，结果放 cv（把终端混进来的杂字符挡掉）
:clean_ip
set "cv="
set "r=%~1"
:c_ip_loop
if "!r!"=="" exit /b
set "c=!r:~0,1!"
set "r=!r:~1!"
if "!c!"=="." goto c_ip_dot
set "d=0"
for %%z in (0 1 2 3 4 5 6 7 8 9) do if "!c!"=="%%z" set "d=1"
if "!d!"=="1" set "cv=!cv!!c!"
goto c_ip_loop
:c_ip_dot
set "cv=!cv!."
goto c_ip_loop

rem 只保留数字，结果放 cv
:clean_port
set "cv="
set "r=%~1"
:c_port_loop
if "!r!"=="" exit /b
set "c=!r:~0,1!"
set "r=!r:~1!"
set "d=0"
for %%z in (0 1 2 3 4 5 6 7 8 9) do if "!c!"=="%%z" set "d=1"
if "!d!"=="1" set "cv=!cv!!c!"
goto c_port_loop

rem 读规则列表到 R_序号_列，总数放 n
:collect_rules
set "n=0"
for /f "tokens=1,2,3,4" %%a in ('netsh interface portproxy show v4tov4 2^>nul') do (
    call :is_port "%%b"
    if "!ok!"=="1" (
        call :is_port "%%d"
        if "!ok!"=="1" (
            set /a n+=1
            set "R_!n!_A=%%a"
            set "R_!n!_B=%%b"
            set "R_!n!_C=%%c"
            set "R_!n!_D=%%d"
        )
    )
)
exit /b

:print_rule
echo   [%~1]   !R_%~1_A!   !R_%~1_B!   !R_%~1_C!   !R_%~1_D!
exit /b

:get_rule
set "GA=!R_%~1_A!"
set "GB=!R_%~1_B!"
set "GC=!R_%~1_C!"
set "GD=!R_%~1_D!"
exit /b

rem 检查某条规则是否已经出现在列表里，结果放 found
:verify_rule
set "found=0"
set "i=0"
:v_loop
set /a i+=1
if !i! GTR !n! exit /b
call :get_rule !i!
if /i "!GA!"=="%~1" if "!GB!"=="%~2" set "found=1"
goto v_loop

rem 判断 addr:port 是否已被监听，结果放 lis
:port_listening
set "lis=0"
netstat -ano | findstr /C:" 0.0.0.0:%~2 " /C:" [::]:%~2 " /C:" %~1:%~2 " >nul && set "lis=1"
exit /b

rem 逐条检查规则端口是否冲突
:conflicts
set "i=0"
:c_loop
set /a i+=1
if !i! GTR !n! exit /b
call :get_rule !i!
call :port_listening "!GA!" "!GB!"
if "!lis!"=="1" echo [注意] 端口 !GB! 已被监听，规则 !GA!:!GB! 可能建立失败
goto c_loop
