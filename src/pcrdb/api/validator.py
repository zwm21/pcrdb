"""
极验远程过码 (移植自 autopcr/sdk/validator.py 的 remoteValidator)

B站 SDK 登录触发极验 (code=200000) 时, 通过远程服务完成点选验证,
拿到 challenge / gt_user_id / validate 供二次登录使用。

默认使用公共服务 https://pcrd.tencentbot.top (autopcr 生产在用),
可用环境变量 PCRDB_GEETEST_API 覆盖 (base url, 不带路径)。
"""
import os
import json
import asyncio

import aiohttp


async def remote_validator():
    """
    远程过码

    Returns:
        dict: {'challenge': ..., 'gt_user_id': ..., 'validate': ...} 成功
        None: 失败
    """
    base = os.getenv('PCRDB_GEETEST_API', 'https://pcrd.tencentbot.top').rstrip('/')
    header = {"Content-Type": "application/json", "User-Agent": "pcrdb/1.0.0"}
    ret = None

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{base}/geetest_renew", headers=header) as resp:
                res = json.loads(await resp.text())
            uuid = res["uuid"]
            print(f"[极验] 开始远程过码, uuid={uuid}")

            ccnt = 0
            up = 5
            while ccnt <= up:
                ccnt += 1
                async with session.get(f"{base}/check/{uuid}", headers=header) as resp:
                    res = json.loads(await resp.text())

                if "queue_num" in res:
                    nu = res["queue_num"]
                    if nu >= 35:
                        raise Exception("Captcha failed (queue full)")
                    tim = min(int(nu), 3) * 10
                    print(f"[极验] 排队中 queue_num={nu}, 等待 {tim}s...")
                    await asyncio.sleep(tim)
                    if tim >= 40:
                        ccnt += 2
                else:
                    info = res.get("info")
                    if info in ["fail", "url invalid"]:
                        raise Exception("Captcha failed")
                    elif info == "in running":
                        await asyncio.sleep(8)
                    elif info and 'validate' in info:
                        ret = info
                        break
            else:
                raise Exception("Captcha failed (retry exhausted)")
    except Exception as e:
        print(f"[极验] 远程过码失败: {e}")
        return None

    print("[极验] 远程过码成功")
    return ret
