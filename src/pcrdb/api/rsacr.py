"""
RSA 加密 (移植自 autopcr/sdk/rsacr.py, 原样保留)

B站 SDK 登录时用服务端下发的 RSA 公钥加密 (hash + 密码)。
依赖 pycryptodome (pcrdb 已有)。
"""
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5 as Cipher_pkcs1_v1_5
import base64


# 加密
def rsacreate(message, public_key):
    rsakey = RSA.importKey(public_key)
    cipher = Cipher_pkcs1_v1_5.new(rsakey)     #创建用于执行pkcs1_v1_5加密或解密的密码
    cipher_text = base64.b64encode(cipher.encrypt(message.encode('utf-8')))
    text = cipher_text.decode('utf-8')
    return text
