import base64
import hashlib
import struct

from Crypto.Cipher import AES

__all__ = ["decrypt_oklink_response"]


def cryptojs_sha1_wordarray_from_string(s: str):
    """SHA1(string) → CryptoJS WordArray (words, sigBytes)"""
    h = hashlib.sha1(s.encode()).digest()  # 20 bytes
    words = []
    for i in range(0, 20, 4):
        w = struct.unpack(">I", h[i : i + 4])[0]
        if w & 0x80000000:
            w -= 0x100000000  # signed int
        words.append(w)
    return words, 20


def cryptojs_sha1_from_wordarray(words, sig_bytes):
    """SHA1(CryptoJS WordArray) → hex digest"""
    b = bytearray()
    for w in words:
        w_u = w & 0xFFFFFFFF
        b.extend([(w_u >> 24) & 0xFF, (w_u >> 16) & 0xFF, (w_u >> 8) & 0xFF, w_u & 0xFF])
    b = b[:sig_bytes]
    return hashlib.sha1(b).hexdigest()


def wordarray_to_bytes(words, sig_bytes):
    """WordArray → Python bytes"""
    out = bytearray()
    for w in words:
        out.extend([(w >> 24) & 0xFF, (w >> 16) & 0xFF, (w >> 8) & 0xFF, w & 0xFF])
    return bytes(out[:sig_bytes])


def derive_aes_key(ts: str) -> bytes:
    """Derive AES-128 key from timestamp (100% same as CryptoJS)"""
    words1, sig1 = cryptojs_sha1_wordarray_from_string(ts)
    sha_hex = cryptojs_sha1_from_wordarray(words1, sig1)
    key_hex = sha_hex[:32]  # AES-128
    key_bytes = bytes.fromhex(key_hex)
    return key_bytes


def decrypt_one_cipher(cipher_b64, key_bytes):
    """解密单个 base64 block"""

    try:
        raw = base64.b64decode(cipher_b64)
    except:
        return cipher_b64  # 非 base64 则直接返回

    # CryptoJS 会把不足 16 字节的 block 用 0 填满
    block = raw + b"\x00" * (16 - len(raw))

    cipher = AES.new(key_bytes, AES.MODE_ECB)
    dec = cipher.decrypt(block)

    # PKCS7 padding
    pad = dec[-1]
    if 1 <= pad <= 16:
        dec = dec[:-pad]

    try:
        return dec.decode()
    except:
        return dec


def decrypt_tag(tag: str, key_bytes):
    """自动解密 'aaa==. bbb==' 或单段 'aaa=='"""

    if not isinstance(tag, str):
        return tag

    if "=" not in tag:
        return tag

    parts = [p.strip() for p in tag.split(".")]
    decrypted = [decrypt_one_cipher(p, key_bytes) for p in parts]

    return ": ".join(str(x) for x in decrypted)


TAG_FIELDS = {"entityTag", "hoverEntityTag", "tokenTag", "propertyTag"}  # 识别字段
LIST_FIELDS = {"entityTags", "propertyTags"}  # 列表字段


def decrypt_recursive(obj, key_bytes):
    """递归解密 JSON 结构"""

    # dict
    if isinstance(obj, dict):
        new = {}
        for k, v in obj.items():
            # 单字段需要解密
            if k in TAG_FIELDS:
                new[k] = decrypt_tag(v, key_bytes)

            # 列表字段（多个 base64）
            elif k in LIST_FIELDS and isinstance(v, list):
                new[k] = [decrypt_tag(x, key_bytes) for x in v]

            else:
                new[k] = decrypt_recursive(v, key_bytes)

        return new

    # list
    if isinstance(obj, list):
        return [decrypt_recursive(x, key_bytes) for x in obj]

    # other
    return obj


def decrypt_oklink_response(json_obj, ts):
    key_bytes = derive_aes_key(str(ts))
    return decrypt_recursive(json_obj, key_bytes)
