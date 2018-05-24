from binascii import unhexlify, hexlify
import base64

def Base64ToHexHash(base64_hash):
    return hexlify(base64.decodestring(base64_hash.strip('\n"\'')))

#st = Base64ToHexHash('YXg/2Z/PYhMRgRXqxJBjpg==')
