import bcrypt

key = "testpw"
byteKey = key.encode("utf-8")
salt = bcrypt.gensalt()
hashedKey = bcrypt.hashpw(byteKey, salt)
print(hashedKey.decode("utf-8"))
