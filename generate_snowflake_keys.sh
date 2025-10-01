mkdir -p ~/.snowflake

# private key
openssl genrsa -out ~/.snowflake/rsa_key_raw.pem 2048
openssl pkcs8 -topk8 -inform PEM -outform PEM -nocrypt -in ~/.snowflake/rsa_key_raw.pem -out ~/.snowflake/rsa_key.pem
rm ~/.snowflake/rsa_key_raw.pem

# public key  
openssl rsa -in ~/.snowflake/rsa_key.pem -pubout -out ~/.snowflake/rsa_key.pub

echo "Public key for Snowflake:"
echo "========================="
grep -v "BEGIN PUBLIC KEY" ~/.snowflake/rsa_key.pub | grep -v "END PUBLIC KEY" | tr -d '\n'
echo ""
echo "========================="
echo ""
echo "Run in Snowflake:"
echo "ALTER USER your_username SET RSA_PUBLIC_KEY='PASTE_KEY_ABOVE';"
