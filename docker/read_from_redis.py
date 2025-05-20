import redis

# Connessione a Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=False)

# Scarica i bytes dal Redis
csv_bytes = r.get("query1.csv")

# Salvalo su disco
with open("downloaded.csv", "wb") as f:
    f.write(csv_bytes)

print("CSV riscaricato e salvato come downloaded.csv")

