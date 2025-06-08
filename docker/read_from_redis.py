import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=False)

csv_bytes = r.get("query1.csv")

with open("downloaded.csv", "wb") as f:
    f.write(csv_bytes)

print("CSV riscaricato e salvato come downloaded.csv")

