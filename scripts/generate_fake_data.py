import sqlite3, pandas as pd, random, datetime as dt
from faker import Faker

fake = Faker()
rows = []
for _ in range(500):
    rows.append({
        "station_id": fake.bothify("ST###"),
        "date": fake.date_between("-30d", "today"),
        "aqi": random.randint(10, 200),
        "co2_ppm": round(random.uniform(350, 450), 1)
    })

df = pd.DataFrame(rows)
con = sqlite3.connect("data/env.db")
df.to_sql("air_quality", con, if_exists="replace", index=False)
con.close()
print("✅ synthetic database saved to data/env.db")
