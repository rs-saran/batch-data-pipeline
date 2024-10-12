import random
import string
from datetime import datetime, timedelta

import pandas as pd

start_date = datetime(2023, 10, 1)  # Company launch date
end_date = datetime.now()


def get_random_date(start, end):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds()))
    )


def generate_conten_ids(num_ids):
    results = []

    for _ in range(num_ids):
        length = 6
        random_id = ''.join(
            random.choices(string.ascii_lowercase + string.digits, k=length)
        )
        random_date = get_random_date(start_date, end_date)
        results.append([random_id, random_date])

    return results


# Generate 3000 IDs
num_ids = 3000
content_ids = generate_conten_ids(num_ids)


df = pd.DataFrame(content_ids, columns=["content_id", "release_date"])
df.to_csv('data/content_ids.csv', index=False)
