import os
import random
# from multiprocessing import Pool
import time
import uuid
from datetime import datetime, timedelta

import pandas as pd

# Start the timer
start_time = time.time()


# Load content CSV with content_id and release_date
content_df = pd.read_csv("data/content_ids.csv")
content_df['release_date'] = pd.to_datetime(content_df['release_date'])


# Helper function to generate random timestamp within the last year
def random_date(start, end):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds()))
    )


def save_events_to_parquet(events, base_path="data/clickstream_data"):
    for event in events:
        event_name = event['event_name']
        # event_date = datetime.fromtimestamp(event['eventtimestamp'])
        event_date = event['eventtimestamp']
        year = event_date.year
        month = event_date.month
        day = event_date.day

        # Create directory path if not exists
        dir_path = f"{base_path}/{event_name}/{year}/{month}/{day}"
        os.makedirs(dir_path, exist_ok=True)

        # Convert the event into a DataFrame for saving
        df = pd.DataFrame([event])

        # Save as parquet
        file_path = f"{dir_path}/events.parquet"
        if os.path.exists(file_path):
            df.to_parquet(file_path, engine='fastparquet', append=True)
        else:
            df.to_parquet(file_path, engine='fastparquet')


def generate_events_for_one_user(user_id, n):
    start_time2 = time.time()
    session_start_events = []
    session_end_events = []
    consumption_start_events = []
    consumption_end_events = []
    total_seconds_in_a_day = 86400
    start_date = datetime(2023, 10, 1)  # Company launch date
    end_date = datetime.now()

    user_total_days = random.randint(
        0, 300
    )  # Total days the user has activity
    user_activity_days = sorted(
        [random_date(start_date, end_date) for _ in range(user_total_days)]
    )

    for day in user_activity_days:
        day_total_consumption = 0  # Reset daily consumption total
        # session_count = random.randint(1, 8)  # Random number of sessions for the day
        daily_consumption_efficiency = random.uniform(0, 1)

        next_session_duration = random.randint(10, 11800)
        day_total_consumption += next_session_duration

        while (
            day_total_consumption
            < daily_consumption_efficiency * total_seconds_in_a_day
        ):

            session_id = str(uuid.uuid4())

            # Session start and end timestamps
            session_start = random_date(day, day + timedelta(days=1))
            # session_duration = random.randint(10, 11800)  # Session duration between 10 secs to 6 hours

            # session_end should be after session_start by session_duration
            session_end = session_start + timedelta(
                seconds=next_session_duration
            )
            adid = str(uuid.uuid4())

            session_start_events.append(
                {
                    "event_name": "session_start",
                    "eventtimestamp": session_start,
                    "userid": user_id,
                    "platform": random.choice(['app', 'web']),
                    "adid": adid,
                    "sessionid": session_id,
                    "appversion": random.choice(['1.0', '1.1', '1.2']),
                    # "event_specific_properties": {}  # No specific properties for session_start
                }
            )

            session_end_events.append(
                {
                    "event_name": "session_end",
                    "eventtimestamp": session_end,
                    "userid": user_id,
                    "platform": random.choice(['app', 'web']),
                    "adid": adid,
                    "sessionid": session_id,
                    "appversion": random.choice(['1.0', '1.1', '1.2']),
                    "event_specific_properties": {
                        "session_duration": next_session_duration
                    },  # No specific properties for session_end
                }
            )

            consumption_efficieny = random.uniform(0, 1)
            session_total_consumption = 0
            next_consumption_duration = random.randint(
                0, 7200
            )  # first consumption duration
            session_total_consumption += next_consumption_duration

            # Generate consumption events as long as daily consumption doesn't exceed 86400 seconds
            while (
                session_total_consumption
                < next_session_duration * consumption_efficieny
            ):

                # Pick a content that was launched before or on the session day
                valid_content = content_df[
                    content_df['release_date'] <= session_start
                ]
                if valid_content.empty:
                    break

                content_row = valid_content.sample(1).iloc[0]
                content_id = content_row['content_id']
                consumption_start = random_date(session_start, session_end)

                consumption_end = consumption_start + timedelta(
                    seconds=next_consumption_duration
                )
                consumption_rate = random.uniform(
                    0, 1
                )  # Random rate of completion

                # Record the consumption events
                consumption_start_events.append(
                    {
                        "event_name": "consumption_start",
                        "eventtimestamp": consumption_start,
                        "userid": user_id,
                        "platform": random.choice(['app', 'web']),
                        "sessionid": session_id,
                        "appversion": random.choice(['1.0', '1.1', '1.2']),
                        "event_specific_properties": {
                            "content_id": content_id,
                            "consumption_type": random.choice(
                                ['online', 'downloaded']
                            ),
                            "is_continued": random.choice([True, False]),
                        },
                    }
                )

                consumption_end_events.append(
                    {
                        "event_name": "consumption_end",
                        "eventtimestamp": consumption_end,
                        "userid": user_id,
                        "platform": random.choice(['app', 'web']),
                        "sessionid": session_id,
                        "appversion": random.choice(['1.0', '1.1', '1.2']),
                        "event_specific_properties": {
                            "content_id": content_id,
                            "consumption_type": random.choice(
                                ['online', 'downloaded']
                            ),
                            "is_continued": random.choice([True, False]),
                            "consumption_rate": consumption_rate,
                            "consumption_duration": next_consumption_duration,
                        },
                    }
                )

                next_consumption_duration = random.randint(0, 7200)
                session_total_consumption += next_consumption_duration
            next_session_duration = random.randint(10, 11800)
            day_total_consumption += next_session_duration

    save_events_to_parquet(session_start_events)
    save_events_to_parquet(session_end_events)
    save_events_to_parquet(consumption_start_events)
    save_events_to_parquet(consumption_end_events)

    end_time2 = time.time()
    elapsed_time = end_time2 - start_time2
    print()
    print(
        f"Completed generating data for {n}th user with {len(user_activity_days)} days in {elapsed_time:.4f} seconds"
    )


# Generate clickstream data
def generate_clickstream_events(num_users):
    uid_list = []
    # Loop for the number of users
    print(time)
    for n in range(num_users):
        user_id = str(uuid.uuid4())
        # uid_list.append(user_id)
        generate_events_for_one_user(user_id, n)


# Parameters
if __name__ == '__main__':
    num_users = 10000
    generate_clickstream_events(num_users)
