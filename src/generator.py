import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from .config import Config

ACTION_MAP = {
    "first_visit": 1,
    "register": 2,
    "login": 3,
    "logout": 4,
    "create_topic": 5,
    "visit_topic": 6,
    "delete_topic": 7,
    "write_message": 8,
}


def generate_users(n, start_date, days):
    users = []
    for i in range(1, n + 1):
        users.append(
            {
                "id": i,
                "username": f"user_{i}",
                "created_at": start_date
                + timedelta(
                    days=random.randint(0, days - 1),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                ),
            }
        )
    return users


def generate_topics(users, start_date, days):
    topics = []
    for i in range(1, 201):
        author = random.choice(users)
        topics.append(
            {
                "id": i,
                "title": f"Topic {i}",
                "text": f"Content of topic {i}",
                "author": author["id"],
                "deleted": random.random() < 0.1,
                "created_at": start_date
                + timedelta(
                    days=random.randint(0, days - 1), hours=random.randint(0, 23)
                ),
            }
        )
    return topics


def generate_messages(topics, users, start_date, days):
    messages = []
    for i in range(1, 501):
        topic = random.choice(topics)

        author = random.choice(users) if random.random() < 0.5 else None
        messages.append(
            {
                "id": i,
                "text": f"Message {i}",
                "topic": topic["id"],
                "author": author["id"] if author else None,
                "created_at": start_date
                + timedelta(
                    days=random.randint(0, days - 1), hours=random.randint(0, 23)
                ),
            }
        )
    return messages


def generate_logs(users, topics, messages, start_date, days):
    logs = []
    actions = list(ACTION_MAP.keys())

    total_topics = 0
    for day in range(days):
        current = start_date + timedelta(days=day)

        for action in actions:
            n_actions = max(Config.MIN_ACTIONS_PER_TYPE, random.randint(0, 20))

            if action == "delete_topic":
                n_actions = min(n_actions, total_topics)

            for _ in range(n_actions):
                timestamp = current + timedelta(
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59),
                )

                if action == "create_topic" or action == "delete_topic":
                    if random.random() < 0.95:
                        user = random.choice(users)["id"]
                        success = True
                        if action == "create_topic":
                            total_topics += 1
                        else:
                            total_topics -= 1
                    else:
                        user = None
                        success = False
                    target_type = 2
                    target_id = random.choice(topics)["id"] if topics else None

                elif action == "write_message":
                    user = random.choice(users)["id"] if random.random() < 0.5 else None
                    success = True
                    target_type = 1
                    target_id = random.choice(messages)["id"] if messages else None

                elif action == "first_visit":
                    user = None
                    success = True
                    target_type = None
                    target_id = None

                else:
                    user = random.choice(users)["id"] if users else None
                    success = True
                    target_type = None
                    target_id = None

                logs.append(
                    {
                        "action": ACTION_MAP[action],
                        "user_id": user,
                        "timestamp": timestamp,
                        "target_type": target_type,
                        "target_id": target_id,
                        "success": success,
                    }
                )

    return logs


def save_csv(data, filename, fieldnames, output_dir):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    filepath = Path(output_dir) / filename

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            row_clean = {}
            for k, v in row.items():
                if isinstance(v, datetime):
                    row_clean[k] = v.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    row_clean[k] = v if v is not None else ""
            writer.writerow(row_clean)

    return filepath


def generate_all_data(start_date, days, n_users, output_dir):
    users = generate_users(n_users, start_date, days)
    topics = generate_topics(users, start_date, days)
    messages = generate_messages(topics, users, start_date, days)
    logs = generate_logs(users, topics, messages, start_date, days)

    save_csv(users, "users.csv", ["id", "username", "created_at"], output_dir)
    save_csv(
        topics,
        "topics.csv",
        ["id", "title", "text", "author", "deleted", "created_at"],
        output_dir,
    )
    save_csv(
        messages,
        "messages.csv",
        ["id", "text", "topic", "author", "created_at"],
        output_dir,
    )
    save_csv(
        logs,
        "logs.csv",
        ["action", "user_id", "timestamp", "target_type", "target_id", "success"],
        output_dir,
    )

    return {
        "users": len(users),
        "topics": len(topics),
        "messages": len(messages),
        "logs": len(logs),
    }
