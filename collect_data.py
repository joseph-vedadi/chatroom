import sqlite3
import json
import os
from datetime import datetime
import time

db_name = "Master.db"
sql_transaction = []
start_row = 0
cleanup = 1000000
Data_dir = "../deep_data/data"
connection = sqlite3.connect("{}".format(db_name))
c = connection.cursor()


def get_files():
    return [
        os.path.join(Data_dir, filepath)
        for filepath in sorted(os.listdir(Data_dir))
        if filepath.startswith("RC")
    ]


def cleanup():
    print("Cleanin up!")
    sql = "DELETE FROM communication WHERE parent IS NULL"
    c.execute(sql)
    connection.commit()
    c.execute("VACUUM")
    connection.commit()
    print("Done Cleaning ..")

def create_table():
    c.execute(
        "CREATE TABLE IF NOT EXISTS communication (parent_id TEXT PRIMARY KEY, comment_id TEXT "
        "UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, created_utc INT, score INT)"
    )


def format_data(data):
    data = (
        data.replace("\n", " newlinechar ")
        .replace("\r", " newlinechar ")
        .replace('"', "'")
    )
    return data


def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        c.execute("BEGIN TRANSACTION")
        for s in sql_transaction:
            try:
                c.execute(s)
            except:
                pass
        connection.commit()
        sql_transaction = []


def sql_insert_replace_comment(
    comment_id, parent_id, parent, comment, subreddit, created_utc, score
):
    try:
        sql = """UPDATE communication SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, created_utc = ?, score = ? WHERE parent_id =?;""".format(
            parent_id,
            comment_id,
            parent,
            comment,
            subreddit,
            created_utc,
            score,
            parent_id,
        )
        transaction_bldr(sql)
    except Exception as e:
        print("s0 insertion", str(e))


def sql_insert_has_parent(
    comment_id, parent_id, parent, comment, subreddit, created_utc, score
):
    try:
        sql = """INSERT INTO communication (parent_id, comment_id, parent, comment, subreddit, created_utc, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(
            parent_id, comment_id, parent, comment, subreddit, created_utc, score
        )
        transaction_bldr(sql)
    except Exception as e:
        print("s0 insertion", str(e))


def sql_insert_no_parent(comment_id, parent_id, comment, subreddit, created_utc, score):
    try:
        sql = """INSERT INTO communication (parent_id, comment_id, comment, subreddit, created_utc, score) VALUES ("{}","{}","{}","{}",{},{});""".format(
            parent_id, comment_id, comment, subreddit, created_utc, score
        )
        transaction_bldr(sql)
    except Exception as e:
        print("s0 insertion", str(e))


def acceptable(data):
    if len(data.split(" ")) > 1000 or len(data) < 1:
        return False
    elif len(data) > 32000:
        return False
    elif data == "[deleted]":
        return False
    elif data == "[removed]":
        return False
    else:
        return True


def find_parent(pid):
    try:
        sql = "SELECT comment FROM communication WHERE comment_id = '{}' LIMIT 1".format(
            pid
        )
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        print(str(e))
        return False


def find_existing_score(pid):
    try:
        sql = "SELECT score FROM communication WHERE parent_id = '{}' LIMIT 1".format(
            pid
        )
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        # print(str(e))
        return False


def main():
    create_table()
    row_counter = 0
    paired_rows = 0
    for file_path in get_files():
        print("Proccessing {}".format(file_path))
        with open(file_path, buffering=10000000) as f:
            time_start = time.time()
            for row in f:
                row_counter += 1
                if row_counter > start_row:
                    try:
                        row = json.loads(row)

                        parent_id = row["parent_id"].split("_")[1]
                        body = format_data(row["body"])
                        created_utc = int(row["created_utc"])
                        score = int(row["score"])
                        comment_id = row["id"]
                        subreddit = row["subreddit"]
                        parent_data = find_parent(parent_id)
                        data = {
                            "comment_id": comment_id,
                            "parent_id": parent_id,
                            "parent": parent_data,
                            "comment": body,
                            "subreddit": subreddit,
                            "created_utc": created_utc,
                            "score": score,
                        }
                        existing_comment_score = find_existing_score(parent_id)
                        if existing_comment_score:
                            if score > existing_comment_score:
                                if acceptable(body):
                                    sql_insert_replace_comment(**data)

                        else:
                            if acceptable(body):
                                if parent_data:
                                    if score >= 2:
                                        sql_insert_has_parent(**data)
                                        paired_rows += 1
                                else:
                                    data.pop("parent")
                                    sql_insert_no_parent(**data)
                    except Exception as e:
                        print(str(e))

                if row_counter % 100000 == 0:

                    print(
                        "Total Rows Read: {}, Paired Rows: {}, Time: {}".format(
                            row_counter, paired_rows, time.time() - time_start
                        )
                    )
                    time_start = time.time()
        print("Deleting Log File")
        os.remove(file_path)

    print(
        "Total Rows Read: {}, Paired Rows: {}, Time: {}".format(
            row_counter, paired_rows, str(datetime.now())
        )
    )

    print("Cleaning Up")
    cleanup()
    print("Done")
cleanup()
