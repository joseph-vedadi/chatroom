import os
import json
from datetime import datetime
import logging
import sqlite3

Data_dir = "../deep_data/data"
MIN_SCORE = 0
Max_Words = 100
Data_Len = 10000

row_counter = 0
paired_rows = 0
Insert = 0
Update = 0
LowScore = 0
Bad_Text = 0
logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
DateTimeInt = int(datetime.now().timestamp())
connection = sqlite3.connect("{}.db".format(DateTimeInt))
c = connection.cursor()


def create_table():
    c.execute(
        "CREATE TABLE IF NOT EXISTS communication(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)"
    )


def format_data(data):
    data = (
        data.replace("\r\n", " newlinechar ")
        .replace("\n", " newlinechar ")
        .replace("\r", " newlinechar ")
        .replace('"', "'")
    )
    return data


def sql_replace_comment(args):
    global Update
    Update += 1
    query = session.query(Comment).filter_by(parent_id=args["parent_id"])
    query.update(args)


def sql_insert(args):
    global Insert
    Insert += 1
    comment = Comment(**args)
    session.add(comment)


def acceptable(data):
    if len(data.split(" ")) > Max_Words or len(data) < 1:
        return False
    elif len(data) > Data_Len:
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
    except Exception:
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


if __name__ == "__main__":
    create_table()
    for filepath in sorted(os.listdir(Data_dir)):
        if (
            filepath.startswith(".")
            or filepath.endswith(".bz2")
            or not filepath.startswith("RC")
            or filepath != "RC_2018-07"
        ):
            continue
        filepath = os.path.join(Data_dir, filepath)
        logging.info("Proccessing : {} ".format(filepath))
        with open(filepath, buffering=100000) as f:
            for row in f:
                row_counter += 1
                try:
                    row = json.loads(row)
                    parent_id = row["parent_id"].split("_")[1]
                    comment_id = row["id"]
                    body = format_data(row["body"])
                    created_utc = row["created_utc"]
                    score = row["score"]
                    parent_data = find_parent(parent_id)

                    args = {
                        "comment_id": comment_id,
                        "parent_id": parent_id,
                        "score": score,
                        "subreddit": row["subreddit"],
                        "parent": None if not parent_data else parent_data,
                        "comment": body,
                        "created_utc": row["created_utc"],
                    }
                    if score < MIN_SCORE:
                        LowScore += 1
                    else:
                        if not acceptable(body):
                            Bad_Text += 1
                        else:
                            existing_comment_score = find_existing_score(parent_id)
                            if existing_comment_score:
                                if score > existing_comment_score:
                                    sql_replace_comment(args)
                            else:
                                sql_insert(args)
                                if parent_data:
                                    paired_rows += 1
                except Exception as e:
                    logging.info(str(e))
                    pass
                if row_counter % 1000 == 0:
                    logging.info(
                        "row_counter: {} paired_rows:{} Update: {} Insert: {} LowScore:{}  Bad_Text:{}".format(
                            row_counter, paired_rows, Update, Insert, LowScore, Bad_Text
                        )
                    )
                    session.flush()
session.commit()
# logging.info("Cleaning up!")
# session.query(Comment).filter(Comment.comment.isnot(None)).delete()
