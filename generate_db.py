import os
import json
from datetime import datetime
import logging
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, TEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

POSTGRES_PORT = 5432
POSTGRES_PASSWORD = "postgres"
POSTGRES_USER = "postgres"
POSTGRES_DB = "postgres"
POSTGRES_HOST = "localhost"
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

engine_str = "postgresql://{user}:{password}@{hostname}:{port}/{dbname}".format(
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    hostname=POSTGRES_HOST,
    port=POSTGRES_PORT,
    dbname=POSTGRES_DB,
)

engine_str = "sqlite:///{}.db".format(int(datetime.now().timestamp()))

Base = declarative_base()


class Comment(Base):

    __tablename__ = "comments"
    parent_id = Column(String, primary_key=True, index=True)
    comment_id = Column(String, unique=True)
    parent = Column(TEXT)
    comment = Column(TEXT)
    subreddit = Column(TEXT)
    created_utc = Column(Integer)
    score = Column(Integer)


engine = create_engine(engine_str)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()


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
        query = session.query(Comment).filter_by(comment_id=pid).first()
        return False if query is None else query.comment
    except:
        return False


def find_existing_score(pid):
    try:
        query = session.query(Comment).filter_by(parent_id=pid).first()
        return False if query is None else query.score
    except:
        return False


if __name__ == "__main__":
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
                    session.commit()
session.commit()
# logging.info("Cleaning up!")
# session.query(Comment).filter(Comment.comment.isnot(None)).delete()
