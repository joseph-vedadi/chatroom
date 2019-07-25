import sqlite3
import json
from datetime import datetime
import time

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, ForeignKey, TEXT

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref
import os, json

POSTGRES_PORT = 5432
POSTGRES_PASSWORD = "postgres"
POSTGRES_USER = "postgres"
POSTGRES_DB = "postgres"
POSTGRES_HOST = "localhost"
Data_dir = "./Text_Data/"
MIN_SCORE = 0
Max_Words = 50
Data_Len = 1000


engine_str = "postgresql://{user}:{password}@{hostname}:{port}/{dbname}".format(
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    hostname=POSTGRES_HOST,
    port=POSTGRES_PORT,
    dbname=POSTGRES_DB,
)


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
    query = session.query(Comment).filter_by(parent_id=args["parent_id"])
    query.update(args)


def sql_insert(args):
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
    query = session.query(Comment).filter_by(comment_id=pid).first()
    return False if query is None else query.comment


def find_existing_score(pid):
    query = session.query(Comment).filter_by(parent_id=pid).first()
    return False if query is None else query.score


if __name__ == "__main__":
    row_counter = 0
    paired_rows = 0
    for filepath in sorted(os.listdir(Data_dir)):
        if filepath.startswith("."):
            continue
        filepath = os.path.join(Data_dir, filepath)
        print(
            "Proccessing : {} row_counter: {} paired_rows:{}".format(
                filepath, row_counter, paired_rows
            )
        )
        with open(filepath, buffering=1000) as f:
            for row in f:
                row_counter += 1
                try:
                    row = json.loads(row)
                    parent_id = row["parent_id"]
                    body = format_data(row["body"])
                    created_utc = row["created_utc"]
                    score = row["score"]
                    parent_data = find_parent(parent_id)
                    args = {
                        "comment_id": row["link_id"],
                        "parent_id": parent_id,
                        "score": score,
                        "subreddit": row["subreddit"],
                        "parent": None if not parent_data else parent_data,
                        "comment": body,
                        "created_utc": row["created_utc"],
                    }
                    if not score > MIN_SCORE:
                        if acceptable(body):
                            existing_comment_score = find_existing_score(parent_id)
                            if existing_comment_score:
                                if score > existing_comment_score:
                                    sql_replace_comment(args)
                            else:
                                sql_insert(args)
                                if parent_data:
                                    paired_rows += 1
                except Exception as e:
                    print(str(e))
                    exit()


# print("Cleaning up!")
# session.query(Comment).filter(Comment.comment.isnot(None)).delete()


session.commit()
