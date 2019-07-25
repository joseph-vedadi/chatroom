from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, ForeignKey

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref
import os, json

POSTGRES_PORT = 5432
POSTGRES_PASSWORD = "postgres"
POSTGRES_USER = "postgres"
POSTGRES_DB = "chatroom"
POSTGRES_HOST = "localhost"
Data_dir = "./Text_Data/"

engine_str = "postgresql://{user}:{password}@{hostname}:{port}/{dbname}".format(
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    hostname=POSTGRES_HOST,
    port=POSTGRES_PORT,
    dbname=POSTGRES_DB,
)
engine = create_engine(engine_str)
Base = declarative_base()


class Comment(Base):

    __tablename__ = "comments"
    id = Column(String, primary_key=True, index=True)
    body = Column(String)
    subreddit = Column(String)
    retrieved_on = Column(Integer)
    score = Column(Integer)
    # This is what you need to add to make the database link it self
    parent_id = Column(String, ForeignKey("comments.id"))
    parent = relationship(
        "Comment", remote_side="Comment.id", backref=backref("children")
    )


Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()


def acceptable(score, data):
    if score < 1:
        return False
    if len(data.split(" ")) > 50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    elif data == "[deleted]":
        return False
    elif data == "[removed]":
        return False
    else:
        return True


def create_comment(data):
    global total, paired
    if not acceptable(data["score"], data["body"]):
        return
    new_data = {
        key: data[key]
        for key in ["id", "body", "subreddit", "retrieved_on", "score", "parent_id"]
    }
    new_data["parent_id"] = new_data["parent_id"].split("_")[0]
    if session.query(Comment).filter_by(id=new_data["parent_id"]).count() == 0:
        new_data["parent_id"] = None
    else:
        paired += 1
    new_data["body"] = format_body(new_data["body"])
    session.add(Comment(**new_data))
    total += 1


def format_body(body):
    body = body.replace(r"\n|\r", " NEWLINECHAR ")
    body = body.replace('"', "'")
    return body


total = 0
paired = 0
if __name__ == "__main__":
    session.query(Comment).delete()
    session.commit()
    for filepath in sorted(os.listdir(Data_dir)):
        if filepath.startswith("."):
            continue

        filepath = os.path.join(Data_dir, filepath)
        print("Proccessing : {}".format(filepath))
        with open(filepath, "r") as fp:
            line = fp.readline()
            while line:
                data = json.loads(line)
                create_comment(data)
                line = fp.readline()
        print("{}   {}:{} ".format(paired / total, total, filepath))
    session.commit()

