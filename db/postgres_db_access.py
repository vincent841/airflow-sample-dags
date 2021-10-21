from sqlalchemy import MetaData, Table, Column, String, Integer
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as db
from db.db_config import DagConfiguration


class RdsDatabase:
    __engine = db.create_engine(
        DagConfiguration.POSTGRES_DB_CONNECTION,
        echo=True,
    )

    def __init__(self):
        self.connection = self.__engine.connect()
        print("DB Instance created")

    @property
    def engine(self):
        return self.__engine

    @engine.setter
    def data(self, data):
        self.__engine = data


Base = declarative_base()


class TestData(Base):
    __tablename__ = "testdata"
    id = Column(Integer, primary_key=True)
    date = Column(Integer)
    name = Column(String(256))
