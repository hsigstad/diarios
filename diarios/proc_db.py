from sqlalchemy import Column, Integer, String, BigInteger, Date
from sqlalchemy import Float, Text, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy import Table, MetaData
import diarios.database as db

Base = declarative_base()    


class Proc(Base):
    __tablename__ = 'proc'
    proc_id = Column(BigInteger, primary_key=True)
    number = Column(String(50))
    tribunal_id = Column(Integer)
    comarca_id = Column(Integer)
    filingyear = Column(Integer)
    classe = Column(String(255))
    def __repr__(self):
        return "Proc('%s')" % self.numero


class Parte(Base):
    __tablename__ = 'parte'
    parte_id = Column(Integer, primary_key=True)
    proc_id = Column(BigInteger, ForeignKey('proc.proc_id'))
    parte = Column(String(255))
    tipo_parte_id = Column(Integer)
    def __repr__(self):
        return "Parte(%s,%s)" % (self.parte, self.number)


class Mov(Base):
    __tablename__ = 'mov'
    mov_id = Column(Integer, primary_key=True)
    proc_id = Column(BigInteger, ForeignKey('proc.proc_id'))    
    date = Column(Date)
    caderno_id = Column(Integer)
    line = Column(Integer)
    text = Column(MEDIUMTEXT)
    def __repr__(self):
        return "Parte(%s,%s)" % (self.number, self.date)        


def insert_proc(
    database_name,
    columns=[],
    proc_files=['build/clean/proc.csv'],
    parte_files=['build/clean/parte.csv'],
    mov_files=['build/clean/mov.csv']
):
    for c in columns:
        if c.table == 'proc':
            col = c.column
            col.name = c.name
            Proc.__table__.append_column(col)
        if c.table == 'mov':
            col = c.column
            col.name = c.name
            Mov.__table__.append_column(col)
    engine = db.get_db_engine(database_name)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    db.insert(
        database=database_name,
        table=Proc,
        files=proc_files
    )    
    db.insert(
        database=database_name,
        table=Parte,
        files=parte_files
    )
    db.create_index(
        database=database_name,
        table='parte',
        columns=['parte'],
        name='parte_fulltext',
        index_type='FULLTEXT'
    )
    db.create_index(
        database=database_name,
        table='parte',
        columns=['parte'],
        name='parte'
    )
    db.insert(
        database=database_name,
        table=Mov,
        files=mov_files
    )
    db.create_index(
        database=database_name,
        table='mov',
        columns=['text'],
        name='text_fulltext',
        index_type='FULLTEXT'
    )

