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
    tribunal = Column(String(6))
    comarca_id = Column(Integer)
    filingyear = Column(Integer)
    classe = Column(String(255))
    def __repr__(self):
        return "Proc('%s')" % self.numero


class Parte(Base):
    __tablename__ = 'parte'
    parte_id = Column(Integer, primary_key=True)
    mov_id = Column(BigInteger, ForeignKey('mov.mov_id'))
    proc_id = Column(BigInteger, ForeignKey('proc.proc_id'))    
    parte = Column(String(255))
    tipo_parte_id = Column(Integer)
    def __repr__(self):
        return "Parte(%s,%s)" % (self.parte, self.number)


class Mov(Base):
    __tablename__ = 'mov'
    mov_id = Column(BigInteger, primary_key=True)
    proc_id = Column(BigInteger, ForeignKey('proc.proc_id'))
    classe = Column(String(255))    
    date = Column(Date)
    caderno_id = Column(Integer)
    line = Column(Integer)
    text = Column(MEDIUMTEXT)
    def __repr__(self):
        return "Parte(%s,%s)" % (self.number, self.date)        

    
class Col:
    def __init__(
        self,
        column,
        name,
        table
    ):
        self.name = name
        self.column = column
        self.table = table        

        
indices = [
    {
        'table': 'parte',
        'columns': ['parte'],
        'name': 'parte_fulltext',
        'index_type': 'FULLTEXT'
    },
    {
        'table': 'parte',
        'columns': ['parte'],
        'name': 'parte',
        'index_type': ''
    },
    {
        'table': 'mov',
        'columns': ['text'],
        'name': 'text_fulltext',
        'index_type': 'FULLTEXT'
    }
]
    

def insert_proc(
    database_name,
    proc_table=Proc,
    mov_table=Mov,
    parte_table=Parte,
    proc_files=['build/clean/proc.csv'],
    parte_files=['build/clean/parte.csv'],
    mov_files=['build/clean/mov.csv'],
    outdir='build/insert',
    indices=indices
):
    engine = db.get_db_engine(database_name)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    db.insert(
        database=database_name,
        table=proc_table,
        files=proc_files,
        outdir=outdir
    )    
    db.insert(
        database=database_name,
        table=parte_table,
        files=parte_files,
        outdir=outdir        
    )
    db.insert(
        database=database_name,
        table=mov_table,
        files=mov_files,
        outdir=outdir        
    )
    for index in indices:
        db.create_index(
            database=database_name,
            **index
        )

