import os
from fastapi import FastAPI, Depends
from sqlalchemy import String, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

engine=create_engine(os.environ["DATABASE_URL"]);Local=sessionmaker(bind=engine)
class Base(DeclarativeBase):pass
class Agent(Base):
 __tablename__="agents";id:Mapped[int]=mapped_column(primary_key=True);key:Mapped[str]=mapped_column(String(80),unique=True);name:Mapped[str]=mapped_column(String(80));role:Mapped[str]=mapped_column(String(80));capabilities:Mapped[str]=mapped_column(String,default="")
app=FastAPI(title="A2A Control Plane")
def db():
 with Local() as s:yield s
@app.on_event("startup")
def boot():
 Base.metadata.create_all(engine)
 with Local() as s:
  if not s.scalar(select(Agent.id)):
   s.add_all([Agent(key="task-decomposer",name="任务拆分",role="planner"),Agent(key="architecture-agent",name="架构设计",role="architect")]);s.commit()
@app.get('/api/agents')
def agents(s:Session=Depends(db)):return [{"key":a.key,"name":a.name,"role":a.role,"capabilities":a.capabilities} for a in s.scalars(select(Agent))]
@app.post('/api/agents')
def add(agent:dict,s:Session=Depends(db)):
 item=Agent(**agent);s.add(item);s.commit();s.refresh(item);return {"key":item.key,"name":item.name,"role":item.role}
