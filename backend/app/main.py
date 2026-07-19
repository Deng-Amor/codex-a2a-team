import os
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import String, create_engine, select
from uuid import uuid4
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

engine=create_engine(os.environ["DATABASE_URL"]);Local=sessionmaker(bind=engine)
class Base(DeclarativeBase):pass
class Agent(Base):
 __tablename__="agents";id:Mapped[int]=mapped_column(primary_key=True);key:Mapped[str]=mapped_column(String(80),unique=True);name:Mapped[str]=mapped_column(String(80));role:Mapped[str]=mapped_column(String(80));capabilities:Mapped[str]=mapped_column(String,default="")
class Stage(Base):
 __tablename__="workflow_stages";id:Mapped[int]=mapped_column(primary_key=True);key:Mapped[str]=mapped_column(String(80),unique=True);agent_key:Mapped[str]=mapped_column(String(80));depends_on:Mapped[str]=mapped_column(String,default="")
class Workflow(Base):
 __tablename__="workflows";id:Mapped[str]=mapped_column(String(40),primary_key=True);title:Mapped[str]=mapped_column(String(200));request:Mapped[str]=mapped_column(String);status:Mapped[str]=mapped_column(String(30),default="running")
class Task(Base):
 __tablename__="workflow_tasks";id:Mapped[str]=mapped_column(String(80),primary_key=True);workflow_id:Mapped[str]=mapped_column(String(40));stage_key:Mapped[str]=mapped_column(String(80));agent_key:Mapped[str]=mapped_column(String(80));status:Mapped[str]=mapped_column(String(30));depends_on:Mapped[str]=mapped_column(String,default="")
app=FastAPI(title="A2A Control Plane")
app.add_middleware(CORSMiddleware,allow_origins=["http://127.0.0.1:5173","http://localhost:5173"],allow_methods=["*"],allow_headers=["*"])
def db():
 with Local() as s:yield s
@app.on_event("startup")
def boot():
 Base.metadata.create_all(engine)
 with Local() as s:
  if not s.scalar(select(Agent.id)):
   s.add_all([Agent(key="task-decomposer",name="任务拆分",role="planner"),Agent(key="architecture-agent",name="架构设计",role="architect")]);s.commit()
  if not s.scalar(select(Stage.id)):
   s.add_all([Stage(key="decompose",agent_key="task-decomposer"),Stage(key="architecture",agent_key="architecture-agent",depends_on="decompose")]);s.commit()
@app.get('/api/agents')
def agents(s:Session=Depends(db)):return [{"key":a.key,"name":a.name,"role":a.role,"capabilities":a.capabilities} for a in s.scalars(select(Agent))]
@app.post('/api/agents')
def add(agent:dict,s:Session=Depends(db)):
 item=Agent(**agent);s.add(item);s.commit();s.refresh(item);return {"key":item.key,"name":item.name,"role":item.role}
@app.get('/api/workflow-stages')
def stages(s:Session=Depends(db)):return [{"key":x.key,"agent_key":x.agent_key,"depends_on":x.depends_on.split(',') if x.depends_on else []} for x in s.scalars(select(Stage))]
@app.post('/api/workflow-stages')
def add_stage(stage:dict,s:Session=Depends(db)):
 item=Stage(key=stage['key'],agent_key=stage['agent_key'],depends_on=','.join(stage.get('depends_on',[])));s.add(item);s.commit();return {"key":item.key}
@app.post('/api/workflows')
def create_workflow(payload:dict,s:Session=Depends(db)):
 wid='wf_'+uuid4().hex[:8];workflow=Workflow(id=wid,title=payload['title'],request=payload['request']);s.add(workflow)
 for stage in s.scalars(select(Stage)):
  s.add(Task(id=f'{wid}_{stage.key}',workflow_id=wid,stage_key=stage.key,agent_key=stage.agent_key,status='ready' if not stage.depends_on else 'blocked',depends_on=stage.depends_on))
 s.commit();return {"id":wid,"status":"running"}
@app.get('/api/workflows')
def workflows(s:Session=Depends(db)):return [{"id":x.id,"title":x.title,"status":x.status} for x in s.scalars(select(Workflow))]
@app.get('/api/workflows/{workflow_id}')
def get_workflow(workflow_id:str,s:Session=Depends(db)):
 wf=s.get(Workflow,workflow_id);return {"id":wf.id,"title":wf.title,"status":wf.status,"tasks":[{"id":t.id,"stage":t.stage_key,"agent":t.agent_key,"status":t.status,"depends_on":t.depends_on.split(',') if t.depends_on else []} for t in s.scalars(select(Task).where(Task.workflow_id==workflow_id))]}
