-------------------------------
-- Table structure for Job
-- ----------------------------
CREATE TABLE HANGFIRE_JOB (
  ID number(11) NOT NULL, --AUTO_INCREMENT,
  STATEID number(11) DEFAULT NULL,
  STATENAME nvarchar2(20) DEFAULT NULL,
  INVOCATIONDATA nclob NOT NULL,
  ARGUMENTS nclob NOT NULL,
  CREATEDAT date NOT NULL,
  EXPIREAT date DEFAULT NULL,
  CONSTRAINT PK_HAHANGFIRE_JOB primary key (ID)
);
/
CREATE INDEX IX_HangFire_Job_StateName on HANGFIRE_JOB(STATENAME);
/
CREATE INDEX IX_HangFire_Job_ExpireAt ON HANGFIRE_JOB(EXPIREAT);
/
create sequence SQ_HANGFIRE_JOB;
/
create or replace trigger TR_HANGFIRE_JOB
before insert on HANGFIRE_JOB
for each row
begin
  select SQ_HANGFIRE_JOB.nextval into :new.ID from dual;
end;
/

-- ----------------------------
-- Table structure for Counter
-- ----------------------------
CREATE TABLE HANGFIRE_COUNTER (
  ID number(11) NOT NULL,
  KEY nvarchar2(100) NOT NULL,
  VALUE number(11) NOT NULL,
  EXPIREAT date DEFAULT NULL,
  CONSTRAINT PK_HANGFIRE_COUNTER PRIMARY KEY (ID)
);
/
CREATE INDEX IX_HANGFIRE_COUNTER_KEY on HANGFIRE_COUNTER(KEY);
/
create sequence SQ_HANGFIRE_COUNTER;
/
create or replace trigger TR_HANGFIRE_COUNTER
before insert on HANGFIRE_COUNTER
for each row
begin
  select SQ_HANGFIRE_COUNTER.nextval into :new.ID from dual;
end;
/
CREATE TABLE HANGFIRE_AGGREGATEDCOUNTER (
	ID number(11) NOT NULL, --AUTO_INCREMENT,
	KEY nvarchar2(100) NOT NULL,
	VALUE number(11) NOT NULL,
	EXPIREAT date DEFAULT NULL,
    CONSTRAINT PK_HANGFIRE_AGGREGATEDCOUNTER PRIMARY KEY (ID)
) ;
/
CREATE INDEX IX_HANGFIRE_AGGRCOUNTER_KEY on HANGFIRE_AGGREGATEDCOUNTER(KEY);
/
create sequence SQ_HANGFIRE_AGGREGATEDCOUNTER;
/
create or replace trigger TR_HANGFIRE_AGGREGATEDCOUNTER
before insert on HANGFIRE_AGGREGATEDCOUNTER
for each row
begin
  select SQ_HANGFIRE_AGGREGATEDCOUNTER.nextval into :new.ID from dual;
end;
/


-- ----------------------------
-- Table structure for DistributedLock
-- ----------------------------
CREATE TABLE "HANGFIRE_DISTRIBUTEDLOCK" (
  "RESOURCE" nvarchar2(100) NOT NULL,
  "CREATEDAT" date NOT NULL
);
/

-- ----------------------------
-- Table structure for Hash
-- ----------------------------
CREATE TABLE HANGFIRE_HASH (
  ID number(11) NOT NULL,
  KEY nvarchar2(100) NOT NULL,
  FIELD nvarchar2(40) NOT NULL,
  VALUE nclob,
  EXPIREAT date DEFAULT NULL,
  CONSTRAINT PK_HANGFIRE_HASH PRIMARY KEY (ID)
);
/
CREATE UNIQUE INDEX IX_Hash_Key_Field on HANGFIRE_HASH(KEY,FIELD);
/
create sequence SQ_HANGFIRE_HASH;
/
create or replace trigger TR_HANGFIRE_HASH
before insert on HANGFIRE_HASH
for each row
begin
  select SQ_HANGFIRE_HASH.nextval into :new.ID from dual;
end;
/


-- ----------------------------
-- TABLE STRUCTURE FOR JOBPARAMETER
-- ----------------------------
CREATE TABLE HANGFIRE_JOBPARAMETER (
  ID number(11) NOT NULL, --AUTO_INCREMENT,
  JOBID number(11) NOT NULL,
  NAME nvarchar2(40) NOT NULL,
  VALUE nclob,
  CONSTRAINT PK_HANGFIRE_JOBPARAMETER PRIMARY KEY (ID),
  CONSTRAINT FK_HANGFIRE_JOBPARAMETER_JOB FOREIGN KEY (JOBID) REFERENCES HANGFIRE_JOB (ID) --ON DELETE CASCADE ON UPDATE CASCADE
) ;
/
CREATE UNIQUE INDEX  IX_HANGFIRE_JOBPARAMETER1 on HANGFIRE_JOBPARAMETER(JOBID,NAME);
/
create sequence SQ_HANGFIRE_JOBPARAMETER;
/
create or replace trigger TR_HANGFIRE_JOBPARAMETER
before insert on HANGFIRE_JOBPARAMETER
for each row
begin
  select SQ_HANGFIRE_JOBPARAMETER.nextval into :new.ID from dual;
end;
/


-- ----------------------------
-- Table structure for JobQueue
-- ----------------------------
CREATE TABLE HANGFIRE_JOBQUEUE (
  ID number(11) NOT NULL, --AUTO_INCREMENT,
  JOBID number(11) NOT NULL,
  QUEUE nvarchar2(50) NOT NULL,
  FETCHEDAT DATE DEFAULT NULL,
  FETCHTOKEN nvarchar2(36) DEFAULT NULL,
  CONSTRAINT PK_HANGFIRE_JOBQUEUE PRIMARY KEY (ID)
) ;
/
CREATE INDEX  IX_HANGFIRE_JOBQUEUE1 on HANGFIRE_JOBQUEUE(QUEUE,FETCHEDAT);
/
create sequence SQ_HANGFIRE_JOBQUEUE;
/
create or replace trigger TR_HANGFIRE_JOBQUEUE
before insert on HANGFIRE_JOBQUEUE
for each row
begin
  select SQ_HANGFIRE_JOBQUEUE.nextval into :new.ID from dual;
end;
/


-- ----------------------------
-- TABLE STRUCTURE FOR JOBSTATE
-- ----------------------------
CREATE TABLE HANGFIRE_JOBSTATE (
  ID number(11) NOT NULL, --AUTO_INCREMENT,
  JOBID number(11) NOT NULL,
  NAME nvarchar2(20) NOT NULL,
  REASON nvarchar2(100) DEFAULT NULL,
  CREATEDAT date NOT NULL,
  DATA nclob,
  CONSTRAINT HANGFIRE_JOBSTATE PRIMARY KEY (ID),
  CONSTRAINT FK_HANGFIRE_JOBSTATE_JOB FOREIGN KEY (JOBID) REFERENCES HANGFIRE_JOB (ID) --ON DELETE CASCADE ON UPDATE CASCADE
);
/
create sequence SQ_HANGFIRE_JOBSTATE;
/
create or replace trigger TR_HANGFIRE_JOBSTATE
before insert on HANGFIRE_JOBSTATE
for each row
begin
  select SQ_HANGFIRE_JOBSTATE.nextval into :new.ID from dual;
end;
/


-- ----------------------------
-- TABLE STRUCTURE FOR SERVER
-- ----------------------------
CREATE TABLE HANGFIRE_SERVER (
  ID nvarchar2(100) NOT NULL,
  DATA nclob NOT NULL,
  LASTHEARTBEAT date DEFAULT NULL,
  CONSTRAINT PK_HANGFIRE_SERVER PRIMARY KEY (ID)
) ;
/


-- ----------------------------
-- TABLE STRUCTURE FOR SET
-- ----------------------------
CREATE TABLE HANGFIRE_SET (
  ID number(11) NOT NULL, --AUTO_INCREMENT,
  KEY nvarchar2(100) NOT NULL,
  VALUE nvarchar2(256) NOT NULL,
  SCORE float NOT NULL,
  EXPIREAT date DEFAULT NULL,
  CONSTRAINT PK_HANGFIRE_SET PRIMARY KEY (ID)
) ;
/
CREATE UNIQUE INDEX IX_HANGFIRE_SET_KEY_VALUE on HANGFIRE_SET(KEY,VALUE);
/
create sequence SQ_HANGFIRE_SET;
/
create or replace trigger TR_HANGFIRE_SET
before insert on HANGFIRE_SET
for each row
begin
  select SQ_HANGFIRE_SET.nextval into :new.ID from dual;
end;
/


-- ----------------------------
-- TABLE STRUCTURE FOR STATE
-- ----------------------------
CREATE TABLE HANGFIRE_STATE
(
	ID number(11) NOT NULL, --AUTO_INCREMENT,
	JOBID number(11) NOT NULL,
	NAME nvarchar2(20) NOT NULL,
	REASON nvarchar2(100) NULL,
	CREATEDAT date NOT NULL,
	DATA nclob NULL,
	CONSTRAINT PK_HANGFIRE_STATE PRIMARY KEY (ID),
	CONSTRAINT FK_HANGFIRE_STATE_JOB FOREIGN KEY (JOBID) REFERENCES HANGFIRE_JOB (ID) --ON DELETE CASCADE ON UPDATE CASCADE
);
/
create sequence SQ_HANGFIRE_STATE;
/
create or replace trigger TR_HANGFIRE_STATE
before insert on HANGFIRE_STATE
for each row
begin
  select SQ_HANGFIRE_STATE.nextval into :new.ID from dual;
end;
/


-- ----------------------------
-- TABLE STRUCTURE FOR LIST
-- ----------------------------
CREATE TABLE HANGFIRE_LIST
(
	ID number(11) NOT NULL, --AUTO_INCREMENT,
	KEY nvarchar2(100) NOT NULL,
	VALUE nclob NULL,
	EXPIREAT date NULL,
	PRIMARY KEY (ID)
);
/
create sequence SQ_HANGFIRE_LIST;
/
create or replace trigger TR_HANGFIRE_LIST
before insert on HANGFIRE_LIST
for each row
begin
  select SQ_HANGFIRE_LIST.nextval into :new.ID from dual;
end;
