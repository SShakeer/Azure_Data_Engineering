# Databricks notebook source
emp=[
    (7839,"KING", "PRESIDENT",1234,"1981-11-10",5000,100,10),
    (7698,"BLAKE", "MANAGER",7839,"1981-05-01",2850,500,30),
    (7782,"CLARK", "MANAGER",7839, "1981-06-09", 2450,7890 ,10),
    (7566, "JONES", "MANAGER",7839, "1981-04-01", 2975,987,20),
    (7788, "SCOTT",  "ANALYST",7566, "1990-04-07",3000,786,20),
    (7902, "FORD", "ANALYST",7566, "1981-03-12", 3000,765,20),
    (7369,"SMITH",  "CLERK",7902, "1980-12-01", 800,40,20),
    (7499, "ALLEN",  "SALESMAN", 7698, "1981-02-20",1600,300, 30),
    (7521, "WARD" ,  "SALESMAN", 7698, "1981-02-03",    1250,     500,    30),
    (7654, "MARTIN",    "SALESMAN",    7698, "1981-09-28", 1250 ,   1400,    30),
    (7844, "TURNER",    "SALESMAN",    7698, "1981-09-08",    1500,       0,    30),
    (7876, "ADAMS",    "CLERK",       7788, "1987-09-09",    1100,90,            20),
    (7900,"JAMES",     "CLERK",       7698, "1981-12-03",     950,90,            30),
    (7934, "MILLER",    "CLERK",       7782, "1982-01-01" ,   1300 ,80,           10)
]

# COMMAND ----------

emp_df = spark.createDataFrame(emp, schema="empno INTEGER, ename STRING,job STRING,mgr INTEGER,hiredate STRING,sal INTEGER,comm INTEGER,deptno INTEGER")

# COMMAND ----------

dept=[
    (10, "ACCOUNTING",     "NEW YORK"),
    (20 ,"RESEARCH",       "DALLAS"),
    (30, "SALES",          "CHICAGO"),
    (40, "OPERATIONS",     "BOSTON") 
]

# COMMAND ----------

dept_df=spark.createDataFrame(dept, schema="deptno INTEGER, dname STRING,loc STRING")

# COMMAND ----------

dept_df

# COMMAND ----------


