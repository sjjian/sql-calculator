# sql-calculator
数据库差异对比，并生成对应的 DDL 语句。 SQL 解析基于 TiDB 的解析器
## 一、差异对比
### 1. 支持
* 表：增，删
* 字段： 增，删，改
 
## 二、virtual db
模拟数据库的 ddl 执行得到数据库结构
```go
vb := NewVirtualDB("")
vb.ExecSQL("create database db1")
vb.ExecSQL("use db1")
vb.ExecSQL("create table t1(id int)")
vb.ExecSQL("alter table db1.t1 add column name varchar(255);")

vb.Text()
/* 
output:
CREATE DATABASE `db1`;
CREATE TABLE `db1`.`t1` (`id` INT,`name` VARCHAR(255));
*/
```
