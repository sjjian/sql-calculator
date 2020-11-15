# sql-calculator

## virtual db
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
