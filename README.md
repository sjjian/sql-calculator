# sql-calculator
这是一个基于 TiDB MySQL 语法解析器的一个工具集，目前提供以下功能
1. SQL 指纹
1. 数据库库表对比： 对比两个数据库的库表差异，并生成源库到目标库对应的差异（ DDL） 语句 
## 编译
```
git clone https://github.com/sjjian/sql-calculator
cd sql-calculator
go build -mod vendor
```
## SQL 指纹
指定 SQL 生成 SQL 指纹，支持子查询
### 1. 使用方式
```bash
>> ./sql-calculator fp --help
SQL Fingerprint - Replace all expression value of the SQL with ?

Usage:
   fp [SQL content](string) [flags]

Examples:
   ./sql-calculator fp "update tb1 set a = "2" where a = "3" and b = 4

Output:
   UPDATE `tb1` SET `a`=? WHERE `a`=? AND `b`=?

Flags:
  -h, --help   help for fp

```
## 数据库库表对比
### 1. 支持
* 表：增，删
* 字段： 增，删，改
 
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
