package virtualdb

import (
	"fmt"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/stretchr/testify/assert"
	"testing"
)

func testExec(t *testing.T, input string, expect string, expectError string) {
	vb := NewVirtualDB("")
	err := vb.ExecSQL(input)
	if expectError != "" {
		assert.EqualError(t, err, expectError)
		return
	}
	if err != nil {
		t.Error(err)
	}
	actual, err := vb.Text()
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, expect, actual)
	fmt.Println(actual)
}

func TestExecAddColumnSuccess(t *testing.T) {
	testExec(t, `create database db1;
use db1;
create table t1(id int);
alter table db1.t1 add column name varchar(255);
alter table t1 add index idx_id(id);
`,
		"CREATE DATABASE `db1`;\n"+
			"CREATE TABLE `db1`.`t1` (`id` INT,`name` VARCHAR(255));\n",
		"",
	)
}

func TestExecAddColumnDuplicate(t *testing.T) {
	testExec(t, `create database db1;
use db1;
create table t1(id int);
alter table db1.t1 add column name varchar(255);
alter table db1.t1 add column name varchar(255);
`,
		"",
		"duplicate column name in db1.t1",
	)
}
