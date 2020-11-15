package virtualdb

import (
	"fmt"
	"github.com/pingcap/parser"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/stretchr/testify/assert"
	"testing"
)

func testExec(t *testing.T, input string, expect string) {
	p := parser.New()
	stmts, _, err := p.Parse(input, "", "")
	if err != nil {
		t.Error(err)
	}
	vb := NewVirtualDB("")
	for _, stmt := range stmts {
		err := vb.Exec(stmt)
		if err != nil {
			t.Error(err)
			return
		}
	}
	actual, err := vb.Text()
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, expect, actual)
	fmt.Println(actual)
}

func TestExecAddColumn(t *testing.T) {
	testExec(t, `create database db1;
use db1;
create table t1(id int);
alter table db1.t1 add column name varchar(255);
`,
		"CREATE DATABASE `db1`;\n"+
			"CREATE TABLE `db1`.`t1` (`id` INT,`name` VARCHAR(255));\n",
	)
}
