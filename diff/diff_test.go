package diff

import (
	"fmt"
	"sql-calculator/utils"
	"testing"
)


func TestGetDiffFromSqlFile(t *testing.T) {
	sourceTable := `
CREATE TABLE t1(
id int,
name varchar(20),
code int
);
CREATE TABLE t2(
id int
);
`

	targetTable := `
CREATE TABLE t1(
id int,
name varchar(30),
age int
);
CREATE TABLE t3(
id int
);
`

	alters, err := GetDiffFromSqlFile("db1", sourceTable, targetTable)
	if err != nil {
		t.Error(err)
		return
	}
	for _, alter := range alters {
		sql, _ := utils.RestoreToSql(alter)
		fmt.Println(sql)
	}
}
