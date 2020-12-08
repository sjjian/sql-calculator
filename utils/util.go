package utils

import (
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"strings"
)

func RestoreToSql(stmt ast.Node) (string, error) {
	return RestoreToSqlWithFlag(format.DefaultRestoreFlags, stmt)
}

func RestoreToSqlWithFlag(flag format.RestoreFlags, stmt ast.Node) (string, error) {
	var sb strings.Builder
	ctx := format.NewRestoreCtx(flag, &sb)
	err := stmt.Restore(ctx)
	if err != nil {
		return "", err
	}
	return sb.String(), nil
}

func ParseOneSql(sql string) (ast.StmtNode, error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, err
	}
	return stmt, nil
}
