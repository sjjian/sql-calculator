package virtualdb

import (
	"fmt"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"strings"
)

func restoreToSql(stmt ast.StmtNode) (string, error) {
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	err := stmt.Restore(ctx)
	if err != nil {
		return "", err
	}
	return sb.String(), nil
}

// parseCreateTableStmt parse create table sql text to CreateTableStmt ast.
func parseCreateTableStmt(sql string) (*ast.CreateTableStmt, error) {
	t, err := parseOneSql(sql)
	if err != nil {
		return nil, err
	}
	createStmt, ok := t.(*ast.CreateTableStmt)
	if !ok {
		return nil, fmt.Errorf("stmt not support")
	}
	return createStmt, nil
}

func parseOneSql(sql string) (ast.StmtNode, error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, err
	}
	return stmt, nil
}

func getAlterTableSpecByTp(specs []*ast.AlterTableSpec, ts ...ast.AlterTableType) []*ast.AlterTableSpec {
	s := []*ast.AlterTableSpec{}
	if specs == nil {
		return s
	}
	for _, spec := range specs {
		for _, tp := range ts {
			if spec.Tp == tp {
				s = append(s, spec)
			}
		}
	}
	return s
}

func hasOneInOptions(Options []*ast.ColumnOption, opTp ...ast.ColumnOptionType) bool {
	// has one exists, return true
	for _, tp := range opTp {
		for _, op := range Options {
			if tp == op.Tp {
				return true
			}
		}
	}
	return false
}

func getPrimaryKey(stmt *ast.CreateTableStmt) (map[string]struct{}, bool) {
	hasPk := false
	pkColumnsName := map[string]struct{}{}
	for _, constraint := range stmt.Constraints {
		if constraint.Tp == ast.ConstraintPrimaryKey {
			hasPk = true
			for _, col := range constraint.Keys {
				pkColumnsName[col.Column.Name.L] = struct{}{}
			}
		}
	}
	if !hasPk {
		for _, col := range stmt.Cols {
			if hasOneInOptions(col.Options, ast.ColumnOptionPrimaryKey) {
				hasPk = true
				pkColumnsName[col.Name.Name.L] = struct{}{}
			}
		}
	}
	return pkColumnsName, hasPk
}

func hasPrimaryKey(stmt *ast.CreateTableStmt) bool {
	_, hasPk := getPrimaryKey(stmt)
	return hasPk
}
