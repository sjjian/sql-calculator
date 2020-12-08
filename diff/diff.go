package diff

import (
	"fmt"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"sql-calculator/utils"
	"sql-calculator/virtualdb"
)

func GetDiffFromSqlFile(dbName, sourceSqlFile, targetSqlFile string) ([]ast.StmtNode, error) {
	sourceDb := virtualdb.NewVirtualDB(dbName)
	sourceDb.ExecSQL(fmt.Sprintf("create database if not exists %s", dbName))
	err := sourceDb.ExecSQL(sourceSqlFile)
	if err != nil {
		return nil, err
	}

	targetDb := virtualdb.NewVirtualDB(dbName)
	targetDb.ExecSQL(fmt.Sprintf("create database if not exists %s", dbName))

	targetDb.ExecSQL(targetSqlFile)
	if err != nil {
		return nil, err
	}

	allDDL := []ast.StmtNode{}
	targetTables, _ := targetDb.GetTableStmts(dbName)

	for _, targetTable := range targetTables {
		err := sourceDb.Exec(targetTable.Table)
		if err == nil {
			allDDL = append(allDDL, targetTable.Table)
			continue
		}

		for _, col := range targetTable.Table.Cols {
			addColumn := changeColumnToAddColumn(targetTable.Table.Table, col)
			err := sourceDb.Exec(addColumn)
			if err == nil {
				allDDL = append(allDDL, addColumn)
				continue
			}
			column, _, _ := sourceDb.GetColumn(dbName,
				targetTable.Table.Table.Name.String(), col.Name.String())

			if compareColumn(col, column) {
				continue
			}
			modifyColumn := changeColumnToModifyColumn(targetTable.Table.Table, col)
			allDDL = append(allDDL, modifyColumn)
		}
	}

	sourceTables, _ := sourceDb.GetTableStmts(dbName)
	for _, source := range sourceTables {
		err := targetDb.Exec(source.Table)
		if err == nil {
			allDDL = append(allDDL, &ast.DropTableStmt{
				Tables: []*ast.TableName{
					source.Table.Table,
				},
			})
			continue
		}
		for _, col := range source.Table.Cols {
			addColumn := changeColumnToAddColumn(source.Table.Table, col)
			err := targetDb.Exec(addColumn)
			if err == nil {
				allDDL = append(allDDL, changeColumnToDropColumn(source.Table.Table, col))
				continue
			}
		}
	}
	return allDDL, nil
}

func changeColumnToAddColumn(table *ast.TableName,
	col *ast.ColumnDef) *ast.AlterTableStmt {
	return &ast.AlterTableStmt{
		Table: table,
		Specs: []*ast.AlterTableSpec{
			&ast.AlterTableSpec{
				Tp: ast.AlterTableAddColumns,
				NewColumns: []*ast.ColumnDef{
					col,
				},
			},
		},
	}
}

func changeColumnToModifyColumn(table *ast.TableName,
	col *ast.ColumnDef) *ast.AlterTableStmt {
	return &ast.AlterTableStmt{
		Table: table,
		Specs: []*ast.AlterTableSpec{
			&ast.AlterTableSpec{
				Tp:         ast.AlterTableModifyColumn,
				NewColumns: []*ast.ColumnDef{col},
				Position: &ast.ColumnPosition{
					Tp: ast.ColumnPositionNone,
				},
			},
		},
	}
}

func changeConstraintToAddConstraint(table *ast.TableName,
	constraint *ast.Constraint) *ast.AlterTableStmt {
	return &ast.AlterTableStmt{
		Table: table,
		Specs: []*ast.AlterTableSpec{
			&ast.AlterTableSpec{
				Tp:         ast.AlterTableAddConstraint,
				Constraint: constraint,
			},
		},
	}
}

func changeColumnToDropColumn(table *ast.TableName,
	col *ast.ColumnDef) *ast.AlterTableStmt {
	return &ast.AlterTableStmt{
		Table: table,
		Specs: []*ast.AlterTableSpec{
			&ast.AlterTableSpec{
				Tp:            ast.AlterTableDropColumn,
				OldColumnName: col.Name,
			},
		},
	}
}

func compareColumn(source, target *ast.ColumnDef) bool {
	s, _ := utils.RestoreToSql(source)
	t, _ := utils.RestoreToSql(target)
	return s == t
}
