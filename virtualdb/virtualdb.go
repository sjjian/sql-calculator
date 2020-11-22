package virtualdb

import (
	"fmt"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"strings"
)

type TableInfo struct {
	Table *ast.CreateTableStmt
}

type SchemaInfo struct {
	Schema *ast.CreateDatabaseStmt
	Tables map[string]*TableInfo
}

type VirtualDB struct {
	// currentSchema will change after sql "use database"
	currentSchema string
	schemas       map[string]*SchemaInfo
}

func NewVirtualDB(defaultSchema string) *VirtualDB {
	return &VirtualDB{
		currentSchema: defaultSchema,
		schemas:       map[string]*SchemaInfo{},
	}
}

func (c *VirtualDB) useSchema(schema string) error {
	if !c.hasSchema(schema) {
		return fmt.Errorf(NotExistSchemaErrorPattern, schema)
	}
	c.currentSchema = schema
	return nil
}

// getSchemaName get schema name from TableName ast;
// if schema name is default, using current schema from SQL ctx.
func (c *VirtualDB) getSchemaName(stmt *ast.TableName) string {
	if stmt.Schema.String() == "" {
		return c.currentSchema
	} else {
		return stmt.Schema.String()
	}
}

func (c *VirtualDB) getSchema(schemaName string) (*SchemaInfo, bool) {
	schema, exist := c.schemas[schemaName]
	return schema, exist
}

func (c *VirtualDB) hasSchema(schemaName string) (has bool) {
	_, exist := c.getSchema(schemaName)
	return exist
}

func (c *VirtualDB) delSchema(name string) error {
	if !c.hasSchema(name) {
		return fmt.Errorf(NotExistSchemaErrorPattern, name)
	}
	delete(c.schemas, name)
	return nil
}

func (c *VirtualDB) addSchema(schema *ast.CreateDatabaseStmt) error {
	schemaName := schema.Name
	exist := c.hasSchema(schemaName)
	if !exist {
		c.schemas[schemaName] = &SchemaInfo{
			Schema: schema,
			Tables: map[string]*TableInfo{},
		}
		return nil
	}
	if schema.IfNotExists {
		return nil
	}
	return fmt.Errorf(DuplicateSchemaErrorPattern, schemaName)
}

func (c *VirtualDB) getTable(schemaName, tableName string) (*TableInfo, bool, error) {
	schema, SchemaExist := c.getSchema(schemaName)
	if !SchemaExist {
		return nil, false, fmt.Errorf(NotExistSchemaErrorPattern, schemaName)
	}
	table, tableExist := schema.Tables[tableName]
	return table, tableExist, nil
}

func (c *VirtualDB) hasTable(schemaName, tableName string) (bool, error) {
	_, exist, err := c.getTable(schemaName, tableName)
	return exist, err
}

func (c *VirtualDB) delTable(schemaName, tableName string) error {
	exist, err := c.hasTable(schemaName, tableName)
	if err != nil {
		return err
	}
	if !exist {
		return fmt.Errorf(NotExistTableErrorPattern, schemaName, tableName)
	}
	delete(c.schemas[schemaName].Tables, tableName)
	return nil
}

func (c *VirtualDB) addTable(info *TableInfo) error {
	table := info.Table
	schemaName := c.getSchemaName(table.Table)
	tableName := table.Table.Name.String()

	if table.Table.Schema.String() == "" {
		table.Table.Schema = model.NewCIStr(schemaName)
	}

	exist, err := c.hasTable(schemaName, tableName)
	if err != nil {
		return err
	}
	if !exist {
		c.schemas[schemaName].Tables[tableName] = info
		return nil
	}
	// if not exists, do nothing
	if info.Table.IfNotExists {
		return nil
	}
	return fmt.Errorf(DuplicateTableErrorPattern, schemaName, tableName)
}

func (c *VirtualDB) dropTables(stmt *ast.DropTableStmt) error {
	errs := []string{}
	for _, table := range stmt.Tables {
		schemaName := c.getSchemaName(table)
		tableName := table.Name.String()
		exist, err := c.hasTable(schemaName, tableName)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		if !exist {
			schemaName := c.getSchemaName(table)
			tableName := table.Name.String()
			msg := fmt.Sprintf(NotExistTableErrorPattern, schemaName, tableName)
			errs = append(errs, msg)
		}
	}
	if len(errs) != 0 {
		return fmt.Errorf(strings.Join(errs, ","))
	}
	for _, table := range stmt.Tables {
		schemaName := c.getSchemaName(table)
		tableName := table.Name.String()
		err := c.delTable(schemaName, tableName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *VirtualDB) alertTable(alter *ast.AlterTableStmt) error {
	schemaName := c.getSchemaName(alter.Table)
	tableName := alter.Table.Name.String()
	info, exist, err := c.getTable(schemaName, tableName)
	if err != nil {
		return err
	}
	if !exist {
		return fmt.Errorf(NotExistTableErrorPattern, schemaName, tableName)
	}

	newTable, err := c.mergeAlterToTable(info.Table, alter)
	if err != nil {
		return err
	}

	// rename table
	newTableName := newTable.Table.Name.String()
	if newTableName != tableName {
		newInfo := &TableInfo{Table: newTable}
		err := c.addTable(newInfo)
		if err != nil {
			return err
		}
		err = c.delTable(schemaName, tableName)
		if err != nil {
			return err
		}
	} else {
		info.Table = newTable
	}
	return nil
}

func (c *VirtualDB) mergeAlterToTable(oldTable *ast.CreateTableStmt,
	alterTable *ast.AlterTableStmt) (*ast.CreateTableStmt, error) {

	oldTableString, err := restoreToSql(oldTable)
	if err != nil {
		return nil, err
	}
	tmpTable, err := parseCreateTableStmt(oldTableString)
	if err != nil {
		return nil, err
	}

	schemaName := c.getSchemaName(tmpTable.Table)
	tableName := tmpTable.Table.Name.String()

	// rename table
	for _, spec := range getAlterTableSpecByTp(alterTable.Specs, ast.AlterTableRenameTable) {
		tmpTable.Table = spec.NewTable
	}

	// drop column
	for _, spec := range getAlterTableSpecByTp(alterTable.Specs, ast.AlterTableDropColumn) {
		colExists := false
		columnName := spec.OldColumnName.Name.L
		for i, col := range tmpTable.Cols {
			if columnName == col.Name.Name.L {
				colExists = true
				tmpTable.Cols = append(tmpTable.Cols[:i], tmpTable.Cols[i+1:]...)
				break
			}
		}
		if !colExists {
			return nil, fmt.Errorf(NotExistColumnErrorPattern,
				columnName, schemaName, tableName)
		}
	}

	// change column
	for _, spec := range getAlterTableSpecByTp(alterTable.Specs, ast.AlterTableChangeColumn) {
		colExists := false
		columnName := spec.OldColumnName.Name.L
		for i, col := range tmpTable.Cols {
			if columnName == col.Name.Name.L {
				colExists = true
				tmpTable.Cols[i] = spec.NewColumns[0]
				break
			}
		}
		if !colExists {
			return nil, fmt.Errorf(NotExistColumnErrorPattern,
				columnName, schemaName, tableName)
		}
	}

	// modify column
	for _, spec := range getAlterTableSpecByTp(alterTable.Specs, ast.AlterTableModifyColumn) {
		colExists := false
		columnName := spec.NewColumns[0].Name.Name.L
		for i, col := range tmpTable.Cols {
			if columnName == col.Name.Name.L {
				colExists = true
				tmpTable.Cols[i] = spec.NewColumns[0]
				break
			}
		}
		if !colExists {
			return nil, fmt.Errorf(NotExistColumnErrorPattern,
				columnName, schemaName, tableName)
		}
	}

	// alter column
	for _, spec := range getAlterTableSpecByTp(alterTable.Specs, ast.AlterTableAlterColumn) {
		colExists := false
		newCol := spec.NewColumns[0]
		columnName := newCol.Name.Name.L
		for _, col := range tmpTable.Cols {
			if columnName == col.Name.Name.L {
				colExists = true
				// alter table alter column drop default
				if newCol.Options == nil {
					for i, op := range col.Options {
						if op.Tp == ast.ColumnOptionDefaultValue {
							col.Options = append(col.Options[:i], col.Options[i+1:]...)
						}
					}
				} else {
					if hasOneInOptions(col.Options, ast.ColumnOptionDefaultValue) {
						for i, op := range col.Options {
							if op.Tp == ast.ColumnOptionDefaultValue {
								col.Options[i] = newCol.Options[0]
							}
						}
					} else {
						col.Options = append(col.Options, newCol.Options...)
					}
				}
			}
		}
		if !colExists {
			return nil, fmt.Errorf(NotExistColumnErrorPattern,
				columnName, schemaName, tableName)
		}
	}

	// add column
	for _, spec := range getAlterTableSpecByTp(alterTable.Specs, ast.AlterTableAddColumns) {
		for _, newCol := range spec.NewColumns {
			colExist := false
			columnName := newCol.Name.Name.L
			for _, col := range tmpTable.Cols {
				if columnName == col.Name.Name.L {
					colExist = true
					break
				}
			}
			if colExist {
				return nil, fmt.Errorf(DuplicateColumnErrorPattern,
					columnName, schemaName, tableName)
			}
			tmpTable.Cols = append(tmpTable.Cols, newCol)
		}
	}

	// drop primary key
	for _, spec := range getAlterTableSpecByTp(alterTable.Specs, ast.AlterTableDropPrimaryKey) {
		_ = spec
		if !hasPrimaryKey(tmpTable) {
			return nil, NoPrimaryKeyError
		}
		for i, constraint := range tmpTable.Constraints {
			switch constraint.Tp {
			case ast.ConstraintPrimaryKey:
				tmpTable.Constraints = append(tmpTable.Constraints[:i], tmpTable.Constraints[i+1:]...)
			}
		}
		for _, col := range tmpTable.Cols {
			for i, op := range col.Options {
				switch op.Tp {
				case ast.ColumnOptionPrimaryKey:
					col.Options = append(col.Options[:i], col.Options[i+1:]...)
				}
			}
		}
	}

	// drop index
	for _, spec := range getAlterTableSpecByTp(alterTable.Specs, ast.AlterTableDropIndex) {
		indexName := spec.Name
		constraintExists := false
		for i, constraint := range tmpTable.Constraints {
			if constraint.Name == indexName {
				constraintExists = true
				tmpTable.Constraints = append(tmpTable.Constraints[:i], tmpTable.Constraints[i+1:]...)
			}
		}
		if !constraintExists {
			return nil, fmt.Errorf(NotExistIndexErrorPattern,
				indexName, schemaName, tableName)
		}
	}

	// rename index
	for _, spec := range getAlterTableSpecByTp(alterTable.Specs, ast.AlterTableRenameIndex) {
		oldName := spec.FromKey
		newName := spec.ToKey

		// new name exist
		newConstraintExist := false
		for _, constraint := range tmpTable.Constraints {
			if newName.String() == constraint.Name {
				newConstraintExist = true
				constraint.Name = newName.String()
			}
		}
		if newConstraintExist {
			return nil, fmt.Errorf(DuplicateIndexErrorPattern,
				newName, schemaName, tableName)
		}

		oldConstraintExists := false
		for _, constraint := range tmpTable.Constraints {
			if oldName.String() == constraint.Name {
				oldConstraintExists = true
				constraint.Name = newName.String()
			}
		}
		if !oldConstraintExists {
			return nil, fmt.Errorf(NotExistIndexErrorPattern,
				oldName, schemaName, tableName)
		}
	}

	// add index
	for _, spec := range getAlterTableSpecByTp(alterTable.Specs, ast.AlterTableAddConstraint) {
		switch spec.Constraint.Tp {
		case ast.ConstraintPrimaryKey:
			if hasPrimaryKey(tmpTable) {
				return nil, ExistPrimaryKeyError
			}
			tmpTable.Constraints = append(tmpTable.Constraints, spec.Constraint)
		default:
			indexName := spec.Constraint.Name
			constraintExists := false
			for _, constraint := range tmpTable.Constraints {
				if indexName == constraint.Name {
					constraintExists = true
					break
				}
			}
			if constraintExists {
				return nil, fmt.Errorf(DuplicateIndexErrorPattern,
					indexName, schemaName, tableName)
			}
			tmpTable.Constraints = append(tmpTable.Constraints, spec.Constraint)
		}
	}
	return tmpTable, nil
}

func (c *VirtualDB) Exec(node ast.Node) error {
	switch s := node.(type) {
	case *ast.UseStmt:
		return c.useSchema(s.DBName)

	case *ast.CreateDatabaseStmt:
		return c.addSchema(s)

	case *ast.DropDatabaseStmt:
		return c.delSchema(s.Name)

	case *ast.CreateTableStmt:
		return c.addTable(&TableInfo{
			Table: s,
		})

	case *ast.DropTableStmt:
		return c.dropTables(s)

	case *ast.AlterTableStmt:
		return c.alertTable(s)
	default:
	}
	return nil
}

func (c *VirtualDB) ExecSQL(sql string) error {
	p := parser.New()
	stmts, _, err := p.Parse(sql, "", "")
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		err := c.Exec(stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *VirtualDB) Text() (string, error) {
	var sb strings.Builder
	for _, schema := range c.schemas {
		sql, err := restoreToSql(schema.Schema)
		if err != nil {
			return "", err
		}
		sb.WriteString(sql)
		sb.WriteString(";\n")
		for _, table := range schema.Tables {
			sql, err := restoreToSql(table.Table)
			if err != nil {
				return "", err
			}
			sb.WriteString(sql)
			sb.WriteString(";\n")
		}
	}
	return sb.String(), nil
}

func (c *VirtualDB) GetTableStmts(schemaName string) (map[string]*TableInfo, bool) {
	schema, exist := c.getSchema(schemaName)
	if !exist {
		return nil, false
	}
	return schema.Tables, true
}

func (c *VirtualDB) GetColumn(schemaName, tableName, columnName string) (*ast.ColumnDef, bool, error) {
	table, exist, err := c.getTable(schemaName, tableName)
	if err != nil {
		return nil, false, err
	}
	if !exist {
		return nil, false, fmt.Errorf(NotExistTableErrorPattern, schemaName, tableName)
	}
	for _, col := range table.Table.Cols {
		if col.Name.String() == columnName {
			return col, true, nil
		}
	}
	return nil, false, nil
}
