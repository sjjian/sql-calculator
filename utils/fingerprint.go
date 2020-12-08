package utils

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	_ "github.com/pingcap/tidb/types/parser_driver"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

func Fingerprint(sql string) (string, error) {
	node, err := ParseOneSql(sql)
	if err != nil {
		return "", err
	}
	node.Accept(&FingerprintVisitor{})
	fp, err := RestoreToSqlWithFlag(format.RestoreKeyWordUppercase|format.RestoreNameBackQuotes, node)
	if err != nil {
		return "", err
	}
	return fp, nil
}

type FingerprintVisitor struct{}

func (f *FingerprintVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	if v, ok := n.(*driver.ValueExpr); ok {
		v.Type.Charset = ""
		v.SetValue([]byte("?"))
	}
	return n, false
}

func (f *FingerprintVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, true
}
