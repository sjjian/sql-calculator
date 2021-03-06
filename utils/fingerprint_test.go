package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type FpCase struct {
	input  string
	expect string
}

func TestFingerprint(t *testing.T) {
	cases := []FpCase{
		{
			input:  `update  tb1 set a = "2" where a = "3" and b = 4`,
			expect: "UPDATE `tb1` SET `a`=? WHERE `a`=? AND `b`=?",
		},
		{
			input:  "select * from tb1 where a in (select a from tb2 where b = 2) and c = 100",
			expect: "SELECT * FROM `tb1` WHERE `a` IN (SELECT `a` FROM `tb2` WHERE `b`=?) AND `c`=?",
		},
		{
			input:  `delete from tb1 where a="2"`,
			expect: "DELETE FROM `tb1` WHERE `a`=?",
		},
		{
			input:  `select a from tb1 where b in (1,2,3)`,
			expect: "SELECT `a` FROM `tb1` WHERE `b` IN (?,?,?)",
		},
		{
			input:  `select sleep(5)`,
			expect: "SELECT SLEEP(?)",
		},
		{
			input:  `select 1`,
			expect: "SELECT ?",
		},
		{
			input:  `select * from tb1 limit 10`,
			expect: "SELECT * FROM `tb1` LIMIT ?",
		},
		{
			input:  `select * from tb1 limit 10,10`,
			expect: "SELECT * FROM `tb1` LIMIT ?,?",
		},
		{
			input:  `select * from tb1 limit 10 offset 10`,
			expect: "SELECT * FROM `tb1` LIMIT ?,?",
		},
	}
	for _, c := range cases {
		testFingerprint(t, c.input, c.expect)
	}
}

func testFingerprint(t *testing.T, input, expect string) {
	actual, err := Fingerprint(input)
	assert.NoError(t, err)
	if err != nil {
		return
	}
	assert.Equal(t, expect, actual)
}
