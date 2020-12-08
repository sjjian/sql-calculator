package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"sql-calculator/utils"
)

var sql *string

var FingerprintCmd = &cobra.Command{
	Use:   "fp [SQL content](string)",
	Args:  cobra.MinimumNArgs(1),
	Short: "SQL Fingerprint",
	Example: "   ./sql-calculator fp \"update tb1 set a = \"2\" where a = \"3\" and b = 4\n\n" +
		"Output:\n   UPDATE `tb1` SET `a`=? WHERE `a`=? AND `b`=?",
	Long: "SQL Fingerprint - Replace all expression value of the SQL with ?",
	Run: func(cmd *cobra.Command, args []string) {
		if q, err := utils.Fingerprint(args[0]); err != nil {
			fmt.Println(err)
			os.Exit(1)
		} else {
			fmt.Println(q)
		}
	},
}
