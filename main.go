package main

import (
	"github.com/spf13/cobra"
	"sql-calculator/cmd"
)

func main() {
	var rootCmd = &cobra.Command{}
	rootCmd.AddCommand(cmd.FingerprintCmd)
	rootCmd.Execute()
}
