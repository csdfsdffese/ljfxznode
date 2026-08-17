package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/csdfsdffese/ljfxznode/common/exec"
	"github.com/spf13/cobra"
)

var targetVersion string

var (
	updateCommand = cobra.Command{
		Use:   "update",
		Short: "Update ljfxznode version",
		Run: func(_ *cobra.Command, _ []string) {
			// bash -c 才能解析进程替换 <(...)；域名与 ljfxznode-script/install.sh 保持一致
			// install.sh 依据「是否传参」决定走 latest 还是指定版本分支：
			// 未指定版本时不传任何参数（传空字符串会被解析为版本 "v"，导致下载 404）。
			args := ""
			if targetVersion != "" {
				args = " \"" + targetVersion + "\""
			}
			exec.RunCommandStd("bash", "-c",
				"bash <(curl -Ls https://raw.githubusercontent.com/csdfsdffese/ljfxznode-script/master/install.sh)"+args)
		},
		Args: cobra.NoArgs,
	}
	uninstallCommand = cobra.Command{
		Use:   "uninstall",
		Short: "Uninstall ljfxznode",
		Run:   uninstallHandle,
	}
)

func init() {
	updateCommand.PersistentFlags().StringVar(&targetVersion, "version", "", "update target version")
	command.AddCommand(&updateCommand)
	command.AddCommand(&uninstallCommand)
}

func uninstallHandle(_ *cobra.Command, _ []string) {
	var yes string
	fmt.Println(Warn("确定要卸载 ljfxznode 吗?(Y/n)"))
	fmt.Scan(&yes)
	if strings.ToLower(yes) != "y" {
		fmt.Println("已取消卸载")
		return
	}
	_, err := exec.RunCommandByShell("systemctl stop ljfxznode&&systemctl disable ljfxznode")
	if err != nil {
		fmt.Println(Err("exec cmd error: ", err))
		fmt.Println(Err("卸载失败"))
		return
	}
	_ = os.RemoveAll("/etc/systemd/system/ljfxznode.service")
	_ = os.RemoveAll("/etc/ljfxznode/")
	_ = os.RemoveAll("/usr/local/ljfxznode/")
	_ = os.RemoveAll("/usr/bin/ljfxznode")
	_, err = exec.RunCommandByShell("systemctl daemon-reload&&systemctl reset-failed")
	if err != nil {
		fmt.Println(Err("exec cmd error: ", err))
		fmt.Println(Err("卸载失败"))
		return
	}
	fmt.Println(Ok("卸载成功"))
}
