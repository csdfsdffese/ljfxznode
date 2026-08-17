package cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/csdfsdffese/ljfxznode/common/exec"
	"github.com/spf13/cobra"
)

func checkRunning() (bool, error) {
	o, err := exec.RunCommandByShell("systemctl status ljfxznode | grep Active")
	if err != nil {
		return false, err
	}
	return strings.Contains(o, "running"), nil
}

var (
	startCommand = cobra.Command{
		Use:   "start",
		Short: "Start ljfxznode service",
		Run:   startHandle,
	}
	stopCommand = cobra.Command{
		Use:   "stop",
		Short: "Stop ljfxznode service",
		Run:   stopHandle,
	}
	restartCommand = cobra.Command{
		Use:   "restart",
		Short: "Restart ljfxznode service",
		Run:   restartHandle,
	}
	logCommand = cobra.Command{
		Use:   "log",
		Short: "Output ljfxznode log",
		Run: func(_ *cobra.Command, _ []string) {
			exec.RunCommandStd("journalctl", "-u", "ljfxznode.service", "-e", "--no-pager", "-f")
		},
	}
)

func init() {
	command.AddCommand(&startCommand)
	command.AddCommand(&stopCommand)
	command.AddCommand(&restartCommand)
	command.AddCommand(&logCommand)
}

func startHandle(_ *cobra.Command, _ []string) {
	r, err := checkRunning()
	if err != nil {
		fmt.Println(Err("check status error: ", err))
		fmt.Println(Err("ljfxznode启动失败"))
		return
	}
	if r {
		fmt.Println(Ok("ljfxznode已运行，无需再次启动，如需重启请选择重启"))
		return
	}
	_, err = exec.RunCommandByShell("systemctl start ljfxznode.service")
	if err != nil {
		fmt.Println(Err("exec start cmd error: ", err))
		fmt.Println(Err("ljfxznode启动失败"))
		return
	}
	time.Sleep(time.Second * 3)
	r, err = checkRunning()
	if err != nil {
		fmt.Println(Err("check status error: ", err))
		fmt.Println(Err("ljfxznode启动失败"))
	}
	if !r {
		fmt.Println(Err("ljfxznode可能启动失败，请稍后使用 ljfxznode log 查看日志信息"))
		return
	}
	fmt.Println(Ok("ljfxznode 启动成功，请使用 ljfxznode log 查看运行日志"))
}

func stopHandle(_ *cobra.Command, _ []string) {
	_, err := exec.RunCommandByShell("systemctl stop ljfxznode.service")
	if err != nil {
		fmt.Println(Err("exec stop cmd error: ", err))
		fmt.Println(Err("ljfxznode停止失败"))
		return
	}
	time.Sleep(2 * time.Second)
	r, err := checkRunning()
	if err != nil {
		fmt.Println(Err("check status error:", err))
		fmt.Println(Err("ljfxznode停止失败"))
		return
	}
	if r {
		fmt.Println(Err("ljfxznode停止失败，可能是因为停止时间超过了两秒，请稍后查看日志信息"))
		return
	}
	fmt.Println(Ok("ljfxznode 停止成功"))
}

func restartHandle(_ *cobra.Command, _ []string) {
	_, err := exec.RunCommandByShell("systemctl restart ljfxznode.service")
	if err != nil {
		fmt.Println(Err("exec restart cmd error: ", err))
		fmt.Println(Err("ljfxznode重启失败"))
		return
	}
	r, err := checkRunning()
	if err != nil {
		fmt.Println(Err("check status error: ", err))
		fmt.Println(Err("ljfxznode重启失败"))
		return
	}
	if !r {
		fmt.Println(Err("ljfxznode可能启动失败，请稍后使用 ljfxznode log 查看日志信息"))
		return
	}
	fmt.Println(Ok("ljfxznode重启成功"))
}
