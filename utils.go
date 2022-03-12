package zfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// List of ZFS properties to retrieve from zfs list command on a non-Solaris platform
var dsPropList = []string{"name", "origin", "used", "available", "mountpoint", "compression", "volsize", "quota", "referenced", "written", "logicalused", "usedbydataset"}

var dsPropListOptions = strings.Join(dsPropList, ",")

type command struct {
	Command string
	Stdin   io.Reader
	Stdout  io.Writer
}

func (c *command) Run(arg ...string) ([][]string, error) {
	cmd := exec.Command(c.Command, arg...)

	var stdout, stderr bytes.Buffer

	if c.Stdout == nil {
		cmd.Stdout = &stdout
	} else {
		cmd.Stdout = c.Stdout
	}

	if c.Stdin != nil {
		cmd.Stdin = c.Stdin
	}
	cmd.Stderr = &stderr

	id := uuid.New().String()
	joinedArgs := strings.Join(cmd.Args, " ")

	logger.Log([]string{"ID:" + id, "START", joinedArgs})
	err := cmd.Run()
	logger.Log([]string{"ID:" + id, "FINISH"})

	if err != nil {
		return nil, &Error{
			Err:    err,
			Debug:  strings.Join([]string{cmd.Path, joinedArgs[1:]}, " "),
			Stderr: stderr.String(),
		}
	}

	// assume if you passed in something for stdout, that you know what to do with it
	if c.Stdout != nil {
		return nil, nil
	}

	lines := strings.Split(stdout.String(), "\n")

	// last line is always blank
	lines = lines[0 : len(lines)-1]
	output := make([][]string, len(lines))

	for i, l := range lines {
		output[i] = strings.Fields(l)
	}

	return output, nil
}

func setString(field *string, value string) {
	v := ""
	if value != "-" {
		v = value
	}
	*field = v
}

func setUint(field *uint64, value string) error {
	var v uint64
	if value != "-" {
		var err error
		v, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			return err
		}
	}
	*field = v
	return nil
}

// Info contains dataset info
type Info struct {
	Name          string
	Origin        string
	Used          uint64
	Avail         uint64
	Mountpoint    string
	Compression   string
	Written       uint64
	Volsize       uint64
	Logicalused   uint64
	Usedbydataset uint64
	Quota         uint64
	Referenced    uint64
}

func info(t, filter string, recursive bool) ([]Info, error) {
	args := []string{"list", "-Hp", "-t", t, "-o", dsPropListOptions}

	if filter != "" {
		if recursive {
			args = append(args, "-r")
		}
		args = append(args, filter)
	}
	out, err := zfs(args...)
	if err != nil {
		return nil, err
	}

	infos := []Info{}
	for _, line := range out {
		var info Info
		if err := parseLine(line, &info); err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}

	return infos, nil
}

func parseLine(line []string, info *Info) error {
	var err error

	if len(line) != len(dsPropList) {
		return errors.New("output does not match what is expected on this platform")
	}
	setString(&info.Name, line[0])
	setString(&info.Origin, line[1])

	if err = setUint(&info.Used, line[2]); err != nil {
		return err
	}
	if err = setUint(&info.Avail, line[3]); err != nil {
		return err
	}

	setString(&info.Mountpoint, line[4])
	setString(&info.Compression, line[5])

	if err = setUint(&info.Volsize, line[6]); err != nil {
		return err
	}
	if err = setUint(&info.Quota, line[7]); err != nil {
		return err
	}
	if err = setUint(&info.Referenced, line[8]); err != nil {
		return err
	}

	if err = setUint(&info.Written, line[9]); err != nil {
		return err
	}
	if err = setUint(&info.Logicalused, line[10]); err != nil {
		return err
	}
	if err = setUint(&info.Usedbydataset, line[11]); err != nil {
		return err
	}

	return nil
}

func propsSlice(properties map[string]string) []string {
	args := make([]string, 0, len(properties)*3)
	for k, v := range properties {
		args = append(args, "-o")
		args = append(args, fmt.Sprintf("%s=%s", k, v))
	}
	return args
}
