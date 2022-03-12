package zfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"

	"github.com/outofforest/libexec"
)

var dsPropListOptions = strings.Join([]string{"name", "origin", "used", "available", "mountpoint", "compression", "volsize", "quota", "referenced", "written", "logicalused", "usedbydataset"}, ",")

type cmdError struct {
	Err    error
	Stderr string
}

// Error returns the string representation of an Error.
func (e cmdError) Error() string {
	return fmt.Sprintf("%s => %s", e.Err, e.Stderr)
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

func info(ctx context.Context, t, filter string, depth uint16) ([]Info, error) {
	args := []string{"list", "-Hp", "-t", t, "-o", dsPropListOptions, "-d", strconv.FormatUint(uint64(depth), 10)}
	if filter != "" {
		args = append(args, filter)
	}
	out, err := zfs(ctx, args...)
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

// DestroyFlag is the options flag passed to Destroy
type DestroyFlag int

// Valid destroy options
const (
	DestroyDefault         DestroyFlag = 1 << iota
	DestroyRecursive                   = 1 << iota
	DestroyRecursiveClones             = 1 << iota
	DestroyDeferDeletion               = 1 << iota
	DestroyForceUmount                 = 1 << iota
)

// zfs is a helper function to wrap typical calls to zfs.
func zfs(ctx context.Context, args ...string) ([][]string, error) {
	return zfsStdin(ctx, nil, args...)
}

func zfsStdin(ctx context.Context, stdin io.Reader, args ...string) ([][]string, error) {
	sOut := &bytes.Buffer{}
	sErr := &bytes.Buffer{}
	cmd := exec.Command("zfs", args...)
	cmd.Stdout = sOut
	cmd.Stderr = sErr
	cmd.Stdin = stdin
	if err := libexec.Exec(ctx, cmd); err != nil {
		return nil, &cmdError{Err: err, Stderr: sErr.String()}
	}

	lines := strings.Split(sOut.String(), "\n")

	// last line is always blank
	lines = lines[0 : len(lines)-1]
	output := make([][]string, len(lines))

	for i, l := range lines {
		output[i] = strings.Fields(l)
	}

	return output, nil
}

func zfsStdout(ctx context.Context, stdout io.Writer, args ...string) error {
	sErr := &bytes.Buffer{}
	cmd := exec.Command("zfs", args...)
	cmd.Stdout = stdout
	cmd.Stderr = sErr
	if err := libexec.Exec(ctx, cmd); err != nil {
		return &cmdError{Err: err, Stderr: sErr.String()}
	}
	return nil
}

func destroy(ctx context.Context, name string, flags DestroyFlag) error {
	args := make([]string, 1, 3)
	args[0] = "destroy"
	if flags&DestroyRecursive != 0 {
		args = append(args, "-r")
	}

	if flags&DestroyRecursiveClones != 0 {
		args = append(args, "-R")
	}

	if flags&DestroyDeferDeletion != 0 {
		args = append(args, "-d")
	}

	if flags&DestroyForceUmount != 0 {
		args = append(args, "-f")
	}

	args = append(args, name)
	_, err := zfs(ctx, args...)
	return err
}

func setProperty(ctx context.Context, name, key, val string) error {
	prop := strings.Join([]string{key, val}, "=")
	_, err := zfs(ctx, "set", prop, name)
	return err
}

func getProperty(ctx context.Context, name, key string) (string, error) {
	out, err := zfs(ctx, "get", "-H", key, name)
	if err != nil {
		return "", err
	}

	return out[0][2], nil
}
