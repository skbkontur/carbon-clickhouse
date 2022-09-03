package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"go.uber.org/zap"
)

type InputType int

const (
	InputPlainTCP InputType = iota
)

var inputStrings []string = []string{"tcp_plain"}

func (a *InputType) String() string {
	return inputStrings[*a]
}

func (a *InputType) Set(value string) error {
	switch value {
	case "plain_tcp":
		*a = InputPlainTCP
	default:
		return fmt.Errorf("invalid input type %s", value)
	}
	return nil
}

func (a *InputType) UnmarshalText(text []byte) error {
	return a.Set(string(text))
}

type Verify struct {
	Query  string   `yaml:"query"`
	Output []string `yaml:"output"`
}

type TestSchema struct {
	InputTypes []InputType `toml:"input_types"` // carbon-clickhouse input types

	Input      []string     `toml:"input"`           // carbon-clickhouse input
	ConfigTpl  string       `toml:"config_template"` // carbon-clickhouse config template
	Clickhouse []Clickhouse `yaml:"clickhouse"`

	Verify []Verify `yaml:"verify"`

	name string `yaml:"-"` // test alias (from config name)
}

func getFreeTCPPort(name string) (string, error) {
	if len(name) == 0 {
		name = "127.0.0.1:0"
	} else if !strings.Contains(name, ":") {
		name = name + ":0"
	}
	addr, err := net.ResolveTCPAddr("tcp", name)
	if err != nil {
		return name, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return name, err
	}
	defer l.Close()
	return l.Addr().String(), nil
}

func sendPlain(network, address string, input []string) error {
	if conn, err := net.DialTimeout(network, address, time.Second); err != nil {
		return err
	} else {
		for _, m := range input {
			conn.SetDeadline(time.Now().Add(time.Second))
			if _, err = conn.Write([]byte(m + "\n")); err != nil {
				return err
			}
		}
		return conn.Close()
	}
}

func verifyOut(address string, verify Verify) []string {
	var errs []string

	q := []byte(verify.Query)
	req, err := http.NewRequest("POST", "http://"+address+"/", bytes.NewBuffer(q))

	client := &http.Client{Timeout: time.Second * 5}
	resp, err := client.Do(req)
	if err != nil {
		return []string{err.Error()}
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []string{err.Error()}
	}
	s := strings.TrimRight(string(body), "\n")
	if resp.StatusCode != 200 {
		return []string{"response status is '" + resp.Status + "', " + s}
	}

	s = strings.ReplaceAll(s, "\t", " ")
	ss := strings.Split(s, "\n")
	if len(ss) == 1 && len(ss[0]) == 0 {
		ss = []string{} /* results is empthy */
	}

	max := len(ss)
	if max < len(verify.Output) {
		max = len(verify.Output)
	}
	for i := 0; i < max; i++ {
		if i >= len(ss) {
			errs = append(errs, "- "+verify.Output[i])
		} else if i >= len(verify.Output) {
			errs = append(errs, "+ "+ss[i])
		} else if ss[i] != verify.Output[i] {
			errs = append(errs, "- "+verify.Output[i])
			errs = append(errs, "+ "+ss[i])
		}
	}
	return errs
}

func testCarbonClickhouse(
	inputType InputType, test *TestSchema, clickhouse Clickhouse,
	testDir, rootDir string,
	verbose bool, logger *zap.Logger) (testSuccess bool) {

	testSuccess = true

	clickhouseDir := clickhouse.Dir // for logging
	if !strings.HasPrefix(clickhouse.Dir, "/") {
		clickhouse.Dir = rootDir + "/" + clickhouse.Dir
	}
	err, out := clickhouse.Start()
	if err != nil {
		logger.Error("starting clickhouse",
			zap.String("config", test.name),
			zap.String("input", inputType.String()),
			zap.Any("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouseDir),
			zap.Error(err),
			zap.String("out", out),
		)
		testSuccess = false
		clickhouse.Stop(true)
		return
	}

	cch := CarbonClickhouse{
		ConfigTpl: testDir + "/" + test.ConfigTpl,
	}
	err = cch.Start(clickhouse.Address())
	if err != nil {
		logger.Error("starting carbon-clickhouse",
			zap.String("config", test.name),
			zap.String("input", inputType.String()),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouseDir),
			zap.Error(err),
			zap.String("out", out),
		)
		testSuccess = false
	}

	if testSuccess {
		logger.Info("starting e2e test",
			zap.String("config", test.name),
			zap.String("input", inputType.String()),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouseDir),
		)
		time.Sleep(2 * time.Second)
		// Run test

		if len(test.Input) > 0 {
			switch inputType {
			case InputPlainTCP:
				err = sendPlain("tcp", cch.address, test.Input)
			default:
				err = fmt.Errorf("input type not implemented")
			}
			if err != nil {
				logger.Error("send plain to carbon-clickhouse",
					zap.String("config", test.name),
					zap.String("input", inputType.String()),
					zap.String("clickhouse version", clickhouse.Version),
					zap.String("clickhouse config", clickhouseDir),
					zap.Error(err),
				)
				testSuccess = false
			}
		}

		if testSuccess {
			verifyFailed := 0
			time.Sleep(10 * time.Second)
			for _, verify := range test.Verify {
				if errs := verifyOut(clickhouse.Address(), verify); len(errs) > 0 {
					testSuccess = false
					verifyFailed++
					for _, e := range errs {
						fmt.Fprintln(os.Stderr, e)
					}
					logger.Error("verify records in clickhouse",
						zap.String("config", test.name),
						zap.String("input", inputType.String()),
						zap.String("clickhouse version", clickhouse.Version),
						zap.String("clickhouse config", clickhouseDir),
						zap.String("verify", verify.Query),
					)
				} else if verbose {
					logger.Info("verify records in clickhouse",
						zap.String("config", test.name),
						zap.String("input", inputType.String()),
						zap.String("clickhouse version", clickhouse.Version),
						zap.String("clickhouse config", clickhouseDir),
						zap.String("verify", verify.Query),
					)
				}
			}
			if verifyFailed > 0 {
				logger.Error("verify records in clickhouse",
					zap.String("config", test.name),
					zap.String("input", inputType.String()),
					zap.String("clickhouse version", clickhouse.Version),
					zap.String("clickhouse config", clickhouseDir),
					zap.Int("verify failed", verifyFailed),
					zap.Int("verify total", len(test.Verify)),
				)
			} else {
				logger.Info("verify records in clickhouse",
					zap.String("config", test.name),
					zap.String("input", inputType.String()),
					zap.String("clickhouse version", clickhouse.Version),
					zap.String("clickhouse config", clickhouseDir),
					zap.Int("verify success", len(test.Verify)),
					zap.Int("verify total", len(test.Verify)),
				)
			}
		}
	}

	err = cch.Stop()
	cch.Cleanup()
	if err != nil {
		logger.Error("stoping carbon-clickhouse",
			zap.String("config", test.name),
			zap.String("input", inputType.String()),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouseDir),
			zap.Error(err),
			zap.String("out", out),
		)
		testSuccess = false
	}

	err, out = clickhouse.Stop(true)
	if err != nil {
		logger.Error("stoping clickhouse",
			zap.String("config", test.name),
			zap.String("input", inputType.String()),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouseDir),
			zap.Error(err),
			zap.String("out", out),
		)
		testSuccess = false
	}

	if testSuccess {
		logger.Info("end e2e test",
			zap.String("config", test.name),
			zap.String("input", inputType.String()),
			zap.String("status", "success"),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouseDir),
		)
	} else {
		logger.Error("end e2e test",
			zap.String("config", test.name),
			zap.String("input", inputType.String()),
			zap.String("status", "failed"),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouseDir),
		)
	}

	return
}

func runTest(config string, rootDir string, verbose bool, logger *zap.Logger) (failed, total int) {
	testDir := path.Dir(config)
	d, err := ioutil.ReadFile(config)
	if err != nil {
		logger.Error("failed to read config",
			zap.String("config", config),
			zap.Error(err),
		)
		failed++
		total++
		return
	}

	confShort := strings.ReplaceAll(config, rootDir+"/", "")

	var cfg = MainConfig{}
	if _, err := toml.Decode(string(d), &cfg); err != nil {
		logger.Fatal("failed to decode config",
			zap.String("config", confShort),
			zap.Error(err),
		)
	}

	cfg.Test.name = confShort
	if len(cfg.Test.InputTypes) == 0 {
		cfg.Test.InputTypes = []InputType{InputPlainTCP}
	}

	if len(cfg.Test.Input) == 0 {
		logger.Fatal("input not set",
			zap.String("config", confShort),
		)
	}

	for _, clickhouse := range cfg.Test.Clickhouse {
		for _, inputType := range cfg.Test.InputTypes {
			total++
			if !testCarbonClickhouse(inputType, cfg.Test, clickhouse, testDir, rootDir, verbose, logger) {
				failed++
			}
		}
	}

	return
}
