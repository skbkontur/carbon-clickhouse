package main

import (
	"bufio"
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
	"github.com/lomik/carbon-clickhouse/helper/tests"
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

	maxLen := tests.Max(len(ss), len(verify.Output))
	for i := 0; i < maxLen; i++ {
		if i >= len(ss) {
			errs = append(errs, fmt.Sprintf("- [%d]: %s", i, verify.Output[i]))
		} else if i >= len(verify.Output) {
			errs = append(errs, fmt.Sprintf("+ [%d]: %s", i, ss[i]))
		} else if ss[i] != verify.Output[i] {
			errs = append(errs, fmt.Sprintf("- [%d]: %s", i, verify.Output[i]))
			errs = append(errs, fmt.Sprintf("+ [%d]: %s", i, ss[i]))
		}
	}
	return errs
}

func testCarbonClickhouse(
	inputType InputType, test *TestSchema, clickhouse Clickhouse,
	testDir, rootDir string,
	verbose, breakOnError bool, logger *zap.Logger) (testSuccess bool) {

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
				if breakOnError {
					debug(test, &clickhouse, &cch)
				}
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
					if breakOnError {
						debug(test, &clickhouse, &cch)
					}
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

func runTest(config string, rootDir string, verbose, breakOnError bool, logger *zap.Logger) (failed, total int) {
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
		if exist, out := containerExist(clickhouse.Docker, ClickhouseContainerName); exist {
			logger.Error("clickhouse already exist",
				zap.String("container", ClickhouseContainerName),
				zap.String("out", out),
			)
			failed++
			total++
			continue
		}
		for _, inputType := range cfg.Test.InputTypes {
			total++
			if !testCarbonClickhouse(inputType, cfg.Test, clickhouse, testDir, rootDir, verbose, breakOnError, logger) {
				failed++
			}
		}
	}

	return
}

func debug(test *TestSchema, ch *Clickhouse, cch *CarbonClickhouse) {
	for {
		fmt.Printf("carbon-clickhouse URL: %s , clickhouse URL: %s\n",
			cch.Address(), cch.Address())
		fmt.Println("Some queries was failed, press y for continue after debug test, k for kill carbon-clickhouse:")
		in := bufio.NewScanner(os.Stdin)
		in.Scan()
		s := in.Text()
		if s == "y" || s == "Y" {
			break
		} else if s == "k" || s == "K" {
			cch.Stop()
		}
	}
}
