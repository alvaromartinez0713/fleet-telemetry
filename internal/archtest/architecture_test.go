// Package archtest enforces the dependency rules that make this repository a
// modular monolith: one module is one independently deployable unit
// (internal/<context>/<module>) with its own binary, a minimal public surface
// (module.go) and no knowledge of another module's internals.
//
// The rules are checked by parsing imports, not by reading code: the test walks
// every package of the repository, follows the import graph and reports the
// offending file and import. Each rule has its own checker function and its own
// failing case in the tests below, so a violation is fixed from the message.
//
// The three rules, as the dependency contract states them:
//
//  1. A module may import internal/contracts and internal/platform/...; it must
//     not import any other module, at any depth. A shared package (contracts,
//     platform) must not import a module either, or every importer of the shared
//     package would depend on that module.
//  2. An app package (the core) must not import a framework or driver
//     (gin-gonic/gin, net/http, jackc/pgx, twmb/franz-go) nor its own adapter,
//     directly or through another package of this repository. Every external
//     interaction crosses a port the core declares.
//  3. A cmd/ entrypoint may import the standard library, internal/platform/...
//     and its own module. It is a composition root: config, build, run,
//     graceful shutdown, and nothing else.
package archtest

import (
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// modulePath is the Go module every import path is relative to (go.mod).
const modulePath = "fleet-telemetry"

// ruleNames make a failure readable on its own: the number says which rule, the
// name says what it protects.
var ruleNames = map[int]string{
	1: "module isolation",
	2: "core independence",
	3: "composition root",
}

// coreForbidden are the framework and driver packages an app package must not
// reach, directly or indirectly.
var coreForbidden = []string{
	"net/http",
	"github.com/gin-gonic/gin",
	"github.com/jackc/pgx",
	"github.com/twmb/franz-go",
}

// pkg is one package of this repository as the rules see it.
type pkg struct {
	// dir is the repository-relative directory, slash separated.
	dir string
	// imports are the imports of its build files.
	imports []importLine
}

// importLine is one import: what was imported and the file that imported it, so
// a failure names both.
type importLine struct {
	path string
	file string
}

// importPath is the package's own import path.
func (p pkg) importPath() string {
	if p.dir == "" || p.dir == "." {
		return modulePath
	}
	return modulePath + "/" + p.dir
}

// fileOf returns the file of this package that carries the import, so a
// violation can point at the line to change.
func (p pkg) fileOf(imp string) string {
	for _, line := range p.imports {
		if line.path == imp {
			return line.file
		}
	}
	return ""
}

// violation is one broken rule. String is what the reviewer reads in the test
// output: the rule, the offending file and import, and why it is broken.
type violation struct {
	Rule   int
	File   string
	Import string
	Why    string
}

func (v violation) String() string {
	return fmt.Sprintf("rule %d (%s): %s imports %s — %s", v.Rule, ruleNames[v.Rule], v.File, v.Import, v.Why)
}

// loaded is the parsed repository: every package indexed by directory and by
// import path, plus the module list the command rule needs.
type loaded struct {
	pkgs     []*pkg
	byDir    map[string]*pkg
	byImport map[string]*pkg
	modules  []string
}

// reach walks the repository import graph from p and returns every repository
// package p imports at any depth, mapped to the direct import of p that leads
// there. Standard library and third-party imports are leaves: they cannot import
// back into this repository.
func (l *loaded) reach(p *pkg) map[string]string {
	via := map[string]string{p.dir: ""}
	queue := []string{p.dir}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, imp := range l.byDir[current].imports {
			next, ok := l.byImport[imp.path]
			if !ok {
				continue
			}
			if _, seen := via[next.dir]; seen {
				continue
			}
			first := via[current]
			if first == "" {
				first = imp.path
			}
			via[next.dir] = first
			queue = append(queue, next.dir)
		}
	}
	return via
}

// moduleNamed returns the module a cmd/<name> entrypoint wires: the module with
// the same name. Empty when the repository has no single such module.
func (l *loaded) moduleNamed(name string) string {
	found := ""
	for _, module := range l.modules {
		if path.Base(module) != name {
			continue
		}
		if found != "" {
			return ""
		}
		found = module
	}
	return found
}

// moduleOf returns the module that owns dir: internal/<context>/<module>.
// Shared packages (internal/contracts, internal/platform/...) and tooling such
// as this test belong to no module and return "".
func moduleOf(dir string) string {
	parts := strings.Split(dir, "/")
	if len(parts) < 3 || parts[0] != "internal" {
		return ""
	}
	if parts[1] == "contracts" || parts[1] == "platform" {
		return ""
	}
	return strings.Join(parts[:3], "/")
}

// moduleOfImport is moduleOf for an import path, and "" for a package outside
// this repository.
func moduleOfImport(imp string) string {
	if !under(imp, modulePath) {
		return ""
	}
	return moduleOf(strings.TrimPrefix(strings.TrimPrefix(imp, modulePath), "/"))
}

// under reports whether imp is base or one of its subpackages.
func under(imp, base string) bool {
	return imp == base || strings.HasPrefix(imp, base+"/")
}

// isStdlib reports whether an import path belongs to the standard library: a
// standard import path has no dot in its first segment. This repository's own
// paths are checked first, because fleet-telemetry has no dot either.
func isStdlib(imp string) bool {
	if under(imp, modulePath) {
		return false
	}
	first, _, _ := strings.Cut(imp, "/")
	return !strings.Contains(first, ".")
}

// rule1 is module isolation: a module must not import another module at any
// depth, and a shared package must not import a module at all.
func rule1(l *loaded, p *pkg) []violation {
	// Only the internal tree is organised in modules; cmd/ is governed by rule 3
	// and the shared tooling packages by nothing at all.
	if !under(p.dir, "internal") {
		return nil
	}
	from := moduleOf(p.dir)
	var out []violation
	for dir, first := range l.reach(p) {
		if dir == p.dir {
			continue
		}
		to := moduleOf(dir)
		if to == "" || to == from {
			continue
		}

		var why string
		if from == "" {
			why = fmt.Sprintf("shared package %s must not import module %s: every importer of a shared package would depend on that module", p.dir, to)
		} else {
			why = fmt.Sprintf("module %s must not import module %s", from, to)
		}
		if first != modulePath+"/"+dir {
			why = fmt.Sprintf("%s is reached through %s at depth; %s", modulePath+"/"+dir, first, why)
		}
		out = append(out, violation{Rule: 1, File: p.fileOf(first), Import: first, Why: why})
	}
	sortViolations(out)
	return out
}

// rule2 is core independence: an app package must not import a framework,
// driver or its own adapter, directly or through another repository package.
// Wrapping the framework in a helper package does not make the core independent
// of it, so the rule follows the import graph.
func rule2(l *loaded, p *pkg) []violation {
	if path.Base(p.dir) != "app" {
		return nil
	}
	own := moduleOf(p.dir)
	var out []violation
	for dir, first := range l.reach(p) {
		target := l.byDir[dir]
		for _, imp := range target.imports {
			var why string
			switch {
			case reachesFramework(imp.path):
				why = fmt.Sprintf("%s is a framework or driver; app package %s must reach every external system through a port it declares", imp.path, p.dir)
			case own != "" && under(imp.path, modulePath+"/"+own+"/adapter"):
				why = fmt.Sprintf("%s is %s's own adapter; the dependency points from the adapter to the port, never back", imp.path, own)
			default:
				continue
			}
			if dir != p.dir {
				why = fmt.Sprintf("reached through %s at depth: %s", first, why)
			}
			out = append(out, violation{Rule: 2, File: target.fileOf(imp.path), Import: imp.path, Why: why})
		}
	}
	sortViolations(out)
	return out
}

// rule3 is composition root: a cmd/ entrypoint may import the standard library,
// internal/platform/... and its own module, and nothing else.
func rule3(l *loaded, p *pkg) []violation {
	parts := strings.Split(p.dir, "/")
	if len(parts) < 2 || parts[0] != "cmd" {
		return nil
	}
	name := parts[1]
	own := l.moduleNamed(name)
	var out []violation
	for _, imp := range p.imports {
		switch {
		case isStdlib(imp.path):
		case under(imp.path, modulePath+"/internal/platform"):
		case own != "" && under(imp.path, modulePath+"/"+own):
		default:
			var why string
			if own == "" {
				why = fmt.Sprintf("there is no single module named %q under internal/<context>/, so this entrypoint may wire only the standard library and internal/platform", name)
			} else {
				why = fmt.Sprintf("a command may import the standard library, internal/platform and its own module %s only", modulePath+"/"+own)
			}
			out = append(out, violation{Rule: 3, File: imp.file, Import: imp.path, Why: why})
		}
	}
	return out
}

// reachesFramework reports whether imp is a framework or driver a core package
// must not depend on.
func reachesFramework(imp string) bool {
	for _, forbidden := range coreForbidden {
		if under(imp, forbidden) {
			return true
		}
	}
	return false
}

func sortViolations(out []violation) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].File != out[j].File {
			return out[i].File < out[j].File
		}
		return out[i].Import < out[j].Import
	})
}

// loadRepo parses every build file of the repository. Test files are skipped on
// purpose: they are not part of the package another package imports.
func loadRepo() (*loaded, error) {
	root := filepath.Join("..", "..")
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		return nil, fmt.Errorf("locate the module root at %s: %w", root, err)
	}

	l := &loaded{byDir: map[string]*pkg{}, byImport: map[string]*pkg{}}
	fset := token.NewFileSet()
	var dirs []string

	err := filepath.WalkDir(root, func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case "vendor", "testdata", "node_modules":
				return filepath.SkipDir
			}
			if name != root && strings.HasPrefix(entry.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}

		rel, err := filepath.Rel(root, name)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)

		file, err := parser.ParseFile(fset, name, nil, parser.ImportsOnly)
		if err != nil {
			return fmt.Errorf("parse %s: %w", rel, err)
		}

		dir := path.Dir(rel)
		p := l.byDir[dir]
		if p == nil {
			p = &pkg{dir: dir}
			l.byDir[dir] = p
			l.byImport[p.importPath()] = p
			dirs = append(dirs, dir)
		}
		for _, spec := range file.Imports {
			unquoted, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return fmt.Errorf("read import of %s: %w", rel, err)
			}
			p.imports = append(p.imports, importLine{path: unquoted, file: rel})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Strings(dirs)
	for _, dir := range dirs {
		l.pkgs = append(l.pkgs, l.byDir[dir])
	}
	seen := map[string]bool{}
	for _, dir := range dirs {
		module := moduleOf(dir)
		if module == "" || seen[module] {
			continue
		}
		seen[module] = true
		l.modules = append(l.modules, module)
	}
	sort.Strings(l.modules)
	return l, nil
}

func mustLoad(t *testing.T) *loaded {
	t.Helper()
	l, err := loadRepo()
	if err != nil {
		t.Fatalf("load the repository import graph: %v", err)
	}
	return l
}

// TestArchitecture applies the three rules to every package of the repository.
func TestArchitecture(t *testing.T) {
	l := mustLoad(t)

	rules := []func(*loaded, *pkg) []violation{rule1, rule2, rule3}
	for _, p := range l.pkgs {
		for _, rule := range rules {
			for _, v := range rule(l, p) {
				t.Error(v)
			}
		}
	}
	t.Logf("checked %d packages and %d modules", len(l.pkgs), len(l.modules))
}

// fakeRepo builds the import graph of a synthetic repository, so each rule is
// tested on the case it must catch without touching the real tree.
func fakeRepo(pkgs ...pkg) *loaded {
	l := &loaded{byDir: map[string]*pkg{}, byImport: map[string]*pkg{}}
	for i := range pkgs {
		p := &pkgs[i]
		l.pkgs = append(l.pkgs, p)
		l.byDir[p.dir] = p
		l.byImport[p.importPath()] = p
	}
	seen := map[string]bool{}
	for _, p := range l.pkgs {
		module := moduleOf(p.dir)
		if module == "" || seen[module] {
			continue
		}
		seen[module] = true
		l.modules = append(l.modules, module)
	}
	sort.Strings(l.modules)
	return l
}

// assertViolations checks that a rule reported exactly the violations wanted, and
// that each message carries the fragments a reviewer needs to fix it.
func assertViolations(t *testing.T, got []violation, want []string) {
	t.Helper()
	if len(want) == 0 {
		if len(got) != 0 {
			t.Fatalf("got %d violation(s), want none:\n%s", len(got), joinViolations(got))
		}
		return
	}
	if len(got) != 1 {
		t.Fatalf("got %d violation(s), want exactly 1:\n%s", len(got), joinViolations(got))
	}
	for _, fragment := range want {
		if !strings.Contains(got[0].String(), fragment) {
			t.Fatalf("violation %q does not carry %q", got[0], fragment)
		}
	}
}

func joinViolations(vs []violation) string {
	lines := make([]string, len(vs))
	for i, v := range vs {
		lines[i] = v.String()
	}
	return strings.Join(lines, "\n")
}

// TestRule1ModulesStayIsolated covers the failing case of rule 1: one module
// importing another, directly and through a shared package.
func TestRule1ModulesStayIsolated(t *testing.T) {
	const (
		journalApp = "fleet-telemetry/internal/telemetry/journal/app"
		intakeApp  = "internal/telemetry/intake/app"
	)

	tests := []struct {
		name string
		repo []pkg
		from string
		want []string
	}{
		{
			name: "a module importing another module directly",
			repo: []pkg{
				{dir: intakeApp, imports: []importLine{{path: journalApp, file: intakeApp + "/submit.go"}}},
				{dir: "internal/telemetry/journal/app"},
			},
			from: intakeApp,
			want: []string{
				"rule 1 (module isolation)",
				intakeApp + "/submit.go imports " + journalApp,
				"module internal/telemetry/intake must not import module internal/telemetry/journal",
			},
		},
		{
			name: "a module importing another module through a shared package",
			repo: []pkg{
				{dir: intakeApp, imports: []importLine{{path: "fleet-telemetry/internal/platform/httpx", file: intakeApp + "/submit.go"}}},
				{dir: "internal/platform/httpx", imports: []importLine{{path: journalApp, file: "internal/platform/httpx/journal.go"}}},
				{dir: "internal/telemetry/journal/app"},
			},
			from: intakeApp,
			want: []string{
				intakeApp + "/submit.go imports fleet-telemetry/internal/platform/httpx",
				journalApp + " is reached through fleet-telemetry/internal/platform/httpx at depth",
			},
		},
		{
			name: "a shared package importing a module",
			repo: []pkg{
				{dir: "internal/platform/config", imports: []importLine{{path: journalApp, file: "internal/platform/config/config.go"}}},
				{dir: "internal/telemetry/journal/app"},
			},
			from: "internal/platform/config",
			want: []string{
				"shared package internal/platform/config must not import module internal/telemetry/journal",
			},
		},
		{
			name: "contracts, platform and the module's own tree stay importable",
			repo: []pkg{
				{dir: "internal/telemetry/journal/adapter", imports: []importLine{
					{path: "fleet-telemetry/internal/contracts", file: "internal/telemetry/journal/adapter/timescale.go"},
					{path: "fleet-telemetry/internal/platform/httpx", file: "internal/telemetry/journal/adapter/timescale.go"},
					{path: journalApp, file: "internal/telemetry/journal/adapter/timescale.go"},
				}},
				{dir: "internal/contracts"},
				{dir: "internal/platform/httpx"},
				{dir: "internal/telemetry/journal/app"},
			},
			from: "internal/telemetry/journal/adapter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := fakeRepo(tt.repo...)
			assertViolations(t, rule1(l, l.byDir[tt.from]), tt.want)
		})
	}
}

// TestRule2CoreStaysIndependent covers the failing case of rule 2: an app
// package reaching a framework, a driver or its own adapter.
func TestRule2CoreStaysIndependent(t *testing.T) {
	const (
		intakeApp     = "internal/telemetry/intake/app"
		intakeAdapter = "internal/telemetry/intake/adapter"
	)

	tests := []struct {
		name string
		repo []pkg
		from string
		want []string
	}{
		{
			name: "app importing the HTTP framework",
			repo: []pkg{{dir: intakeApp, imports: []importLine{
				{path: "github.com/gin-gonic/gin", file: intakeApp + "/submit.go"},
			}}},
			from: intakeApp,
			want: []string{
				"rule 2 (core independence)",
				intakeApp + "/submit.go imports github.com/gin-gonic/gin",
				"must reach every external system through a port it declares",
			},
		},
		{
			name: "app importing net/http",
			repo: []pkg{{dir: intakeApp, imports: []importLine{
				{path: "net/http", file: intakeApp + "/submit.go"},
			}}},
			from: intakeApp,
			want: []string{intakeApp + "/submit.go imports net/http"},
		},
		{
			name: "app importing a database driver",
			repo: []pkg{{dir: intakeApp, imports: []importLine{
				{path: "github.com/jackc/pgx/v5/pgxpool", file: intakeApp + "/submit.go"},
			}}},
			from: intakeApp,
			want: []string{intakeApp + "/submit.go imports github.com/jackc/pgx/v5/pgxpool"},
		},
		{
			name: "app importing a bus driver",
			repo: []pkg{{dir: intakeApp, imports: []importLine{
				{path: "github.com/twmb/franz-go/pkg/kgo", file: intakeApp + "/submit.go"},
			}}},
			from: intakeApp,
			want: []string{intakeApp + "/submit.go imports github.com/twmb/franz-go/pkg/kgo"},
		},
		{
			name: "app importing its own adapter",
			repo: []pkg{
				{dir: intakeApp, imports: []importLine{{path: "fleet-telemetry/" + intakeAdapter, file: intakeApp + "/submit.go"}}},
				{dir: intakeAdapter},
			},
			from: intakeApp,
			want: []string{intakeApp + "/submit.go imports fleet-telemetry/" + intakeAdapter, "own adapter"},
		},
		{
			name: "app reaching the framework through a helper package",
			repo: []pkg{
				{dir: intakeApp, imports: []importLine{{path: "fleet-telemetry/internal/platform/httpx", file: intakeApp + "/submit.go"}}},
				{dir: "internal/platform/httpx", imports: []importLine{{path: "github.com/gin-gonic/gin", file: "internal/platform/httpx/gin.go"}}},
			},
			from: intakeApp,
			want: []string{
				"internal/platform/httpx/gin.go imports github.com/gin-gonic/gin",
				"reached through fleet-telemetry/internal/platform/httpx at depth",
			},
		},
		{
			name: "app importing contracts and its own core packages",
			repo: []pkg{
				{dir: intakeApp, imports: []importLine{
					{path: "fleet-telemetry/internal/contracts", file: intakeApp + "/submit.go"},
					{path: "fleet-telemetry/internal/telemetry/intake/app/watermark", file: intakeApp + "/submit.go"},
				}},
				{dir: "internal/contracts"},
				{dir: "internal/telemetry/intake/app/watermark"},
			},
			from: intakeApp,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := fakeRepo(tt.repo...)
			assertViolations(t, rule2(l, l.byDir[tt.from]), tt.want)
		})
	}
}

// TestRule3CommandsOnlyWire covers the failing case of rule 3: an entrypoint
// importing something other than the standard library, internal/platform and its
// own module.
func TestRule3CommandsOnlyWire(t *testing.T) {
	const (
		intakeModule = "internal/telemetry/intake"
		journalApp   = "internal/telemetry/journal/app"
	)

	tests := []struct {
		name string
		repo []pkg
		from string
		want []string
	}{
		{
			name: "the standard library, platform and the command's own module",
			repo: []pkg{
				{dir: "cmd/intake", imports: []importLine{
					{path: "context", file: "cmd/intake/main.go"},
					{path: "fleet-telemetry/internal/platform/config", file: "cmd/intake/main.go"},
					{path: "fleet-telemetry/internal/telemetry/intake", file: "cmd/intake/main.go"},
				}},
				{dir: intakeModule},
				{dir: "internal/platform/config"},
			},
			from: "cmd/intake",
		},
		{
			name: "a command importing another module's internals",
			repo: []pkg{
				{dir: "cmd/intake", imports: []importLine{{path: "fleet-telemetry/" + journalApp, file: "cmd/intake/main.go"}}},
				{dir: intakeModule},
				{dir: journalApp},
			},
			from: "cmd/intake",
			want: []string{
				"rule 3 (composition root)",
				"cmd/intake/main.go imports fleet-telemetry/" + journalApp,
				"a command may import the standard library, internal/platform and its own module fleet-telemetry/" + intakeModule + " only",
			},
		},
		{
			name: "a command importing a shared contract directly",
			repo: []pkg{
				{dir: "cmd/intake", imports: []importLine{{path: "fleet-telemetry/internal/contracts", file: "cmd/intake/main.go"}}},
				{dir: intakeModule},
			},
			from: "cmd/intake",
			want: []string{"cmd/intake/main.go imports fleet-telemetry/internal/contracts"},
		},
		{
			name: "a command importing a framework",
			repo: []pkg{
				{dir: "cmd/intake", imports: []importLine{{path: "github.com/gin-gonic/gin", file: "cmd/intake/main.go"}}},
				{dir: intakeModule},
			},
			from: "cmd/intake",
			want: []string{"cmd/intake/main.go imports github.com/gin-gonic/gin"},
		},
		{
			name: "a command with no module of its own",
			repo: []pkg{
				{dir: "cmd/orphan", imports: []importLine{{path: "fleet-telemetry/internal/telemetry/journal", file: "cmd/orphan/main.go"}}},
				{dir: intakeModule},
			},
			from: "cmd/orphan",
			want: []string{"there is no single module named \"orphan\""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := fakeRepo(tt.repo...)
			assertViolations(t, rule3(l, l.byDir[tt.from]), tt.want)
		})
	}
}
