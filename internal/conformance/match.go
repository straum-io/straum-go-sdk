package conformance

import (
	"encoding/json"
	"fmt"
	mbig "math/big"
	"reflect"
)

// Match reports whether actual matches expected under the rules of
// conformance/README.md: every field expected names must match (omitted
// fields are not checked); arrays have the same length and match element
// by element; "*" matches anything; {"$le": n} and {"$ge": n} compare
// integers given as numbers or decimal strings; {"$len": n} checks an
// array's length. It returns nil or the path of the first mismatch.
func Match(expected, actual any, path string) error {
	fail := func() error { return fmt.Errorf("%s: expected %s, got %s", path, show(expected), show(actual)) }
	switch e := expected.(type) {
	case string:
		if e == "*" {
			return nil
		}
	case Obj:
		if len(e) == 1 {
			for op, want := range e {
				switch op {
				case "$len":
					a, ok := actual.([]any)
					n, okN := integer(want)
					if !ok || !okN || mbig.NewInt(int64(len(a))).Cmp(n) != 0 {
						return fail()
					}
					return nil
				case "$le", "$ge":
					w, ok1 := integer(want)
					a, ok2 := integer(actual)
					if !ok1 || !ok2 {
						return fail()
					}
					if c := a.Cmp(w); (op == "$le" && c <= 0) || (op == "$ge" && c >= 0) {
						return nil
					}
					return fail()
				}
				if len(op) > 0 && op[0] == '$' {
					return fmt.Errorf("%s: unknown matcher %s", path, op)
				}
			}
		}
		a, ok := actual.(Obj)
		if !ok {
			return fail()
		}
		for k, v := range e {
			av, ok := a[k]
			if !ok {
				return fmt.Errorf("%s.%s: missing in %s", path, k, show(actual))
			}
			if err := Match(v, av, path+"."+k); err != nil {
				return err
			}
		}
		return nil
	case []any:
		a, ok := actual.([]any)
		if !ok {
			return fail()
		}
		if len(a) != len(e) {
			return fmt.Errorf("%s: expected %d elements, got %d: %s", path, len(e), len(a), show(actual))
		}
		for i := range e {
			if err := Match(e[i], a[i], fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
		return nil
	case json.Number:
		// Numbers compare by value: 0 and 0.0 are equal.
		x, ok1 := integer(e)
		y, ok2 := actual.(json.Number)
		if ok2 {
			if z, ok := integer(y); ok1 && ok && x.Cmp(z) == 0 {
				return nil
			}
		}
		return fail()
	}
	if reflect.DeepEqual(expected, actual) {
		return nil
	}
	return fail()
}

func integer(v any) (*mbig.Int, bool) {
	var s string
	switch x := v.(type) {
	case json.Number:
		s = string(x)
	case string:
		s = x
	default:
		return nil, false
	}
	return new(mbig.Int).SetString(s, 10)
}

func show(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	if len(b) > 200 {
		return string(b[:200]) + "…"
	}
	return string(b)
}
