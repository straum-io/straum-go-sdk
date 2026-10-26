package conformance

import "testing"

// The cases of the Rust matcher's test (straum-conformance matching.rs).
func TestMatch(t *testing.T) {
	actual, err := Parse([]byte(`{"a": "5", "b": [1, 2], "c": {"d": true, "e": null}}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, ok := range []string{
		`{}`,
		`{"a": "5"}`,
		`{"a": {"$le": 5}, "b": {"$len": 2}}`,
		`{"c": {"d": true}, "b": [1, "*"]}`,
		`{"a": {"$ge": "4"}}`,
	} {
		want, _ := Parse([]byte(ok))
		if err := Match(want, actual, "$"); err != nil {
			t.Errorf("%s: %v", ok, err)
		}
	}
	for _, bad := range []string{
		`{"a": "6"}`,
		`{"b": [1]}`,
		`{"z": 1}`,
		`{"a": {"$le": 4}}`,
		`{"a": {"$eq": 5}}`,
	} {
		want, _ := Parse([]byte(bad))
		if Match(want, actual, "$") == nil {
			t.Errorf("%s matched", bad)
		}
	}
}
