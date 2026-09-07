package httpcap

import "testing"

func TestFingerprintIsStableAcrossHeaderAndJSONOrdering(t *testing.T) {
	a := Request{Method: "post", URL: "HTTPS://Example.test:443/a?b=2&a=1", Headers: map[string]string{"X-B": "two", "Content-Type": "application/json", "Authorization": "secret"}, Body: []byte(`{"b":2,"a":1}`)}
	b := Request{Method: "POST", URL: "https://example.test/a?a=1&b=2", Headers: map[string]string{"authorization": "secret", "content-type": "application/json", "x-b": "two"}, Body: []byte(`{"a":1,"b":2}`)}
	_, fa, err := Canonicalize(a, "task", 1)
	if err != nil {
		t.Fatal(err)
	}
	_, fb, err := Canonicalize(b, "task", 1)
	if err != nil {
		t.Fatal(err)
	}
	if fa != fb {
		t.Fatalf("equivalent requests differ: %s != %s", fa, fb)
	}
}

func TestFingerprintChangesWithBodyAndDoesNotExposeSecret(t *testing.T) {
	a := Request{Method: "POST", URL: "https://example.test", Headers: map[string]string{"Authorization": "very-secret"}, Body: []byte(`{"value":1}`)}
	b := a
	b.Body = []byte(`{"value":2}`)
	da, fa, err := Canonicalize(a, "task", 1)
	if err != nil {
		t.Fatal(err)
	}
	_, fb, err := Canonicalize(b, "task", 1)
	if err != nil {
		t.Fatal(err)
	}
	if fa == fb {
		t.Fatal("body change did not alter fingerprint")
	}
	if len(da.Headers) != 1 || da.Headers[0][1] == "very-secret" {
		t.Fatalf("secret header leaked into descriptor: %#v", da.Headers)
	}
}
