package docs

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSearchFindsDurableReplay(t *testing.T) {
	results, err := Search("ambiguous replay")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatal("expected durable replay documentation")
	}
}

func TestHandlerServesMarkdownAndBrowserView(t *testing.T) {
	for _, target := range []string{"/docs/lua", "/docs/lua.md"} {
		recorder := httptest.NewRecorder()
		Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, target, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("%s status = %d", target, recorder.Code)
		}
		if !strings.Contains(recorder.Body.String(), "Lua workflow API") {
			t.Fatalf("%s missing content", target)
		}
		if target == "/docs/lua" && !strings.Contains(recorder.Body.String(), "<h1>Lua workflow API</h1>") {
			t.Fatalf("%s did not render Markdown heading: %s", target, recorder.Body.String())
		}
	}
}
