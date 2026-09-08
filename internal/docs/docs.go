// Package docs bundles Fluxa's versioned, offline documentation.
package docs

import (
	"embed"
	"html/template"
	"io/fs"
	"net/http"
	"path"
	"sort"
	"strings"

	"github.com/yuin/goldmark"
)

//go:embed content/*.md
var content embed.FS

type Document struct{ Name, Body string }

func All() ([]Document, error) {
	entries, err := fs.ReadDir(content, "content")
	if err != nil {
		return nil, err
	}
	var documents []Document
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".md") {
			continue
		}
		body, err := content.ReadFile(path.Join("content", entry.Name()))
		if err != nil {
			return nil, err
		}
		documents = append(documents, Document{Name: strings.TrimSuffix(entry.Name(), ".md"), Body: string(body)})
	}
	sort.Slice(documents, func(i, j int) bool { return documents[i].Name < documents[j].Name })
	return documents, nil
}

func Search(query string) ([]Document, error) {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return All()
	}
	words := strings.Fields(query)
	docs, err := All()
	if err != nil {
		return nil, err
	}
	var matches []Document
	for _, doc := range docs {
		haystack := strings.ToLower(doc.Name + "\n" + doc.Body)
		ok := true
		for _, word := range words {
			if !strings.Contains(haystack, word) {
				ok = false
				break
			}
		}
		if ok {
			matches = append(matches, doc)
		}
	}
	return matches, nil
}

var pageTemplate = template.Must(template.New("docs").Parse(`<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Fluxa docs{{if .Title}} · {{.Title}}{{end}}</title>
<style>body{max-width:1050px;margin:0 auto;padding:0 24px 64px;font:16px/1.6 system-ui,sans-serif;color:#17202a}header{border-bottom:1px solid #dbe2ea;padding:18px 0;margin-bottom:32px;display:flex;gap:22px;align-items:center;flex-wrap:wrap}a{color:#0757a0}nav a{margin-right:12px;text-decoration:none}form{margin-left:auto}input{padding:7px 10px;min-width:220px}code{background:#f2f5f7;padding:2px 4px;border-radius:3px}pre{overflow:auto;background:#17202a;color:#f7fafc;padding:16px;border-radius:6px}pre code{background:transparent;padding:0}table{border-collapse:collapse}th,td{border:1px solid #dbe2ea;padding:7px 10px;text-align:left}h1,h2,h3{line-height:1.2;margin-top:1.7em}.notice{background:#eef6ff;border-left:4px solid #0757a0;padding:12px 16px}</style></head>
<body><header><strong><a href="/">Fluxa documentation</a></strong><nav><a href="/docs/getting-started">Start</a><a href="/docs/tutorials">Tutorials</a><a href="/docs/lua">Lua API</a><a href="/docs/operations">Operations</a></nav><form action="/search"><input name="q" value="{{.Query}}" placeholder="Search documentation"><button>Search</button></form></header>{{.Body}}</body></html>`))

type pageData struct {
	Title, Query string
	Body         template.HTML
}

// Handler serves rendered Markdown at /docs/<name>, raw source at
// /docs/<name>.md, an index at /, and search at /search?q=... . Goldmark does
// not render raw HTML by default, so documentation remains safe to host.
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/search" {
			serveSearch(w, r)
			return
		}
		name := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if name == "." || name == "" {
			serveIndex(w)
			return
		}
		if strings.HasPrefix(name, "docs/") {
			name = strings.TrimPrefix(name, "docs/")
		}
		if strings.HasSuffix(name, ".md") {
			name = strings.TrimSuffix(name, ".md")
			w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
		} else {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
		}
		for _, unsafe := range []string{"..", "/", `\\`} {
			if strings.Contains(name, unsafe) {
				http.NotFound(w, r)
				return
			}
		}
		body, err := content.ReadFile("content/" + name + ".md")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, ".md") {
			_, _ = w.Write(body)
			return
		}
		servePage(w, name, string(body), "")
	})
}
func serveIndex(w http.ResponseWriter) {
	documents, err := All()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	var b strings.Builder
	b.WriteString("<h1>Fluxa documentation</h1><p class=notice>Start with <a href='/docs/getting-started'>Getting started</a>, then follow a tutorial or use the API reference.</p><h2>Guides</h2><ul>")
	for _, d := range documents {
		b.WriteString("<li><a href='/docs/" + template.HTMLEscapeString(d.Name) + "'>" + template.HTMLEscapeString(d.Name) + "</a> · <a href='/docs/" + template.HTMLEscapeString(d.Name) + ".md'>Markdown</a></li>")
	}
	b.WriteString("</ul>")
	_ = pageTemplate.Execute(w, pageData{Title: "Home", Body: template.HTML(b.String())})
}
func serveSearch(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	results, err := Search(query)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	var b strings.Builder
	b.WriteString("<h1>Search</h1>")
	if query == "" {
		b.WriteString("<p>Enter a search term above.</p>")
	} else if len(results) == 0 {
		b.WriteString("<p>No documentation matched.</p>")
	} else {
		b.WriteString("<p>Results for <code>" + template.HTMLEscapeString(query) + "</code>:</p><ul>")
		for _, d := range results {
			b.WriteString("<li><a href='/docs/" + template.HTMLEscapeString(d.Name) + "'>" + template.HTMLEscapeString(d.Name) + "</a></li>")
		}
		b.WriteString("</ul>")
	}
	_ = pageTemplate.Execute(w, pageData{Title: "Search", Query: query, Body: template.HTML(b.String())})
}
func servePage(w http.ResponseWriter, title, markdown, query string) {
	var rendered strings.Builder
	if err := goldmark.Convert([]byte(markdown), &rendered); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	raw := template.HTMLEscapeString(title)
	rendered.WriteString("<p><a href='/docs/" + raw + ".md'>View Markdown source</a></p>")
	_ = pageTemplate.Execute(w, pageData{Title: title, Query: query, Body: template.HTML(rendered.String())})
}
