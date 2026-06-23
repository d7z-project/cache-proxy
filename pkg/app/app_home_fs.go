package app

import (
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strings"
)

//go:embed assets/home.html assets/style.css assets/script.js assets/zh.json assets/en.json
var homeAssets embed.FS

var homeTemplate *template.Template

var i18nMaps = map[string]map[string]string{}

func init() {
	for _, pair := range []struct {
		file string
		lang string
	}{
		{"assets/zh.json", "zh"},
		{"assets/en.json", "en"},
	} {
		data, err := homeAssets.ReadFile(pair.file)
		if err != nil {
			panic(err)
		}
		m := map[string]string{}
		if err := json.Unmarshal(data, &m); err != nil {
			panic(err)
		}
		i18nMaps[pair.lang] = m
	}

	htmlData, _ := homeAssets.ReadFile("assets/home.html")
	cssData, _ := homeAssets.ReadFile("assets/style.css")
	jsData, _ := homeAssets.ReadFile("assets/script.js")

	homeTemplate = template.Must(template.New("home").Funcs(template.FuncMap{
		"css": func() template.CSS { return template.CSS(cssData) },
		"js":  func() template.JS { return template.JS(jsData) },
		"t":   func(string, ...any) string { return "" },
	}).Parse(string(htmlData)))
}

type homeData struct {
	Instances   []homeInstance
	Modes       []string
	Summary     homeSummary
	Count       int
	Locale      string
	Theme       string
	LangSwitch  string
	ThemeSwitch string
	I18NJSON    template.JS
}

func renderHome(w http.ResponseWriter, data homeData) {
	i18n := i18nMaps[data.Locale]
	if i18n == nil {
		i18n = i18nMaps["en"]
	}

	tpl, _ := homeTemplate.Clone()
	tpl.Funcs(template.FuncMap{
		"t": func(key string, args ...any) string {
			msg, ok := i18n[key]
			if !ok {
				msg = key
			}
			if len(args) == 0 {
				return msg
			}
			return fmt.Sprintf(msg, args...)
		},
	})
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = tpl.Execute(w, data)
}

func detectLocale(req *http.Request) string {
	if q := req.URL.Query().Get("lang"); q == "zh" || q == "en" {
		return q
	}
	al := req.Header.Get("Accept-Language")
	if strings.HasPrefix(al, "zh") {
		return "zh"
	}
	return "en"
}

func detectTheme(req *http.Request) string {
	if t := req.URL.Query().Get("theme"); t == "light" || t == "dark" {
		return t
	}
	if c, err := req.Cookie("theme"); err == nil && (c.Value == "light" || c.Value == "dark") {
		return c.Value
	}
	return "light"
}
