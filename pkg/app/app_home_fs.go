package app

import (
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"strings"
)

//go:embed assets/home.html assets/style.css
//go:embed assets/script_core.js assets/script_network.js assets/script_stage.js assets/script_status.js
//go:embed assets/en.json assets/zh.json assets/ja.json assets/ko.json assets/de.json assets/fr.json
var homeAssets embed.FS

var homeTemplate *template.Template

var i18nMaps = map[string]map[string]string{}

type localeOption struct {
	Code   string
	Native string
}

var supportedLocales = []localeOption{
	{Code: "en", Native: "English"},
	{Code: "zh", Native: "简体中文"},
	{Code: "ja", Native: "日本語"},
	{Code: "ko", Native: "한국어"},
	{Code: "de", Native: "Deutsch"},
	{Code: "fr", Native: "Français"},
}

func init() {
	for _, locale := range supportedLocales {
		i18nMaps[locale.Code] = loadI18N(locale.Code)
	}

	htmlData, err := homeAssets.ReadFile("assets/home.html")
	if err != nil {
		panic(err)
	}
	cssData, err := homeAssets.ReadFile("assets/style.css")
	if err != nil {
		panic(err)
	}
	jsCoreData, err := homeAssets.ReadFile("assets/script_core.js")
	if err != nil {
		panic(err)
	}
	jsNetworkData, err := homeAssets.ReadFile("assets/script_network.js")
	if err != nil {
		panic(err)
	}
	jsStageData, err := homeAssets.ReadFile("assets/script_stage.js")
	if err != nil {
		panic(err)
	}
	jsStatusData, err := homeAssets.ReadFile("assets/script_status.js")
	if err != nil {
		panic(err)
	}
	jsData := append(append([]byte{}, jsCoreData...), '\n')
	jsData = append(jsData, jsNetworkData...)
	jsData = append(jsData, '\n')
	jsData = append(jsData, jsStageData...)
	jsData = append(jsData, '\n')
	jsData = append(jsData, jsStatusData...)

	homeTemplate = template.Must(template.New("home").Funcs(template.FuncMap{
		"css": func() template.CSS { return template.CSS(cssData) },
		"js":  func() template.JS { return template.JS(jsData) },
		"t":   func(string, ...any) string { return "" },
	}).Parse(string(htmlData)))
}

type homeData struct {
	Instances     []homeInstance
	Modes         []string
	Single        bool
	Locale        string
	Theme         string
	I18NJSON      template.JS
	StoreHealthy  bool
	StoreDegraded int
	Languages     []localeOption
	LocaleLabel   string
}

func renderHome(w http.ResponseWriter, data homeData) {
	i18n := i18nMaps[data.Locale]
	if i18n == nil {
		i18n = i18nMaps["en"]
	}

	tpl, err := homeTemplate.Clone()
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
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
	if err := tpl.Execute(w, data); err != nil {
		slog.Warn("home template execute failed", "err", err)
	}
}

func detectLocale(req *http.Request) string {
	if locale := matchLocale(req.URL.Query().Get("lang")); locale != "" {
		return locale
	}
	for _, item := range strings.Split(req.Header.Get("Accept-Language"), ",") {
		tag := strings.TrimSpace(strings.SplitN(item, ";", 2)[0])
		if locale := matchLocale(tag); locale != "" {
			return locale
		}
	}
	return "en"
}

func loadI18N(locale string) map[string]string {
	data, err := homeAssets.ReadFile("assets/" + locale + ".json")
	if err != nil {
		panic(err)
	}
	m := map[string]string{}
	if err := json.Unmarshal(data, &m); err != nil {
		panic(err)
	}
	return m
}

func matchLocale(tag string) string {
	tag = strings.ToLower(strings.TrimSpace(tag))
	if tag == "" {
		return ""
	}
	for _, locale := range supportedLocales {
		code := strings.ToLower(locale.Code)
		if tag == code || strings.HasPrefix(tag, code+"-") {
			return locale.Code
		}
	}
	return ""
}

func localeLabel(locale string) string {
	for _, item := range supportedLocales {
		if item.Code == locale {
			return item.Native
		}
	}
	return "English"
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
