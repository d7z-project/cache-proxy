package openvsx

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"golang.org/x/net/html"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
)

const maxJSONDepth = 128

func rewriteJSON(source io.Reader, destination io.Writer, origin, publicBase *url.URL) error {
	decoder := json.NewDecoder(source)
	decoder.UseNumber()
	writer := bufio.NewWriterSize(destination, 32<<10)
	if err := rewriteJSONValue(decoder, writer, origin, publicBase, 0); err != nil {
		return err
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf("unexpected JSON token %v", token)
	}
	return writer.Flush()
}

func rewriteJSONValue(decoder *json.Decoder, writer io.Writer, origin, publicBase *url.URL, depth int) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if value, ok := token.(string); ok {
		token = rewriteURL(value, origin, publicBase)
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		encoded, err := json.Marshal(token)
		if err == nil {
			_, err = writer.Write(encoded)
		}
		return err
	}
	if depth >= maxJSONDepth {
		return fmt.Errorf("open VSX JSON exceeds %d nesting levels", maxJSONDepth)
	}
	_, _ = io.WriteString(writer, string(delimiter))
	first := true
	for decoder.More() {
		if !first {
			_, _ = io.WriteString(writer, ",")
		}
		first = false
		if delimiter == '{' {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok {
				return errors.New("open VSX JSON object key is not a string")
			}
			encoded, _ := json.Marshal(name)
			_, _ = writer.Write(encoded)
			_, _ = io.WriteString(writer, ":")
		}
		if err := rewriteJSONValue(decoder, writer, origin, publicBase, depth+1); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return err
	}
	_, _ = io.WriteString(writer, string(closing.(json.Delim)))
	return nil
}

func rewriteHTML(source io.Reader, destination io.Writer, origin, publicBase *url.URL) error {
	document, err := html.Parse(source)
	if err != nil {
		return err
	}
	var visit func(*html.Node)
	visit = func(node *html.Node) {
		for child := node.FirstChild; child != nil; {
			next := child.NextSibling
			if removeHTMLNode(child, origin) {
				node.RemoveChild(child)
			} else {
				rewriteHTMLNode(child, origin, publicBase)
				visit(child)
			}
			child = next
		}
	}
	rewriteHTMLNode(document, origin, publicBase)
	visit(document)
	return html.Render(destination, document)
}

func removeHTMLNode(node *html.Node, origin *url.URL) bool {
	if node.Type != html.ElementNode {
		return false
	}
	if node.Data == "script" && htmlTextContains(node, "googletagmanager.com") {
		return true
	}
	attribute := ""
	switch node.Data {
	case "script", "iframe", "img", "source", "video", "audio", "embed":
		attribute = "src"
	case "object":
		attribute = "data"
	case "link":
		attribute = "href"
	default:
		return false
	}
	for _, item := range node.Attr {
		if item.Key != attribute {
			continue
		}
		target, err := url.Parse(strings.TrimSpace(item.Val))
		if err != nil || target.Host == "" {
			return false
		}
		return !transport.SameOrigin(target, origin)
	}
	return false
}

func rewriteHTMLNode(node *html.Node, origin, publicBase *url.URL) {
	if node.Type == html.TextNode && node.Parent != nil && (node.Parent.Data == "script" || node.Parent.Data == "style") {
		node.Data = rewriteText(node.Data, origin, publicBase)
		return
	}
	if node.Type != html.ElementNode {
		return
	}
	for index := range node.Attr {
		switch node.Attr[index].Key {
		case "href", "src", "action", "data":
			node.Attr[index].Val = rewriteURL(node.Attr[index].Val, origin, publicBase)
		case "srcset":
			parts := strings.Split(node.Attr[index].Val, ",")
			for partIndex, part := range parts {
				fields := strings.Fields(strings.TrimSpace(part))
				if len(fields) > 0 {
					fields[0] = rewriteURL(fields[0], origin, publicBase)
					parts[partIndex] = strings.Join(fields, " ")
				}
			}
			node.Attr[index].Val = strings.Join(parts, ", ")
		}
	}
}

func htmlTextContains(node *html.Node, value string) bool {
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if child.Type == html.TextNode && strings.Contains(strings.ToLower(child.Data), value) {
			return true
		}
	}
	return false
}

func rewriteURL(value string, origin, publicBase *url.URL) string {
	target, err := url.Parse(strings.TrimSpace(value))
	if err != nil || target.Scheme == "" || target.Host == "" || !transport.SameOrigin(target, origin) {
		return value
	}
	rewritten := *target
	rewritten.Scheme = publicBase.Scheme
	rewritten.Host = publicBase.Host
	rewritten.User = nil
	return rewritten.String()
}

func rewriteText(value string, origin, publicBase *url.URL) string {
	upstream := origin.Scheme + "://" + origin.Host
	public := publicBase.Scheme + "://" + publicBase.Host
	value = replaceOrigin(value, upstream, public, false)
	return replaceOrigin(value, strings.ReplaceAll(upstream, "/", `\/`), strings.ReplaceAll(public, "/", `\/`), true)
}

func replaceOrigin(value, origin, replacement string, escaped bool) string {
	var rewritten strings.Builder
	for {
		index := strings.Index(value, origin)
		if index < 0 {
			rewritten.WriteString(value)
			return rewritten.String()
		}
		end := index + len(origin)
		rewritten.WriteString(value[:index])
		validBoundary := end == len(value)
		if !validBoundary {
			switch value[end] {
			case '/', '?', '#', '\\', '"', '\'', ' ', '`', ')', ']', '}', ',', ';', '<', '>', '\t', '\r', '\n':
				validBoundary = true
			}
			if escaped && value[end] == '\\' && end+1 < len(value) && value[end+1] == '/' {
				validBoundary = true
			}
		}
		if validBoundary {
			rewritten.WriteString(replacement)
		} else {
			rewritten.WriteString(origin)
		}
		value = value[end:]
	}
}
