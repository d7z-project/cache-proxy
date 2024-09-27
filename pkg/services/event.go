package services

import "regexp"

type Event struct {
	urls  []string
	rules map[*regexp.Regexp]EventRule
}

type EventRule struct {
}
