package main

import (
	"regexp"
	"strings"
)

type Matcher interface {
	Match(path string) (bool, string)
	Name() string
}

type DirectMatcher struct {
	part string
}

func NewDirectMatcher(part string) *DirectMatcher {
	return &DirectMatcher{
		part: part,
	}
}

func (m *DirectMatcher) Match(part string) (bool, string) {
	if m.part == part {
		return true, ""
	}
	return false, ""
}

func (m *DirectMatcher) Name() string {
	return ""
}

type RegexMatcher struct {
	reg  regexp.Regexp
	name string
}

func NewRegexMatcher(name string, expression string) *RegexMatcher {
	return &RegexMatcher{
		reg:  *regexp.MustCompile(expression),
		name: name,
	}
}

func (r *RegexMatcher) Match(path string) (bool, string) {
	if r.reg.MatchString(path) {
		return true, path
	} else {
		return false, ""
	}
}

func (r *RegexMatcher) Name() string {
	return r.name
}

const MqttSeparator = "/"

func IsSymbol(part string) bool {
	return strings.HasPrefix(part, "{") &&
		strings.HasSuffix(part, "}")
}

func SymbolName(part string) string {
	result := strings.TrimLeft(part, "{")
	result = strings.TrimRight(result, "}")
	return result
}

const MatchAll = "[^\\\\]+"

func SplitSymbol(part string) (string, string) {
	part = SymbolName(part)
	result := strings.Split(part, ":")
	if len(result) >= 2 {
		return result[0], result[1]
	}
	return result[0], MatchAll
}

func createMatchers(topicPath string) []Matcher {
	m := []Matcher{}

	v := strings.Split(topicPath, MqttSeparator)
	for _, part := range v {
		if IsSymbol(part) {
			name, reg := SplitSymbol(part)
			m = append(m, NewRegexMatcher(name, reg))
		} else {
			m = append(m, NewDirectMatcher(part))
		}
	}

	return m
}

type TopicMatcher struct {
	matchers []Matcher
}

func NewTopicMatcher(topicPath string) *TopicMatcher {
	return &TopicMatcher{
		matchers: createMatchers(topicPath),
	}
}

func (m *TopicMatcher) Match(topic string) (bool, map[string]string) {
	result := map[string]string{}
	v := strings.Split(topic, MqttSeparator)
	if len(v) != len(m.matchers) {
		return false, result
	}
	for i := range v {
		matcher := m.matchers[i]
		match, value := matcher.Match(v[i])
		if !match {
			return false, result
		}
		name := matcher.Name()
		if len(name) > 0 {
			result[name] = value
		}

	}

	return true, result
}
