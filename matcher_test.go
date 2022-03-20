package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_RegexMatcher(t *testing.T) {
	assert := assert.New(t)

	m := NewTopicMatcher("base/{location}/{sensor}")
	b, v := m.Match("base/home1/temperature")
	assert.True(b)
	assert.Equal(map[string]string{
		"location": "home1",
		"sensor":   "temperature",
	}, v)
}
