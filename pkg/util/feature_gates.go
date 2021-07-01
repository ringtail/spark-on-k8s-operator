package util

import (
	"strings"
)

const (
	SparkConfNodeName       = "SparkConfNodeName"
	SparkConfToleration     = "SparkConfToleration"
	HideSparkConfEnv        = "HideSparkConfEnv"
	HideSparkConfLabel      = "HideSparkConfLabel"
	HideSparkConfAnnotation = "HideSparkConfAnnotation"
)

var (
	defaultFeatureGates = map[string]bool{
		SparkConfNodeName:       true,
		SparkConfToleration:     true,
		HideSparkConfEnv:        false,
		HideSparkConfLabel:      false,
		HideSparkConfAnnotation: false,
	}
)

// convert feature gates params to map
func ConvertFeatureGatesToMap(gates string) map[string]bool {
	m := make(map[string]bool)
	// apply default gates
	for k, v := range defaultFeatureGates {
		m[k] = v
	}

	// parse from gate params
	gatesArr := strings.Split(gates, ",")
	if len(gatesArr) != 0 {
		for _, gate := range gatesArr {
			gatePair := strings.Split(gate, "=")
			if len(gatePair) == 2 {
				m[gatePair[0]] = isTrue(gatePair[1])
			}
		}
	}

	return m
}

func isTrue(v string) bool {
	if v == "True" || v == "true" {
		return true
	}
	return false
}
