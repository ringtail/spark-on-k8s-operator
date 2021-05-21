/*
Copyright 2017 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cmd

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crdclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
)

var fixCmd = &cobra.Command{
	Use:   "fix",
	Short: "Fix SparkApplication objects",
	Long:  `Fix SparkApplication objects in a given namespaces.`,
	Run: func(cmd *cobra.Command, args []string) {
		crdClientset, err := getSparkApplicationClient()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get SparkApplication client: %v\n", err)
			return
		}

		client, err := getKubeClient()
		if err != nil {
			os.Exit(0)
		}
		nsItems, err := client.CoreV1().Namespaces().List(metav1.ListOptions{})
		if err != nil {
			os.Exit(0)
		}

		if Namespace == "all-namespaces" {
			for _, ns := range nsItems.Items {
				apps, err := crdClientset.SparkoperatorV1beta2().SparkApplications(ns.Name).List(metav1.ListOptions{})
				if err != nil {
					fmt.Errorf("%v", err)
				}

				for _, app := range apps.Items {
					status := app.Status.ExecutorState
					found := false
					for _, v := range status {
						if arr := strings.Split(v, " "); len(arr) == 3 && strings.Contains(arr[1], "State") {
							found = true
							break
						}
					}
					if found == true {
						if err = doFix(crdClientset, ns.Name, app.Name); err != nil {
							fmt.Fprintf(os.Stderr, "failed to list SparkApplications: %v\n", err)
						}
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
		} else {
			if len(args) == 0 {
				apps, err := crdClientset.SparkoperatorV1beta2().SparkApplications(Namespace).List(metav1.ListOptions{})
				if err != nil {
					fmt.Errorf("%v", err)
				}

				for _, app := range apps.Items {
					status := app.Status.ExecutorState
					found := false
					for _, v := range status {
						if arr := strings.Split(v, " "); len(arr) == 3 && strings.Contains(arr[1], "State") {
							found = true
							break
						}
					}
					if found == true {
						if err = doFix(crdClientset, Namespace, app.Name); err != nil {
							fmt.Fprintf(os.Stderr, "failed to list SparkApplications: %v\n", err)
						}
					}
				}
			} else {
				if err = doFix(crdClientset, Namespace, args[0]); err != nil {
					fmt.Fprintf(os.Stderr, "failed to list SparkApplications: %v\n", err)
				}
			}
		}
	},
}

func doFix(crdClientset crdclientset.Interface, ns, name string) error {
	app, err := crdClientset.SparkoperatorV1beta2().SparkApplications(ns).Get(name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	status := app.Status.ExecutorState
	executorStates := make(map[string]string)
	for k, v := range status {
		if arr := strings.Split(v, " "); len(arr) == 3 && strings.Contains(arr[1], "State") {
			executorStates[k] = fmt.Sprintf("%s %s %s", arr[0], fmt.Sprintf("Start:%v", app.Status.DriverInfo.CreationTimestamp.UTC().Format(time.RFC3339)), arr[2])
		} else {
			executorStates[k] = v
		}
	}

	app.Status.ExecutorState = executorStates
	crdClientset.SparkoperatorV1beta2().SparkApplications(ns).UpdateStatus(app)

	return nil
}
