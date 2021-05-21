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
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"os"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
)

var unhealthyCmd = &cobra.Command{
	Use:   "unhealthy",
	Short: "unhealthy SparkApplication objects",
	Long:  `unhealthy SparkApplication objects in a given namespaces.`,
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

		items := make([]v1beta2.SparkApplication, 0)

		if Namespace == "all-namespaces" {
			for _, ns := range nsItems.Items {
				apps, err := crdClientset.SparkoperatorV1beta2().SparkApplications(ns.Name).List(metav1.ListOptions{})
				if err != nil {
					fmt.Errorf("%v", err)
				}

				for _, app := range apps.Items {
					status := app.Status.ExecutorState

					if app.Status.AppState.State == v1beta2.CompletedState || app.Status.AppState.State == v1beta2.FailedState || app.Status.AppState.State == v1beta2.KilledState {
						found := false
						for _, v := range status {
							if arr := strings.Split(v, " "); len(arr) != 3 {
								found = true
								break
							}
						}
						if found == true {
							items = append(items, app)
						}
					} else {

						count := 0

						for _, v := range status {
							if arr := strings.Split(v, " "); len(arr) != 3 {
								count = count + 1
							}
						}

						pods, err := client.CoreV1().Pods(ns.Name).List(metav1.ListOptions{
							LabelSelector: fmt.Sprintf("spark-app-selector=%s,spark-role=executor", app.Name),
						})
						if err != nil {
							fmt.Errorf("%s", err)
						}

						if len(pods.Items) != count {
							items = append(items, app)
						}
					}
				}
			}

			if len(items) == 0 {
				os.Exit(0)
			}

			table := tablewriter.NewWriter(os.Stdout)
			table.SetHeader([]string{"Name", "State", "Submission Age", "Termination Age"})

			for _, app := range items {
				table.Append([]string{
					string(app.Name),
					string(app.Status.AppState.State),
					getSinceTime(app.Status.LastSubmissionAttemptTime),
					getSinceTime(app.Status.TerminationTime),
				})
			}
			table.Render()
		}
	},
}
