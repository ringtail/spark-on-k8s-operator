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
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"os"
	"strings"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crdclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List SparkApplication objects",
	Long:  `List SparkApplication objects in a given namespaces.`,
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
				if err = doList(crdClientset, ns.Name); err != nil {
					fmt.Fprintf(os.Stderr, "failed to list SparkApplications: %v\n", err)
				}
			}
		} else {
			if err = doList(crdClientset, Namespace); err != nil {
				fmt.Fprintf(os.Stderr, "failed to list SparkApplications: %v\n", err)
			}
		}
	},
}

func doList(crdClientset crdclientset.Interface, ns string) error {

	apps, err := crdClientset.SparkoperatorV1beta2().SparkApplications(ns).List(metav1.ListOptions{})
	if err != nil {
		return err
	}

	items := make([]v1beta2.SparkApplication, 0)

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
			items = append(items, app)
		}
	}

	if len(items) == 0 {
		return nil
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

	return nil
}
