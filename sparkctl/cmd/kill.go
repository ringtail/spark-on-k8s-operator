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
	"github.com/spf13/cobra"
	"os"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crdclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
)

var killCmd = &cobra.Command{
	Use:   "kill",
	Short: "kill SparkApplication objects",
	Long:  `kill SparkApplication objects in a given namespaces.`,
	Run: func(cmd *cobra.Command, args []string) {
		crdClientset, err := getSparkApplicationClient()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get SparkApplication client: %v\n", err)
			return
		}
		if len(args) == 0 {
			os.Exit(-1)
		}
		doKill(crdClientset, Namespace, args[0])
	},
}

func doKill(crdClientset crdclientset.Interface, ns, name string) error {
	app, err := crdClientset.SparkoperatorV1beta2().SparkApplications(ns).Get(name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	status := app.Status.ExecutorState
	app.Status.AppState.State = v1beta2.KilledState
	if app.Status.TerminationTime.IsZero() {
		app.Status.TerminationTime = metav1.Now()
	}

	if app.Status.DriverInfo.TerminationTime.IsZero() {
		app.Status.DriverInfo.TerminationTime = metav1.Now()
	}

	if app.Status.DriverInfo.PodState != string(v1beta2.FailedState) && app.Status.DriverInfo.PodState != string(v1beta2.CompletedState){
		app.Status.DriverInfo.PodState = string(v1beta2.FailedState)
		app.Status.DriverInfo.TerminationTime = metav1.Now()
	}

	executorStates := make(map[string]string)
	for k, v := range status {
		if arr := strings.Split(v, " "); len(arr) == 2 {
			executorStates[k] = fmt.Sprintf("%s %s %s", fmt.Sprintf("State:%s", string(v1beta2.ExecutorFailedState)), arr[1], fmt.Sprintf("End:%s", arr[1][6:]))
		} else {
			executorStates[k] = fmt.Sprintf("%s %s %s", fmt.Sprintf("State:%s", string(v1beta2.ExecutorFailedState)), arr[1], fmt.Sprintf("End:%s", arr[1][6:]))
		}
	}

	app.Status.ExecutorState = executorStates
	crdClientset.SparkoperatorV1beta2().SparkApplications(ns).UpdateStatus(app)

	return nil
}
