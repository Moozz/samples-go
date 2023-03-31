package main

import (
	"log"

	testgitlab "github.com/temporalio/samples-go/test-gitlab"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	// The client and worker are heavyweight objects that should be created once per process.
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	w := worker.New(c, "nutdanai_test", worker.Options{})

	w.RegisterWorkflow(testgitlab.Workflow)
	w.RegisterActivity(testgitlab.CreateMergeRequest)

	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("Unable to start worker", err)
	}
}
