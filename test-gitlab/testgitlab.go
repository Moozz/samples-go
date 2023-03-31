package testgitlab

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"

	// TODO(cretz): Remove when tagged
	"github.com/xanzy/go-gitlab"
	_ "go.temporal.io/sdk/contrib/tools/workflowcheck/determinism"
)

const pid = 10781

type Dataset struct {
	Name      string
	Timestamp int64
	ETC       string
}

// Workflow is a Hello World workflow definition.
func Workflow(ctx workflow.Context, name string) (string, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	logger := workflow.GetLogger(ctx)
	logger.Info("Storage MR Creation workflow started", "name", name)

	var result string

	ds := Dataset{
		Name:      name,
		Timestamp: time.Now().Unix(),
		ETC:       "some other things",
	}
	err := workflow.ExecuteActivity(ctx, CreateMergeRequest, ds).Get(ctx, &result)
	if err != nil {
		logger.Error("Activity failed.", "Error", err)
		return "", err
	}

	mrMerged := false
	waitForMRMerged := func(ctx workflow.Context) {
		signalName := fmt.Sprintf("%s-%d-merged", ds.Name, ds.Timestamp)
		for {
			selector := workflow.NewSelector(ctx)
			selector.AddReceive(workflow.GetSignalChannel(ctx, signalName), func(c workflow.ReceiveChannel, more bool) {
				c.Receive(ctx, nil)
				mrMerged = true
				logger.Info("branch merged signal Received")
			})
			selector.Select(ctx)
		}
	}

	workflow.Go(ctx, waitForMRMerged)
	err = workflow.Await(ctx, func() bool {
		return mrMerged
	})
	if err != nil {
		return "", err
	}

	logger.Info("Received branch merged signal, proceed to execute the next activity")

	logger.Info("Storage MR Creation workflow completed.", "result", result)

	return result, nil
}

func CreateMergeRequest(ctx context.Context, ds Dataset) (string, error) {
	gc, err := gitlab.NewClient("xxx",
		gitlab.WithBaseURL("https://gitlab.myteksi.net/api/v4/"))
	if err != nil {
		return "", err
	}
	testContent := `
	{
		"schemaName": "trajectoryMSOMetric",
		"dimensionFieldSpecs": [
		  {
			"name": "cityID",
			"dataType": "LONG"
		  },
		  {
			"name": "decisionID",
			"dataType": "STRING"
		  },
		  {
			"name": "merchantID",
			"dataType": "STRING"
		  },
		  {
			"name": "passengerID",
			"dataType": "STRING"
		  },
		  {
			"name": "featureVec",
			"dataType": "DOUBLE",
			"singleValueField": false
		  },
		  {
			"name": "score",
			"dataType": "DOUBLE"
		  }
		],
		"metricFieldSpecs": [
		  {
			"name": "click",
			"dataType": "LONG"
		  },
		  {
			"name": "impression",
			"dataType": "LONG"
		  }
		],
		"dateTimeFieldSpecs": [
		  {
			"name": "eventTime",
			"dataType": "TIMESTAMP",
			"format" : "1:MILLISECONDS:TIMESTAMP",
			"granularity": "1:MILLISECONDS"
		  },
		  {
			"name": "streamTime",
			"dataType": "TIMESTAMP",
			"format": "1:MILLISECONDS:TIMESTAMP",
			"granularity": "1:NANOSECONDS"
		  }
		]
	  }	
`
	branch := gitlab.String(fmt.Sprintf("%s-%d", ds.Name, ds.Timestamp))
	c, _, err := gc.Commits.CreateCommit(pid, &gitlab.CreateCommitOptions{
		Actions: []*gitlab.CommitActionOptions{
			{
				Action:   gitlab.FileAction(gitlab.FileCreate),
				FilePath: gitlab.String("pinot-test/test.json"),
				Content:  &testContent,
			},
		},
		AuthorEmail:   gitlab.String("nutdanai.phansooksai@grabtaxi.com"),
		AuthorName:    gitlab.String("some-bot"),
		CommitMessage: gitlab.String(fmt.Sprintf("[AUTOMATED] onboarding %s", ds.Name)),
		StartBranch:   gitlab.String("master"),
		Branch:        branch,
	})
	if err != nil {
		return "", err
	}

	mr, _, err := gc.MergeRequests.CreateMergeRequest(pid, &gitlab.CreateMergeRequestOptions{
		SourceBranch: branch,
		TargetBranch: gitlab.String("master"),
		Description:  gitlab.String("[AUTOMATED] an attempt to test"),
		Title:        gitlab.String(c.Message),
		Squash:       gitlab.Bool(true),
	})
	if err != nil {
		return "", err
	}
	return mr.WebURL, nil
}
