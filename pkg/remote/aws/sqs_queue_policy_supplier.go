package aws

import (
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/cloudskiff/driftctl/pkg/parallel"
	"github.com/cloudskiff/driftctl/pkg/remote/deserializer"
	"github.com/cloudskiff/driftctl/pkg/resource"
	"github.com/cloudskiff/driftctl/pkg/resource/aws"
	awsdeserializer "github.com/cloudskiff/driftctl/pkg/resource/aws/deserializer"
	"github.com/cloudskiff/driftctl/pkg/terraform"
	"github.com/sirupsen/logrus"
	"github.com/zclconf/go-cty/cty"
)

type SqsQueuePolicySupplier struct {
	reader       terraform.ResourceReader
	deserializer deserializer.CTYDeserializer
	client       sqsiface.SQSAPI
	runner       *terraform.ParallelResourceReader
}

func NewSqsQueuePolicySupplier(runner *parallel.ParallelRunner, client sqsiface.SQSAPI) *SqsQueuePolicySupplier {
	return &SqsQueuePolicySupplier{
		terraform.Provider(terraform.AWS),
		awsdeserializer.NewSqsQueuePolicyDeserializer(),
		client,
		terraform.NewParallelResourceReader(runner),
	}
}

func (s SqsQueuePolicySupplier) Resources() ([]resource.Resource, error) {
	queues, err := listSqsQueues(s.client)
	if err != nil {
		return nil, err
	}

	for _, queue := range queues {
		q := *queue
		s.runner.Run(func() (cty.Value, error) {
			return s.readSqsQueuePolicy(q)
		})
	}

	resources, err := s.runner.Wait()
	if err != nil {
		return nil, err
	}

	return s.deserializer.Deserialize(resources)
}

func (s SqsQueuePolicySupplier) readSqsQueuePolicy(queue string) (cty.Value, error) {
	var Ty resource.ResourceType = aws.AwsSqsQueuePolicyResourceType
	val, err := s.reader.ReadResource(terraform.ReadResourceArgs{
		Ty: Ty,
		ID: queue,
	})
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"type": Ty,
		}).Error(err)
		return cty.NilVal, err
	}
	return *val, nil
}
